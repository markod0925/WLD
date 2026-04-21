from __future__ import annotations

import base64
import json
import logging
import re
import time
from datetime import date
from pathlib import Path
from typing import Any

import requests

from .batching import SummaryBatch
from .errors import LMStudioConnectionError, LMStudioServiceUnavailableError
from .lmstudio_prompt import LMStudioPromptBuilder, PromptBuildResult
from .models import SummaryRecord
from .lmstudio_logging import (
    log_llm_stage,
    safe_error,
    safe_response_preview,
    set_failed_stage,
)


STRUCTURED_SCHEMA_KEYS = ("summary_text", "key_points", "blocked_activity", "metadata")


class LMStudioStructuredResponse:
    def __init__(
        self,
        summary_text: str,
        key_points: list[str],
        blocked_activity: list[str],
        metadata: dict[str, Any] | None = None,
        raw_response: str | None = None,
    ) -> None:
        self.summary_text = summary_text
        self.key_points = key_points
        self.blocked_activity = blocked_activity
        self.metadata = metadata or {}
        self.raw_response = raw_response

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "summary_text": self.summary_text,
            "key_points": self.key_points,
            "blocked_activity": self.blocked_activity,
            "metadata": self.metadata,
        }
        if self.raw_response is not None:
            payload["raw_response"] = self.raw_response
        return payload


class LMStudioClient:
    def __init__(
        self,
        base_url: str,
        model: str,
        timeout_seconds: int = 600,
        prompt_builder: LMStudioPromptBuilder | None = None,
        max_response_attempts: int = 2,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.prompt_builder = prompt_builder or LMStudioPromptBuilder()
        self.max_response_attempts = max(1, int(max_response_attempts))
        self.logger = logging.getLogger(__name__)

    def summarize_batch(self, batch: SummaryBatch) -> tuple[str, dict[str, Any]]:
        endpoint = f"{self.base_url}/chat/completions"
        text_chars = sum(len(segment.text) for segment in batch.text_segments)
        screenshot_count = len(batch.screenshots)
        log_llm_stage(
            self.logger,
            "payload_build",
            "start",
            model=self.model,
            endpoint=endpoint,
            text_chars=text_chars,
            screenshots=screenshot_count,
        )
        try:
            prompt_result = self.prompt_builder.build_summary_prompt(batch)
            content: list[dict[str, Any]] = [{"type": "text", "text": prompt_result.prompt_text}]
            for screenshot in batch.screenshots:
                image_data = _file_to_data_uri(screenshot.file_path)
                if image_data:
                    content.append({"type": "image_url", "image_url": {"url": image_data}})

            user_content: str | list[dict[str, Any]]
            if len(content) == 1:
                user_content = prompt_result.prompt_text
            else:
                user_content = content

            payload = self._build_payload(
                prompt_text=prompt_result.prompt_text,
                system_message=(
                    "You summarize desktop work activity. "
                    "Respond only with JSON containing summary_text, key_points, blocked_activity, metadata."
                ),
                user_content=user_content,
            )
        except Exception as exc:
            log_llm_stage(
                self.logger,
                "payload_build",
                "error",
                level=logging.ERROR,
                model=self.model,
                endpoint=endpoint,
                text_chars=text_chars,
                screenshots=screenshot_count,
                error_type=exc.__class__.__name__,
                error=safe_error(exc),
                exc_info=True,
            )
            raise set_failed_stage(
                LMStudioServiceUnavailableError("Service unavailable: LM Studio payload could not be built."),
                "payload_build",
            ) from exc

        log_llm_stage(
            self.logger,
            "payload_build",
            "ok",
            model=self.model,
            endpoint=endpoint,
            text_chars=text_chars,
            screenshots=screenshot_count,
            messages=len(payload["messages"]),
            images=len(content) - 1,
            truncated=prompt_result.metadata.get("truncated", False),
        )
        structured = self._request_structured_completion(
            payload=payload,
            response_kind="summary",
            prompt_result=prompt_result,
            endpoint=endpoint,
        )
        return structured.summary_text, structured.to_dict()

    def summarize_daily_recap(self, day: date, summaries: list[SummaryRecord]) -> tuple[str, dict[str, Any]]:
        endpoint = f"{self.base_url}/chat/completions"
        job_id = f"daily_recap:{day.isoformat()}"
        text_chars = sum(len(summary.summary_text) for summary in summaries)
        summary_chunks = self._split_daily_recap_chunks(day=day, summaries=summaries)
        chunk_count = len(summary_chunks)
        log_llm_stage(
            self.logger,
            "chunk_plan",
            "ok",
            job_id=job_id,
            model=self.model,
            endpoint=endpoint,
            summaries=len(summaries),
            chunks=chunk_count,
            max_prompt_chars=self.prompt_builder.max_prompt_chars,
        )
        if chunk_count > 1:
            self.logger.info(
                "event=daily_recap_chunking_enabled job_id=%s summaries=%s chunks=%s max_prompt_chars=%s",
                job_id,
                len(summaries),
                chunk_count,
                self.prompt_builder.max_prompt_chars,
            )
        log_llm_stage(
            self.logger,
            "payload_build",
            "start",
            job_id=job_id,
            model=self.model,
            endpoint=endpoint,
            text_chars=text_chars,
            summaries=len(summaries),
        )
        intermediate: list[LMStudioStructuredResponse] = self._request_daily_recap_chunks(
            day=day,
            chunks=summary_chunks,
            endpoint=endpoint,
            job_id=job_id,
            text_chars=text_chars,
            total_summaries=len(summaries),
            response_kind_prefix="daily_recap_chunk",
            chunk_label_prefix="source",
            system_message=(
                "You generate concise daily work recaps. "
                "Respond only with JSON containing summary_text, key_points, blocked_activity, metadata."
            ),
        )

        if chunk_count == 1:
            structured = intermediate[0]
        else:
            reduction_round = 1
            current = intermediate
            while len(current) > 1:
                aggregate_summaries = self._to_intermediate_summary_records(current)
                aggregate_chunks = self._split_daily_recap_chunks(day=day, summaries=aggregate_summaries)
                if len(aggregate_chunks) >= len(current):
                    structured = self._merge_structured_responses_locally(current)
                    structured.metadata["intermediate_chunk_count"] = chunk_count
                    structured.metadata["aggregation_rounds"] = reduction_round - 1
                    structured.metadata["aggregation_fallback"] = "local_merge_no_progress"
                    return structured.summary_text, structured.to_dict()
                current = self._request_daily_recap_chunks(
                    day=day,
                    chunks=aggregate_chunks,
                    endpoint=endpoint,
                    job_id=job_id,
                    text_chars=sum(len(item.summary_text) for item in aggregate_summaries),
                    total_summaries=len(aggregate_summaries),
                    response_kind_prefix=f"daily_recap_reduce_r{reduction_round}",
                    chunk_label_prefix=f"reduce-{reduction_round}",
                    system_message=(
                        "You generate concise daily work recaps. "
                        "Combine intermediate recap chunks into fewer recap chunks while preserving key outcomes. "
                        "Respond only with JSON containing summary_text, key_points, blocked_activity, metadata."
                    ),
                )
                reduction_round += 1
            structured = current[0]
            structured.metadata["intermediate_chunk_count"] = chunk_count
            structured.metadata["aggregation_rounds"] = reduction_round - 1

        return structured.summary_text, structured.to_dict()

    def _merge_structured_responses_locally(
        self, items: list[LMStudioStructuredResponse]
    ) -> LMStudioStructuredResponse:
        summary_lines: list[str] = []
        key_points: list[str] = []
        blocked_activity: list[str] = []
        for item in items:
            text = item.summary_text.strip()
            if text:
                summary_lines.append(text)
            key_points.extend(point for point in item.key_points if point)
            blocked_activity.extend(entry for entry in item.blocked_activity if entry)
        return LMStudioStructuredResponse(
            summary_text="\n".join(summary_lines),
            key_points=key_points[:20],
            blocked_activity=blocked_activity[:20],
            metadata={"aggregation_fallback": "local_merge"},
        )

    def _request_daily_recap_chunks(
        self,
        *,
        day: date,
        chunks: list[list[SummaryRecord]],
        endpoint: str,
        job_id: str,
        text_chars: int,
        total_summaries: int,
        response_kind_prefix: str,
        chunk_label_prefix: str,
        system_message: str,
    ) -> list[LMStudioStructuredResponse]:
        chunk_count = len(chunks)
        responses: list[LMStudioStructuredResponse] = []
        for index, chunk in enumerate(chunks):
            response_kind = "daily_recap" if chunk_count == 1 else f"{response_kind_prefix}_{index + 1}"
            prompt_result = self._build_daily_recap_prompt_result(
                day=day,
                summaries=chunk,
                endpoint=endpoint,
                job_id=job_id,
                text_chars=text_chars,
                total_summaries=total_summaries,
            )
            payload = self._build_payload(
                prompt_text=prompt_result.prompt_text,
                system_message=system_message,
            )
            log_llm_stage(
                self.logger,
                "payload_build",
                "ok",
                job_id=job_id,
                model=self.model,
                endpoint=endpoint,
                text_chars=text_chars,
                summaries=len(chunk),
                messages=len(payload["messages"]),
                images=0,
                truncated=prompt_result.metadata.get("truncated", False),
                chunk=f"{chunk_label_prefix}:{index + 1}/{chunk_count}",
            )
            structured = self._request_structured_completion(
                payload=payload,
                response_kind=response_kind,
                prompt_result=prompt_result,
                endpoint=endpoint,
            )
            responses.append(structured)
        return responses

    def _build_daily_recap_prompt_result(
        self,
        *,
        day: date,
        summaries: list[SummaryRecord],
        endpoint: str,
        job_id: str,
        text_chars: int,
        total_summaries: int,
    ) -> PromptBuildResult:
        try:
            return self.prompt_builder.build_daily_recap_prompt(day=day, summaries=summaries)
        except Exception as exc:
            log_llm_stage(
                self.logger,
                "payload_build",
                "error",
                level=logging.ERROR,
                job_id=job_id,
                model=self.model,
                endpoint=endpoint,
                text_chars=text_chars,
                summaries=total_summaries,
                error_type=exc.__class__.__name__,
                error=safe_error(exc),
                exc_info=True,
            )
            raise set_failed_stage(
                LMStudioServiceUnavailableError("Service unavailable: LM Studio payload could not be built."),
                "payload_build",
            ) from exc

    def _split_daily_recap_chunks(self, *, day: date, summaries: list[SummaryRecord]) -> list[list[SummaryRecord]]:
        if not summaries:
            return [[]]
        chunks: list[list[SummaryRecord]] = []
        current: list[SummaryRecord] = []
        for summary in summaries:
            candidate = [*current, summary]
            prompt = self.prompt_builder.build_daily_recap_prompt(day=day, summaries=candidate)
            if len(prompt.prompt_text) <= self.prompt_builder.max_prompt_chars or not current:
                current = candidate
                continue
            chunks.append(current)
            current = [summary]
        if current:
            chunks.append(current)
        return chunks

    def _to_intermediate_summary_records(self, items: list[LMStudioStructuredResponse]) -> list[SummaryRecord]:
        records: list[SummaryRecord] = []
        for index, item in enumerate(items, start=1):
            records.append(
                SummaryRecord(
                    id=None,
                    job_id=index,
                    start_ts=float(index),
                    end_ts=float(index),
                    summary_text=item.summary_text,
                    summary_json=item.to_dict(),
                    created_ts=float(index),
                )
            )
        return records

    def _post_chat_completion(self, payload: dict[str, Any], *, endpoint: str) -> requests.Response:
        start = time.perf_counter()
        log_llm_stage(
            self.logger,
            "http_start",
            "start",
            endpoint=endpoint,
            url=endpoint,
            timeout=self.timeout_seconds,
        )
        try:
            response = requests.post(
                endpoint,
                json=payload,
                timeout=self.timeout_seconds,
            )
        except requests.Timeout as exc:
            elapsed_s = time.perf_counter() - start
            log_llm_stage(
                self.logger,
                "http_response",
                "error",
                level=logging.ERROR,
                endpoint=endpoint,
                elapsed_s=elapsed_s,
                error_type="Timeout",
                error=safe_error(exc),
                timeout=self.timeout_seconds,
                exc_info=True,
            )
            raise set_failed_stage(
                LMStudioConnectionError("Connection error: Unable to reach LM Studio. Check that it is running."),
                "http_response",
            ) from exc
        except requests.ConnectionError as exc:
            elapsed_s = time.perf_counter() - start
            log_llm_stage(
                self.logger,
                "http_response",
                "error",
                level=logging.ERROR,
                endpoint=endpoint,
                elapsed_s=elapsed_s,
                error_type="ConnectionError",
                error=safe_error(exc),
                exc_info=True,
            )
            raise set_failed_stage(
                LMStudioConnectionError("Connection error: Unable to reach LM Studio. Check that it is running."),
                "http_response",
            ) from exc
        except requests.RequestException as exc:
            elapsed_s = time.perf_counter() - start
            log_llm_stage(
                self.logger,
                "http_response",
                "error",
                level=logging.ERROR,
                endpoint=endpoint,
                elapsed_s=elapsed_s,
                error_type=exc.__class__.__name__,
                error=safe_error(exc),
                exc_info=True,
            )
            raise set_failed_stage(
                LMStudioConnectionError("Connection error: Unable to reach LM Studio. Check the configured address."),
                "http_response",
            ) from exc

        elapsed_s = time.perf_counter() - start
        http_status = getattr(response, "status_code", None)
        if http_status is None or not 200 <= int(http_status) < 300:
            body_preview = safe_response_preview(getattr(response, "text", ""))
            log_llm_stage(
                self.logger,
                "http_response",
                "error",
                level=logging.ERROR,
                endpoint=endpoint,
                elapsed_s=elapsed_s,
                http_status=http_status,
                error_type="HTTPError",
                error=f"HTTP status {http_status}",
                body_preview=body_preview,
            )
            raise set_failed_stage(
                LMStudioServiceUnavailableError(
                    "Service unavailable: LM Studio could not generate a response. Check that the selected model is loaded."
                ),
                "http_response",
            )

        log_llm_stage(
            self.logger,
            "http_response",
            "ok",
            endpoint=endpoint,
            elapsed_s=elapsed_s,
            http_status=http_status,
        )
        return response

    def _build_payload(
        self,
        *,
        prompt_text: str,
        system_message: str,
        user_content: str | list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return {
            "model": self.model,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": system_message},
                {
                    "role": "user",
                    "content": user_content if user_content is not None else prompt_text,
                },
            ],
        }

    def _request_structured_completion(
        self,
        *,
        payload: dict[str, Any],
        response_kind: str,
        prompt_result: PromptBuildResult,
        endpoint: str,
    ) -> LMStudioStructuredResponse:
        last_error: str | None = None
        last_response_text = ""
        current_payload = payload

        for attempt in range(1, self.max_response_attempts + 1):
            response = self._post_chat_completion(current_payload, endpoint=endpoint)
            raw_response_text = getattr(response, "text", "")
            log_llm_stage(
                self.logger,
                "response_parse",
                "start",
                attempt=attempt,
            )
            try:
                data = response.json()
                if not isinstance(data, dict):
                    raise ValueError("LM Studio response must be a JSON object")
                last_response_text, finish_reason = self._extract_message_content(data)
                parsed = _parse_structured_response(last_response_text, response_kind=response_kind)
                parsed.metadata.update(
                    {
                        "response_kind": response_kind,
                        "prompt_metadata": prompt_result.metadata,
                        "parse_status": "validated",
                        "attempt": attempt,
                    }
                )
                if finish_reason is not None:
                    parsed.metadata["finish_reason"] = finish_reason
                if prompt_result.metadata.get("truncated"):
                    parsed.metadata["truncated"] = True
                log_llm_stage(
                    self.logger,
                    "response_parse",
                    "ok",
                    attempt=attempt,
                    output_chars=len(parsed.summary_text),
                    finish_reason=finish_reason or "unknown",
                )
                return parsed
            except ValueError as exc:
                last_error = str(exc)
                log_llm_stage(
                    self.logger,
                    "response_parse",
                    "error",
                    level=logging.ERROR,
                    attempt=attempt,
                    error_type=exc.__class__.__name__,
                    error=safe_error(exc),
                    response_preview=safe_response_preview(raw_response_text or last_response_text),
                    exc_info=True,
                )
                if attempt >= self.max_response_attempts:
                    break
                current_payload = self._retry_payload(current_payload, response_kind=response_kind, error=last_error)

        raise set_failed_stage(
            LMStudioServiceUnavailableError(
                "Service unavailable: LM Studio returned an unexpected response."
            ),
            "response_parse",
        )

    def _retry_payload(self, payload: dict[str, Any], *, response_kind: str, error: str) -> dict[str, Any]:
        retry_payload = json.loads(json.dumps(payload))
        retry_instruction = (
            "The previous response was invalid. Return only a single JSON object with keys "
            f"{', '.join(STRUCTURED_SCHEMA_KEYS)}. No markdown. No commentary. Error: {error}"
        )
        retry_payload["messages"] = [
            payload["messages"][0],
            {"role": "system", "content": retry_instruction},
            payload["messages"][1],
        ]
        return retry_payload

    def _extract_message_content(self, data: dict[str, Any]) -> tuple[str, str | None]:
        try:
            choice = data["choices"][0]
            message = choice["message"]
            raw_content = message["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError("LM Studio returned an unexpected response") from exc

        finish_reason = choice.get("finish_reason")
        if isinstance(raw_content, list):
            text_content = "\n".join(part.get("text", "") for part in raw_content if isinstance(part, dict))
        elif isinstance(raw_content, dict):
            text_content = str(raw_content.get("text", ""))
        else:
            text_content = str(raw_content)
        return text_content, str(finish_reason) if finish_reason is not None else None


def _parse_structured_response(text: str, *, response_kind: str) -> LMStudioStructuredResponse:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        fence_match = re.search(r"```(?:json)?\s*(.*?)\s*```", cleaned, flags=re.IGNORECASE | re.DOTALL)
        if fence_match:
            cleaned = fence_match.group(1).strip()

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ValueError("Malformed JSON response") from exc

    if not isinstance(parsed, dict):
        raise ValueError("LM Studio response must be a JSON object")

    summary_text = _coerce_text(parsed.get("summary_text") or parsed.get("summary") or parsed.get("recap_text") or cleaned)
    key_points = _coerce_string_list(parsed.get("key_points") or parsed.get("major_activities") or [])
    blocked_activity = _coerce_string_list(parsed.get("blocked_activity") or [])
    metadata_value = parsed.get("metadata")
    metadata = metadata_value if isinstance(metadata_value, dict) else {}
    metadata = {
        **metadata,
        "schema": "worklog.lmstudio.response.v1",
        "response_kind": response_kind,
        "parse_status": "validated",
    }
    return LMStudioStructuredResponse(
        summary_text=summary_text,
        key_points=key_points,
        blocked_activity=blocked_activity,
        metadata=metadata,
        raw_response=text,
    )


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _coerce_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        text = _coerce_text(item)
        if text:
            result.append(text)
    return result


def _file_to_data_uri(path: str) -> str | None:
    file_path = Path(path)
    if not file_path.exists():
        return None
    raw = file_path.read_bytes()
    b64 = base64.b64encode(raw).decode("ascii")
    suffix = file_path.suffix.lower()
    mime = "image/png" if suffix == ".png" else "image/jpeg"
    return f"data:{mime};base64,{b64}"
