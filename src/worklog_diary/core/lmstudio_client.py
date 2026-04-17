from __future__ import annotations

import base64
import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any

import requests

from .batching import SummaryBatch
from .errors import LMStudioConnectionError, LMStudioServiceUnavailableError
from .lmstudio_prompt import LMStudioPromptBuilder, PromptBuildResult
from .models import SummaryRecord


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
        prompt_result = self.prompt_builder.build_summary_prompt(batch)
        prompt_size_bytes = len(prompt_result.prompt_text.encode("utf-8"))
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
        payload_size_bytes = len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        self.logger.info(
            (
                "event=lmstudio_request model=%s base_url=%s start_ts=%.3f end_ts=%.3f "
                "activity_segments=%s text_segments=%s screenshots=%s payload_content_type=%s "
                "prompt_size_bytes=%s payload_size_bytes=%s truncated=%s"
            ),
            self.model,
            self.base_url,
            batch.start_ts,
            batch.end_ts,
            len(batch.activity_segments),
            len(batch.text_segments),
            len(batch.screenshots),
            "multimodal" if isinstance(user_content, list) else "text",
            prompt_size_bytes,
            payload_size_bytes,
            prompt_result.metadata.get("truncated", False),
        )
        structured = self._request_structured_completion(
            payload=payload,
            response_kind="summary",
            prompt_result=prompt_result,
        )
        return structured.summary_text, structured.to_dict()

    def summarize_daily_recap(self, day: date, summaries: list[SummaryRecord]) -> tuple[str, dict[str, Any]]:
        prompt_result = self.prompt_builder.build_daily_recap_prompt(day=day, summaries=summaries)
        prompt_size_bytes = len(prompt_result.prompt_text.encode("utf-8"))
        payload = self._build_payload(
            prompt_text=prompt_result.prompt_text,
            system_message=(
                "You generate concise daily work recaps. "
                "Respond only with JSON containing summary_text, key_points, blocked_activity, metadata."
            ),
        )
        payload_size_bytes = len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))

        self.logger.info(
            (
                "event=lmstudio_daily_recap_request model=%s base_url=%s day=%s source_summaries=%s "
                "prompt_size_bytes=%s payload_size_bytes=%s truncated=%s"
            ),
            self.model,
            self.base_url,
            day.isoformat(),
            len(summaries),
            prompt_size_bytes,
            payload_size_bytes,
            prompt_result.metadata.get("truncated", False),
        )
        structured = self._request_structured_completion(
            payload=payload,
            response_kind="daily_recap",
            prompt_result=prompt_result,
        )
        return structured.summary_text, structured.to_dict()

    def _post_chat_completion(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            data = response.json()
        except requests.Timeout as exc:
            raise LMStudioConnectionError("Connection error: Unable to reach LM Studio. Check that it is running.") from exc
        except requests.ConnectionError as exc:
            raise LMStudioConnectionError("Connection error: Unable to reach LM Studio. Check that it is running.") from exc
        except requests.HTTPError as exc:
            raise LMStudioServiceUnavailableError(
                "Service unavailable: LM Studio could not generate a response. Check that the selected model is loaded."
            ) from exc
        except requests.RequestException as exc:
            raise LMStudioConnectionError("Connection error: Unable to reach LM Studio. Check the configured address.") from exc
        except ValueError as exc:
            raise LMStudioServiceUnavailableError(
                "Service unavailable: LM Studio returned an unexpected response."
            ) from exc

        if not isinstance(data, dict):
            raise LMStudioServiceUnavailableError("Service unavailable: LM Studio returned an unexpected response.")
        return data

    def _extract_message_content(self, data: dict[str, Any]) -> str:
        try:
            raw_content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise LMStudioServiceUnavailableError(
                "Service unavailable: LM Studio returned an unexpected response."
            ) from exc

        if isinstance(raw_content, list):
            text_content = "\n".join(part.get("text", "") for part in raw_content if isinstance(part, dict))
        elif isinstance(raw_content, dict):
            text_content = str(raw_content.get("text", ""))
        else:
            text_content = str(raw_content)
        return text_content

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
    ) -> LMStudioStructuredResponse:
        last_error: str | None = None
        last_response_text = ""
        current_payload = payload

        for attempt in range(1, self.max_response_attempts + 1):
            data = self._post_chat_completion(current_payload)
            last_response_text = self._extract_message_content(data)
            try:
                parsed = _parse_structured_response(last_response_text, response_kind=response_kind)
                parsed.metadata.update(
                    {
                        "response_kind": response_kind,
                        "prompt_metadata": prompt_result.metadata,
                        "parse_status": "validated",
                        "attempt": attempt,
                    }
                )
                if prompt_result.metadata.get("truncated"):
                    parsed.metadata["truncated"] = True
                return parsed
            except ValueError as exc:
                last_error = str(exc)
                self.logger.warning(
                    "event=lmstudio_parse_retry response_kind=%s attempt=%s error=%s",
                    response_kind,
                    attempt,
                    exc,
                )
                if attempt >= self.max_response_attempts:
                    break
                current_payload = self._retry_payload(current_payload, response_kind=response_kind, error=last_error)

        fallback = _build_fallback_response(
            response_kind=response_kind,
            raw_text=last_response_text,
            prompt_metadata=prompt_result.metadata,
            parse_error=last_error or "Unknown parse failure",
            attempts=self.max_response_attempts,
        )
        self.logger.warning(
            "event=lmstudio_parse_fallback response_kind=%s attempts=%s parse_error=%s",
            response_kind,
            self.max_response_attempts,
            last_error,
        )
        return fallback

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
        self.logger.info("event=lmstudio_retry_requested response_kind=%s", response_kind)
        return retry_payload

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


def _build_fallback_response(
    *,
    response_kind: str,
    raw_text: str,
    prompt_metadata: dict[str, Any],
    parse_error: str,
    attempts: int,
) -> LMStudioStructuredResponse:
    fallback_summary = _coerce_text(raw_text or "LM Studio returned a non-JSON response.")
    metadata = {
        "schema": "worklog.lmstudio.response.v1",
        "response_kind": response_kind,
        "parse_status": "fallback",
        "parse_error": parse_error,
        "attempts": attempts,
        "prompt_metadata": prompt_metadata,
    }
    return LMStudioStructuredResponse(
        summary_text=fallback_summary,
        key_points=[],
        blocked_activity=[],
        metadata=metadata,
        raw_response=raw_text or None,
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
