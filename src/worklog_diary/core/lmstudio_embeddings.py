from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any

try:
    import requests
except ModuleNotFoundError:  # pragma: no cover - exercised in frozen builds without optional deps
    requests = None

from .lmstudio_logging import llm_job_context, log_llm_stage, safe_error
from .models import SummaryRecord
from .summary_canonicalization import build_canonical_embedding_text


class LMStudioEmbeddingClient:
    def __init__(self, *, base_url: str, model: str, timeout_seconds: int = 120) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger(__name__)
        self._request_counter = 0

    def embed_text(self, text: str, *, job_id: object | None = None) -> list[float]:
        if requests is None:
            raise RuntimeError("The optional dependency 'requests' is not available")

        self._request_counter += 1
        current_job_id = job_id if job_id is not None else f"embedding:{self._request_counter}"
        input_chars = len(text)
        token_estimate = _estimate_token_count(input_chars)
        endpoint = f"{self.base_url}/embeddings"
        log_llm_stage(
            self.logger,
            "job_created",
            "ok",
            job_id=current_job_id,
            job_type="embedding",
            timeout_s=self.timeout_seconds,
            attempt=1,
            input_chars=input_chars,
            input_token_estimate=token_estimate,
            queue_size=0,
        )
        log_llm_stage(
            self.logger,
            "job_started",
            "ok",
            job_id=current_job_id,
            job_type="embedding",
            timeout_s=self.timeout_seconds,
            attempt=1,
            input_chars=input_chars,
            input_token_estimate=token_estimate,
            queue_wait_s=0,
            queue_size=0,
        )
        started_at = time.perf_counter()
        try:
            with llm_job_context(
                current_job_id,
                job_type="embedding",
                timeout_s=self.timeout_seconds,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=token_estimate,
                queue_size=0,
                queue_wait_s=0,
            ):
                log_llm_stage(
                    self.logger,
                    "request_submit",
                    "start",
                    endpoint=endpoint,
                    timeout_s=self.timeout_seconds,
                )
                response = requests.post(
                    endpoint,
                    json={"model": self.model, "input": text},
                    timeout=self.timeout_seconds,
                )
            response.raise_for_status()
            request_elapsed_s = time.perf_counter() - started_at
            log_llm_stage(
                self.logger,
                "request_success",
                "ok",
                job_id=current_job_id,
                job_type="embedding",
                timeout_s=self.timeout_seconds,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=token_estimate,
                elapsed_s=request_elapsed_s,
            )
            parse_started_at = time.perf_counter()
            log_llm_stage(
                self.logger,
                "response_parse",
                "start",
                job_id=current_job_id,
                job_type="embedding",
                timeout_s=self.timeout_seconds,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=token_estimate,
            )
            payload: dict[str, Any] = response.json()
            data = payload.get("data")
            if not isinstance(data, list) or not data:
                raise RuntimeError("Embedding response contained no vectors")
            first = data[0]
            if not isinstance(first, dict) or not isinstance(first.get("embedding"), list):
                raise RuntimeError("Embedding response shape is invalid")
            vector = [float(value) for value in first["embedding"]]
            parse_elapsed_s = time.perf_counter() - parse_started_at
            log_llm_stage(
                self.logger,
                "response_parse",
                "ok",
                job_id=current_job_id,
                job_type="embedding",
                timeout_s=self.timeout_seconds,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=token_estimate,
                elapsed_s=parse_elapsed_s,
                vector_size=len(vector),
            )
            log_llm_stage(
                self.logger,
                "job_completed",
                "ok",
                job_id=current_job_id,
                job_type="embedding",
                timeout_s=self.timeout_seconds,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=token_estimate,
                elapsed_s=request_elapsed_s,
            )
            return vector
        except requests.Timeout as exc:
            elapsed_s = time.perf_counter() - started_at
            log_llm_stage(
                self.logger,
                "http_response",
                "error",
                level=logging.ERROR,
                job_id=current_job_id,
                job_type="embedding",
                timeout_s=self.timeout_seconds,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=token_estimate,
                elapsed_s=elapsed_s,
                error_type="Timeout",
                error=safe_error(exc),
                exc_info=True,
            )
            log_llm_stage(
                self.logger,
                "job_failed",
                "error",
                level=logging.ERROR,
                job_id=current_job_id,
                job_type="embedding",
                timeout_s=self.timeout_seconds,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=token_estimate,
                elapsed_s=elapsed_s,
                error_type=exc.__class__.__name__,
                error=safe_error(exc),
                exc_info=True,
            )
            raise
        except requests.ConnectionError as exc:
            elapsed_s = time.perf_counter() - started_at
            log_llm_stage(
                self.logger,
                "http_response",
                "error",
                level=logging.ERROR,
                job_id=current_job_id,
                job_type="embedding",
                timeout_s=self.timeout_seconds,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=token_estimate,
                elapsed_s=elapsed_s,
                error_type="ConnectionError",
                error=safe_error(exc),
                exc_info=True,
            )
            log_llm_stage(
                self.logger,
                "job_failed",
                "error",
                level=logging.ERROR,
                job_id=current_job_id,
                job_type="embedding",
                timeout_s=self.timeout_seconds,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=token_estimate,
                elapsed_s=elapsed_s,
                error_type=exc.__class__.__name__,
                error=safe_error(exc),
                exc_info=True,
            )
            raise
        except Exception as exc:
            elapsed_s = time.perf_counter() - started_at
            log_llm_stage(
                self.logger,
                "response_parse",
                "error",
                level=logging.ERROR,
                job_id=current_job_id,
                job_type="embedding",
                timeout_s=self.timeout_seconds,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=token_estimate,
                elapsed_s=elapsed_s,
                error_type=exc.__class__.__name__,
                error=safe_error(exc),
                exc_info=True,
            )
            log_llm_stage(
                self.logger,
                "job_failed",
                "error",
                level=logging.ERROR,
                job_id=current_job_id,
                job_type="embedding",
                timeout_s=self.timeout_seconds,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=token_estimate,
                elapsed_s=elapsed_s,
                error_type=exc.__class__.__name__,
                error=safe_error(exc),
                exc_info=True,
            )
            raise


class SummaryEmbeddingProvider:
    def __init__(self, *, storage: object, client: LMStudioEmbeddingClient) -> None:
        self.storage = storage
        self.client = client
        self.logger = logging.getLogger(__name__)
        self._last_warning_ts: float = 0.0

    def embedding_for_summary(self, summary: SummaryRecord) -> list[float] | None:
        summary_id = int(summary.id or 0)
        if summary_id <= 0:
            return None
        canonical = build_canonical_embedding_text(summary)
        canonical_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        cached = self.storage.get_summary_embedding(summary_id)
        cache_matches_active_client = (
            cached is not None
            and cached.get("canonical_hash") == canonical_hash
            and str(cached.get("model") or "") == self.client.model
            and str(cached.get("base_url") or "").rstrip("/") == self.client.base_url
        )
        if cache_matches_active_client:
            vector = cached.get("embedding")
            if isinstance(vector, list):
                return [float(v) for v in vector]

        try:
            vector = self.client.embed_text(canonical, job_id=f"summary_embedding:{summary_id}")
        except Exception as exc:
            now = time.time()
            if now - self._last_warning_ts >= 30.0:
                self._last_warning_ts = now
                self.logger.warning(
                    "event=embedding_generation_failed summary_id=%s error_type=%s error=%s behavior=degrade_safe",
                    summary_id,
                    exc.__class__.__name__,
                    exc,
                )
            return None

        self.storage.upsert_summary_embedding(
            summary_id=summary_id,
            canonical_hash=canonical_hash,
            embedding=vector,
            model=self.client.model,
            base_url=self.client.base_url,
        )
        return vector


def _estimate_token_count(chars: int) -> int | None:
    if chars <= 0:
        return None
    return max(1, (chars + 3) // 4)
