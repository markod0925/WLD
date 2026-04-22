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

from .models import SummaryRecord
from .summary_canonicalization import build_canonical_embedding_text


class LMStudioEmbeddingClient:
    def __init__(self, *, base_url: str, model: str, timeout_seconds: int = 120) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger(__name__)

    def embed_text(self, text: str) -> list[float]:
        if requests is None:
            raise RuntimeError("The optional dependency 'requests' is not available")

        response = requests.post(
            f"{self.base_url}/embeddings",
            json={"model": self.model, "input": text},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload: dict[str, Any] = response.json()
        data = payload.get("data")
        if not isinstance(data, list) or not data:
            raise RuntimeError("Embedding response contained no vectors")
        first = data[0]
        if not isinstance(first, dict) or not isinstance(first.get("embedding"), list):
            raise RuntimeError("Embedding response shape is invalid")
        return [float(value) for value in first["embedding"]]


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
            vector = self.client.embed_text(canonical)
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
