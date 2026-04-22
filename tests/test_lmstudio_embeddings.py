from __future__ import annotations

from dataclasses import dataclass

from worklog_diary.core.lmstudio_embeddings import LMStudioEmbeddingClient, SummaryEmbeddingProvider


@dataclass
class _SummaryStub:
    id: int
    job_id: int
    start_ts: float
    end_ts: float
    summary_text: str
    summary_json: dict
    created_ts: float


class _StorageStub:
    def __init__(self) -> None:
        self.upserts = 0

    def get_summary_embedding(self, _summary_id: int):
        return None

    def upsert_summary_embedding(self, **_kwargs):
        self.upserts += 1


def test_embedding_client_raises_clean_error_when_requests_missing(monkeypatch) -> None:
    monkeypatch.setattr("worklog_diary.core.lmstudio_embeddings.requests", None)

    client = LMStudioEmbeddingClient(base_url="http://localhost:1234", model="fake")

    try:
        client.embed_text("hello")
    except RuntimeError as exc:
        assert "requests" in str(exc)
    else:
        raise AssertionError("expected RuntimeError when requests is unavailable")


def test_summary_embedding_provider_degrades_safe_when_requests_missing(monkeypatch) -> None:
    monkeypatch.setattr("worklog_diary.core.lmstudio_embeddings.requests", None)

    storage = _StorageStub()
    client = LMStudioEmbeddingClient(base_url="http://localhost:1234", model="fake")
    provider = SummaryEmbeddingProvider(storage=storage, client=client)

    summary = _SummaryStub(
        id=1,
        job_id=1,
        start_ts=1.0,
        end_ts=2.0,
        summary_text="hello",
        summary_json={"summary_text": "hello", "key_points": []},
        created_ts=3.0,
    )

    assert provider.embedding_for_summary(summary) is None
    assert storage.upserts == 0
