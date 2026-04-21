from __future__ import annotations

import logging
from datetime import date, datetime, time
from pathlib import Path

from worklog_diary.core.lmstudio_embeddings import SummaryEmbeddingProvider
from worklog_diary.core.models import SummaryRecord
from worklog_diary.core.semantic_coalescing import (
    SemanticCoalescer,
    SemanticCoalescingConfig,
    SemanticCoalescingEngine,
)
from worklog_diary.core.storage import SQLiteStorage
from worklog_diary.core.summarizer import Summarizer


def _ts(day: date, hour: int, minute: int) -> float:
    return datetime.combine(day, time(hour=hour, minute=minute)).timestamp()


def _insert_summary(
    storage: SQLiteStorage,
    *,
    start_ts: float,
    end_ts: float,
    text: str,
    process: str = "code.exe",
    window: str = "Editor",
    closure_reason: str = "open",
) -> int:
    job_id = storage.create_summary_job(start_ts=start_ts, end_ts=end_ts, status="succeeded")
    return storage.insert_summary(
        job_id=job_id,
        start_ts=start_ts,
        end_ts=end_ts,
        summary_text=text,
        summary_json={
            "summary_text": text,
            "source_context": {
                "process_name": process,
                "window_title": window,
                "closure_reason": closure_reason,
            },
        },
    )


class _EmbeddingProvider:
    def __init__(self, vectors: dict[int, list[float] | None]) -> None:
        self.vectors = vectors

    def embedding_for_summary(self, summary: SummaryRecord) -> list[float] | None:
        return self.vectors.get(int(summary.id or 0))


class _FailingEmbedClient:
    model = "embed"
    base_url = "http://localhost"

    def embed_text(self, text: str) -> list[float]:
        raise RuntimeError("down")


class _CountingEmbedClient:
    def __init__(self, *, model: str = "embed-a", base_url: str = "http://localhost/v1") -> None:
        self.model = model
        self.base_url = base_url
        self.calls = 0

    def embed_text(self, text: str) -> list[float]:
        self.calls += 1
        return [float(self.calls), 0.0]


def _engine(vectors: dict[int, list[float] | None], **kwargs: object) -> SemanticCoalescingEngine:
    kwargs.setdefault("min_merge_score", 0.55)
    config = SemanticCoalescingConfig(enabled=True, **kwargs)
    return SemanticCoalescingEngine(config=config, embedding_provider=_EmbeddingProvider(vectors))


def test_canonical_embedding_text_generation() -> None:
    from worklog_diary.core.summary_canonicalization import build_canonical_embedding_text

    record = SummaryRecord(
        id=1,
        job_id=1,
        start_ts=0,
        end_ts=60,
        summary_text="Worked on parser tuning.",
        summary_json={"source_context": {"process_name": "python.exe", "window_title": "main.py"}},
        created_ts=0,
    )
    text = build_canonical_embedding_text(record)
    assert "app=python.exe" in text
    assert "window=main.py" in text
    assert "summary=Worked on parser tuning." in text


def test_embedding_provider_safe_degradation(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    try:
        sid = _insert_summary(storage, start_ts=1, end_ts=2, text="a")
        record = storage.list_summaries(limit=1)[0]
        provider = SummaryEmbeddingProvider(storage=storage, client=_FailingEmbedClient())
        assert provider.embedding_for_summary(record) is None
        assert storage.get_summary_embedding(sid) is None
    finally:
        storage.close()


def test_embedding_provider_recomputes_when_model_or_base_url_changes(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    try:
        _insert_summary(storage, start_ts=1, end_ts=2, text="a")
        record = storage.list_summaries(limit=1)[0]
        client = _CountingEmbedClient()
        provider = SummaryEmbeddingProvider(storage=storage, client=client)

        vector1 = provider.embedding_for_summary(record)
        vector2 = provider.embedding_for_summary(record)
        assert vector1 == [1.0, 0.0]
        assert vector2 == [1.0, 0.0]
        assert client.calls == 1

        client.model = "embed-b"
        vector3 = provider.embedding_for_summary(record)
        assert vector3 == [2.0, 0.0]
        assert client.calls == 2

        client.base_url = "http://another-host/v1"
        vector4 = provider.embedding_for_summary(record)
        assert vector4 == [3.0, 0.0]
        assert client.calls == 3
    finally:
        storage.close()


def test_no_merge_when_gap_too_large() -> None:
    day = date(2026, 4, 10)
    left = SummaryRecord(1, 1, _ts(day, 9, 0), _ts(day, 9, 5), "coding", {"source_context": {"process_name": "code.exe", "window_title": "a"}}, 0)
    right = SummaryRecord(2, 2, _ts(day, 9, 30), _ts(day, 9, 35), "coding", {"source_context": {"process_name": "code.exe", "window_title": "a"}}, 0)
    plans, diag = _engine({1: [1.0, 0.0], 2: [1.0, 0.0]}).build_coalesced_plans([left, right])
    assert len(plans) == 2
    assert "gap_too_large" in diag[0].blockers


def test_no_merge_across_lock_boundary() -> None:
    day = date(2026, 4, 10)
    left = SummaryRecord(1, 1, _ts(day, 9, 0), _ts(day, 9, 5), "coding", {"source_context": {"process_name": "code.exe", "window_title": "a", "closure_reason": "lock_state_changed"}}, 0)
    right = SummaryRecord(2, 2, _ts(day, 9, 6), _ts(day, 9, 10), "coding", {"source_context": {"process_name": "code.exe", "window_title": "a"}}, 0)
    _, diag = _engine({1: [1.0], 2: [1.0]}).build_coalesced_plans([left, right])
    assert "lock_boundary" in diag[0].blockers


def test_no_merge_across_pause_boundary() -> None:
    day = date(2026, 4, 10)
    left = SummaryRecord(1, 1, _ts(day, 9, 0), _ts(day, 9, 5), "coding", {"source_context": {"process_name": "code.exe", "window_title": "a", "closure_reason": "idle_gap"}}, 0)
    right = SummaryRecord(2, 2, _ts(day, 9, 6), _ts(day, 9, 10), "coding", {"source_context": {"process_name": "code.exe", "window_title": "a"}}, 0)
    _, diag = _engine({1: [1.0], 2: [1.0]}).build_coalesced_plans([left, right])
    assert "pause_boundary" in diag[0].blockers


def test_no_merge_on_strong_app_switch() -> None:
    day = date(2026, 4, 10)
    left = SummaryRecord(1, 1, _ts(day, 9, 0), _ts(day, 9, 5), "coding", {"source_context": {"process_name": "code.exe", "window_title": "a"}}, 0)
    right = SummaryRecord(2, 2, _ts(day, 9, 6), _ts(day, 9, 10), "coding", {"source_context": {"process_name": "teams.exe", "window_title": "meeting"}}, 0)
    plans, _ = _engine({1: [1.0], 2: [1.0]}, app_switch_penalty=0.9).build_coalesced_plans([left, right])
    assert len(plans) == 2


def test_merge_when_adjacent_high_similarity() -> None:
    day = date(2026, 4, 10)
    left = SummaryRecord(1, 1, _ts(day, 9, 0), _ts(day, 9, 5), "worked on parser cleanup", {"source_context": {"process_name": "code.exe", "window_title": "parser.py"}}, 0)
    right = SummaryRecord(2, 2, _ts(day, 9, 6), _ts(day, 9, 10), "continued parser cleanup", {"source_context": {"process_name": "code.exe", "window_title": "parser.py"}}, 0)
    plans, _ = _engine({1: [1.0, 0.0], 2: [1.0, 0.0]}).build_coalesced_plans([left, right])
    assert len(plans) == 1
    assert plans[0].source_summary_ids == [1, 2]


def test_merge_lineage_persistence(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    try:
        day = date(2026, 4, 10)
        sid1 = _insert_summary(storage, start_ts=_ts(day, 9, 0), end_ts=_ts(day, 9, 5), text="parser")
        sid2 = _insert_summary(storage, start_ts=_ts(day, 9, 6), end_ts=_ts(day, 9, 9), text="parser updates")
        engine = _engine({sid1: [1.0], sid2: [1.0]})
        coalescer = SemanticCoalescer(storage=storage, engine=engine, diagnostics_enabled=True)
        coalescer.refresh_day(day)

        effective = storage.list_effective_summaries_for_day(day, use_coalesced=True)
        assert len(effective) == 1
        assert effective[0].summary_json["coalesced_from"] == [sid1, sid2]
        diagnostics = storage.list_semantic_merge_diagnostics(day, limit=20)
        assert len(diagnostics) == 1
        assert diagnostics[0].left_summary_id == sid1
        assert diagnostics[0].right_summary_id == sid2
        assert storage.get_coalesced_member_count(effective[0].id or 0) == 2
    finally:
        storage.close()


def test_semantic_diagnostics_filters(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    try:
        day = date(2026, 4, 10)
        sid1 = _insert_summary(storage, start_ts=_ts(day, 9, 0), end_ts=_ts(day, 9, 5), text="parser")
        sid2 = _insert_summary(storage, start_ts=_ts(day, 9, 6), end_ts=_ts(day, 9, 9), text="parser updates")
        sid3 = _insert_summary(storage, start_ts=_ts(day, 10, 0), end_ts=_ts(day, 10, 5), text="meeting prep", process="teams.exe", window="call")
        coalescer = SemanticCoalescer(storage=storage, engine=_engine({sid1: [1.0], sid2: [1.0], sid3: [0.0]}), diagnostics_enabled=True)
        coalescer.refresh_day(day)

        all_rows = storage.list_semantic_merge_diagnostics(day, limit=50)
        merge_rows = storage.list_semantic_merge_diagnostics(day, decision="merge", limit=50)
        no_merge_rows = storage.list_semantic_merge_diagnostics(day, decision="no_merge", limit=50)
        keyword_rows = storage.list_semantic_merge_diagnostics(day, text_query="below_min_cosine", limit=50)
        id_filtered_rows = storage.list_semantic_merge_diagnostics(day, summary_ids=[sid1, sid2], limit=50)
        low_score_rows = storage.list_semantic_merge_diagnostics(day, decision="merge", max_merge_score=0.90, limit=50)

        assert len(all_rows) >= 2
        assert len(merge_rows) == 1
        assert len(no_merge_rows) >= 1
        assert len(keyword_rows) >= 1
        assert len(id_filtered_rows) >= 1
        assert len(low_score_rows) == 1
    finally:
        storage.close()


def test_daily_summary_uses_coalesced_when_enabled(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    class _Client:
        def summarize_batch(self, batch: object) -> tuple[str, dict]:
            return "", {}
        def summarize_daily_recap(self, day: date, summaries: list[SummaryRecord]) -> tuple[str, dict]:
            return f"count={len(summaries)}", {"count": len(summaries)}

    try:
        day = date(2026, 4, 10)
        sid1 = _insert_summary(storage, start_ts=_ts(day, 9, 0), end_ts=_ts(day, 9, 5), text="parser")
        sid2 = _insert_summary(storage, start_ts=_ts(day, 9, 6), end_ts=_ts(day, 9, 9), text="parser updates")
        coalescer = SemanticCoalescer(storage=storage, engine=_engine({sid1: [1.0], sid2: [1.0]}), diagnostics_enabled=True)
        coalescer.refresh_day(day)

        summarizer = Summarizer(storage=storage, batch_builder=None, lm_client=_Client(), semantic_coalescer=coalescer)  # type: ignore[arg-type]
        _, _ = summarizer.generate_daily_recap_for_day(day)
        saved = storage.get_daily_summary_for_day(day)
        assert saved is not None
        assert saved.recap_json == {"count": 1}
        summarizer.stop()
    finally:
        storage.close()


def test_integration_three_adjacent_with_boundary(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    try:
        day = date(2026, 4, 10)
        s1 = _insert_summary(storage, start_ts=_ts(day, 9, 0), end_ts=_ts(day, 9, 5), text="coding parser")
        s2 = _insert_summary(storage, start_ts=_ts(day, 9, 6), end_ts=_ts(day, 9, 8), text="coding parser cleanup")
        s3 = _insert_summary(storage, start_ts=_ts(day, 9, 9), end_ts=_ts(day, 9, 12), text="coding parser tests")
        _insert_summary(storage, start_ts=_ts(day, 9, 13), end_ts=_ts(day, 9, 16), text="then switched to meeting notes", process="teams.exe", window="call", closure_reason="app_changed")
        s5 = _insert_summary(storage, start_ts=_ts(day, 11, 0), end_ts=_ts(day, 11, 5), text="coding parser cleanup", process="code.exe", window="parser.py")

        vectors = {s1: [1.0, 0.0], s2: [0.99, 0.01], s3: [0.98, 0.02], s5: [0.99, 0.01]}
        coalescer = SemanticCoalescer(storage=storage, engine=_engine(vectors), diagnostics_enabled=True)
        coalescer.refresh_day(day)

        effective = storage.list_effective_summaries_for_day(day, use_coalesced=True)
        assert len(effective) == 3
        assert effective[0].summary_json["coalesced_from"] == [s1, s2, s3]
    finally:
        storage.close()


def test_coalescer_logs_merge_summary(tmp_path: Path, caplog) -> None:
    caplog.set_level(logging.INFO)
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    try:
        day = date(2026, 4, 10)
        sid1 = _insert_summary(storage, start_ts=_ts(day, 9, 0), end_ts=_ts(day, 9, 5), text="parser")
        sid2 = _insert_summary(storage, start_ts=_ts(day, 9, 6), end_ts=_ts(day, 9, 9), text="parser updates")
        coalescer = SemanticCoalescer(storage=storage, engine=_engine({sid1: [1.0], sid2: [1.0]}), diagnostics_enabled=True)
        coalescer.refresh_day(day)
        assert "event=semantic_coalescing_merge" in caplog.text
    finally:
        storage.close()


def test_coalescer_logs_no_merge_summary(tmp_path: Path, caplog) -> None:
    caplog.set_level(logging.INFO)
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    try:
        day = date(2026, 4, 10)
        sid1 = _insert_summary(storage, start_ts=_ts(day, 9, 0), end_ts=_ts(day, 9, 5), text="parser")
        sid2 = _insert_summary(storage, start_ts=_ts(day, 9, 30), end_ts=_ts(day, 9, 40), text="meeting notes", process="teams.exe", window="call")
        coalescer = SemanticCoalescer(storage=storage, engine=_engine({sid1: [1.0], sid2: [0.0]}), diagnostics_enabled=True)
        coalescer.refresh_day(day)
        assert "event=semantic_coalescing_no_merge" in caplog.text
    finally:
        storage.close()
