from __future__ import annotations

import logging
from datetime import date, datetime, time
from pathlib import Path

from worklog_diary.core.lmstudio_embeddings import SummaryEmbeddingProvider
from worklog_diary.core.llm_job_queue import LLMJobMetadata
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
    activity_entities: list[dict[str, object]] | None = None,
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
            "activity_entities": activity_entities or [],
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

    def embed_text(self, text: str, *_args: object, **_kwargs: object) -> list[float]:
        raise RuntimeError("down")


class _CountingEmbedClient:
    def __init__(self, *, model: str = "embed-a", base_url: str = "http://localhost/v1") -> None:
        self.model = model
        self.base_url = base_url
        self.calls = 0

    def embed_text(self, text: str, *_args: object, **_kwargs: object) -> list[float]:
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


def test_merge_when_same_file_and_task_continue_despite_transition_words() -> None:
    day = date(2026, 4, 10)
    entity_rows = [
        {"entity_type": "file_name", "entity_value": "buildImplicitHeliModel.m", "entity_normalized": "buildimplicithelimodel.m"},
        {"entity_type": "task_candidate", "entity_value": "SLG-487", "entity_normalized": "slg-487"},
        {"entity_type": "program", "entity_value": "matlab.exe", "entity_normalized": "matlab.exe"},
    ]
    left = SummaryRecord(
        1,
        1,
        _ts(day, 9, 0),
        _ts(day, 9, 5),
        "worked on buildImplicitHeliModel.m for SLG-487",
        {"source_context": {"process_name": "matlab.exe", "window_title": "buildImplicitHeliModel.m"}, "activity_entities": entity_rows},
        0,
    )
    right = SummaryRecord(
        2,
        2,
        _ts(day, 9, 6),
        _ts(day, 9, 10),
        "then refined the same SLG-487 model",
        {"source_context": {"process_name": "matlab.exe", "window_title": "buildImplicitHeliModel.m"}, "activity_entities": entity_rows},
        0,
    )
    plans, diag = _engine({1: [1.0, 0.0], 2: [0.8, 0.2]}).build_coalesced_plans([left, right])
    assert len(plans) == 1
    assert diag[0].decision == "merge"
    assert " Then " not in plans[0].summary_text
    assert plans[0].summary_json["task_candidates"] == ["SLG-487"]
    assert plans[0].summary_json["files_and_documents"] == ["buildImplicitHeliModel.m"]
    assert plans[0].summary_json["programs_used"] == ["matlab.exe"]


def test_same_app_without_concrete_overlap_does_not_merge() -> None:
    day = date(2026, 4, 10)
    left = SummaryRecord(
        1,
        1,
        _ts(day, 9, 0),
        _ts(day, 9, 5),
        "worked on alpha task",
        {"source_context": {"process_name": "code.exe", "window_title": "alpha.py"}, "activity_entities": [{"entity_type": "file_name", "entity_value": "alpha.py", "entity_normalized": "alpha.py"}]},
        0,
    )
    right = SummaryRecord(
        2,
        2,
        _ts(day, 9, 6),
        _ts(day, 9, 10),
        "worked on beta task",
        {"source_context": {"process_name": "code.exe", "window_title": "beta.py"}, "activity_entities": [{"entity_type": "file_name", "entity_value": "beta.py", "entity_normalized": "beta.py"}]},
        0,
    )
    plans, diag = _engine({1: [1.0, 0.0], 2: [0.9, 0.1]}).build_coalesced_plans([left, right])
    assert len(plans) == 2
    assert diag[0].decision == "no_merge"


def test_lock_boundary_is_penalty_when_concrete_continuity_is_strong() -> None:
    day = date(2026, 4, 10)
    entity_rows = [
        {"entity_type": "file_name", "entity_value": "parser.py", "entity_normalized": "parser.py"},
        {"entity_type": "task_candidate", "entity_value": "ABC-1234", "entity_normalized": "abc-1234"},
    ]
    left = SummaryRecord(
        1,
        1,
        _ts(day, 9, 0),
        _ts(day, 9, 5),
        "worked on parser.py for ABC-1234",
        {"source_context": {"process_name": "code.exe", "window_title": "parser.py", "closure_reason": "lock_state_changed"}, "activity_entities": entity_rows},
        0,
    )
    right = SummaryRecord(
        2,
        2,
        _ts(day, 9, 6),
        _ts(day, 9, 10),
        "continued parser work after unlock",
        {"source_context": {"process_name": "code.exe", "window_title": "parser.py"}, "activity_entities": entity_rows},
        0,
    )
    plans, diag = _engine({1: [1.0, 0.0], 2: [1.0, 0.0]}).build_coalesced_plans([left, right])
    assert len(plans) == 1
    assert "lock_boundary" not in diag[0].blockers
    assert "lock_boundary_penalty" in diag[0].reasons


def test_same_generic_browser_window_alone_does_not_merge() -> None:
    day = date(2026, 4, 10)
    left = SummaryRecord(
        1,
        1,
        _ts(day, 9, 0),
        _ts(day, 9, 5),
        "reviewed one page",
        {"source_context": {"process_name": "msedge.exe", "window_title": "New Tab"}, "activity_entities": []},
        0,
    )
    right = SummaryRecord(
        2,
        2,
        _ts(day, 9, 6),
        _ts(day, 9, 10),
        "reviewed another page",
        {"source_context": {"process_name": "msedge.exe", "window_title": "New Tab"}, "activity_entities": []},
        0,
    )
    plans, diag = _engine({1: [1.0, 0.0], 2: [1.0, 0.0]}).build_coalesced_plans([left, right])
    assert len(plans) == 2
    assert diag[0].decision == "no_merge"


def test_same_basename_without_folder_or_task_overlap_does_not_merge() -> None:
    day = date(2026, 4, 10)
    left = SummaryRecord(
        1,
        1,
        _ts(day, 9, 0),
        _ts(day, 9, 5),
        "worked on shared README",
        {
            "source_context": {"process_name": "code.exe", "window_title": "workspace-a"},
            "activity_entities": [{"entity_type": "file_name", "entity_value": "README.md", "entity_normalized": "readme.md"}],
        },
        0,
    )
    right = SummaryRecord(
        2,
        2,
        _ts(day, 9, 6),
        _ts(day, 9, 10),
        "worked on other shared README",
        {
            "source_context": {"process_name": "code.exe", "window_title": "workspace-b"},
            "activity_entities": [{"entity_type": "file_name", "entity_value": "README.md", "entity_normalized": "readme.md"}],
        },
        0,
    )
    plans, diag = _engine({1: [1.0, 0.0], 2: [1.0, 0.0]}).build_coalesced_plans([left, right])
    assert len(plans) == 2
    assert diag[0].decision == "no_merge"


def test_neighbor_count_does_not_bridge_across_unrelated_middle_summary() -> None:
    day = date(2026, 4, 10)
    summaries = [
        SummaryRecord(
            1,
            1,
            _ts(day, 9, 0),
            _ts(day, 9, 5),
            "worked on parser.py for ABC-1234",
            {
                "source_context": {"process_name": "code.exe", "window_title": "parser.py"},
                "activity_entities": [
                    {"entity_type": "file_name", "entity_value": "parser.py", "entity_normalized": "parser.py"},
                    {"entity_type": "task_candidate", "entity_value": "ABC-1234", "entity_normalized": "abc-1234"},
                ],
            },
            0,
        ),
        SummaryRecord(
            2,
            2,
            _ts(day, 9, 6),
            _ts(day, 9, 10),
            "continued ABC-1234 checklist work",
            {
                "source_context": {"process_name": "code.exe", "window_title": "abc-1234-notes"},
                "activity_entities": [
                    {"entity_type": "task_candidate", "entity_value": "ABC-1234", "entity_normalized": "abc-1234"},
                ],
            },
            0,
        ),
        SummaryRecord(
            3,
            3,
            _ts(day, 9, 11),
            _ts(day, 9, 15),
            "more parser.py work",
            {
                "source_context": {"process_name": "code.exe", "window_title": "parser.py"},
                "activity_entities": [{"entity_type": "file_name", "entity_value": "parser.py", "entity_normalized": "parser.py"}],
            },
            0,
        ),
    ]
    engine = _engine({1: [1.0, 0.0], 2: [0.9, 0.1], 3: [1.0, 0.0]}, max_neighbor_count=2)
    plans, diag = engine.build_coalesced_plans(summaries)
    assert len(plans) == 2
    assert any("transitive_bridge_blocked" in item.reasons for item in diag)


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
        assert all(row.final_score <= 0.90 for row in low_score_rows)
    finally:
        storage.close()


def test_daily_summary_uses_coalesced_when_enabled(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))

    class _Client:
        def summarize_batch(self, *_args: object, **_kwargs: object) -> tuple[str, dict]:
            return "", {}

        def summarize_daily_recap(self, *args: object, **_kwargs: object) -> tuple[str, dict]:
            summaries = _kwargs.get("summaries")
            if summaries is None and len(args) > 1:
                summaries = args[1]
            if summaries is None:
                summaries = []
            on_started = _kwargs.get("on_started")
            if callable(on_started):
                on_started(
                    LLMJobMetadata(
                        job_id=_kwargs.get("job_id", "test-job"),
                        job_type=str(_kwargs.get("job_type", "day_summary")),
                        queued_at=0.0,
                        started_at=0.0,
                        timeout_s=float(_kwargs.get("timeout_s", 600)),
                        attempt=int(_kwargs.get("attempt", 1)),
                        input_chars=int(_kwargs.get("input_chars", 0)),
                        input_token_estimate=_kwargs.get("input_token_estimate"),
                        priority=int(_kwargs.get("priority", 100)),
                    )
                )
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
        assert saved.recap_json is not None
        assert saved.recap_json["count"] == 1
        assert saved.recap_json["evidence_quality_report"]["summary_kind"] == "daily"
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


def test_coalescing_ignores_internal_artifact_paths_in_evidence() -> None:
    day = date(2026, 4, 10)
    internal_path = r"C:\\Users\\11261\\Desktop\\WLD\\data\\screenshots\\x.png"
    left = SummaryRecord(
        1, 1, _ts(day, 9, 0), _ts(day, 9, 5), "analysis",
        {"source_context": {"process_name": "matlab.exe", "window_title": "ga"}, "activity_entities": [
            {"entity_type": "file_path", "entity_value": internal_path, "entity_normalized": internal_path.lower()},
            {"entity_type": "program", "entity_value": "matlab.exe", "entity_normalized": "matlab.exe"},
        ], "files_and_documents": [internal_path]},
        0,
    )
    right = SummaryRecord(
        2, 2, _ts(day, 9, 6), _ts(day, 9, 10), "analysis",
        {"source_context": {"process_name": "matlab.exe", "window_title": "ga"}, "activity_entities": [
            {"entity_type": "file_path", "entity_value": internal_path, "entity_normalized": internal_path.lower()},
            {"entity_type": "program", "entity_value": "matlab.exe", "entity_normalized": "matlab.exe"},
        ], "files_and_documents": [internal_path]},
        0,
    )
    plans, _ = _engine({1: [1.0, 0.0], 2: [1.0, 0.0]}).build_coalesced_plans([left, right])
    assert len(plans) == 1
    assert internal_path not in plans[0].summary_json.get("files_and_documents", [])
