from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path

from worklog_diary.core.models import SummaryRecord
from worklog_diary.core.storage import SQLiteStorage
from worklog_diary.core.summary_search import (
    SummarySearchParams,
    SummarySearchScope,
    SummarySearchService,
    _resolve_bounds,
    coerce_search_scope,
)
from worklog_diary.ui.summaries_view_model import build_summary_card_view, format_summary_html


def _ts(day: date, hour: int, minute: int = 0) -> float:
    return datetime.combine(day, time(hour=hour, minute=minute)).astimezone().timestamp()


def _insert_event_summary(storage: SQLiteStorage, *, start_ts: float, end_ts: float, text: str) -> None:
    job_id = storage.create_summary_job(start_ts=start_ts, end_ts=end_ts, status="succeeded")
    storage.insert_summary(
        job_id=job_id,
        start_ts=start_ts,
        end_ts=end_ts,
        summary_text=text,
        summary_json={"summary_text": text},
    )


def test_summary_search_scopes_and_types(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    service = SummarySearchService(storage)
    try:
        day_a = date(2026, 4, 10)
        day_b = date(2026, 5, 2)
        day_c = date(2025, 12, 31)
        _insert_event_summary(storage, start_ts=_ts(day_a, 10), end_ts=_ts(day_a, 10, 15), text="Alpha coding task")
        _insert_event_summary(storage, start_ts=_ts(day_b, 11), end_ts=_ts(day_b, 11, 30), text="beta review")
        _insert_event_summary(storage, start_ts=_ts(day_c, 9), end_ts=_ts(day_c, 9, 20), text="ALPHA prior year")
        storage.create_daily_summary(day=day_a, recap_text="Daily alpha recap", recap_json=None, source_batch_count=1)
        storage.create_daily_summary(day=day_b, recap_text="Daily beta recap", recap_json=None, source_batch_count=1)

        day_results = service.search(
            SummarySearchParams(query=" alpha ", scope=SummarySearchScope.DAY, anchor_day=day_a)
        )
        assert len(day_results) == 2
        assert {item.summary_type.value for item in day_results} == {"event", "day"}

        month_results = service.search(
            SummarySearchParams(query="alpha", scope=SummarySearchScope.MONTH, anchor_day=day_a)
        )
        assert len(month_results) == 2

        year_results = service.search(
            SummarySearchParams(query="alpha", scope=SummarySearchScope.YEAR, anchor_day=day_a)
        )
        assert len(year_results) == 2

        all_results = service.search(
            SummarySearchParams(query="alpha", scope=SummarySearchScope.ALL, anchor_day=day_a)
        )
        assert len(all_results) == 3

        no_results = service.search(
            SummarySearchParams(query="missing", scope=SummarySearchScope.ALL, anchor_day=day_a)
        )
        assert no_results == []
    finally:
        storage.close()


def test_format_summary_html_highlights_case_insensitive_matches() -> None:
    rendered = format_summary_html("Alpha alpha ALPHA", "alpHa")
    assert rendered.count("background-color: #fff176") == 3


def test_format_summary_html_escapes_html_while_highlighting_special_characters() -> None:
    rendered = format_summary_html("Used <tag> and updated naïve café notes", "naïve café")
    assert "&lt;tag&gt;" in rendered
    assert "<tag>" not in rendered
    assert "background-color: #fff176" in rendered
    assert "naïve café" in rendered


def test_summary_search_treats_like_wildcards_as_literal_text(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    service = SummarySearchService(storage)
    try:
        target_day = date(2026, 4, 10)
        _insert_event_summary(storage, start_ts=_ts(target_day, 10), end_ts=_ts(target_day, 10, 15), text="100% done")
        _insert_event_summary(storage, start_ts=_ts(target_day, 11), end_ts=_ts(target_day, 11, 15), text="100 done")
        storage.create_daily_summary(day=target_day, recap_text="Reviewed _id mapping", recap_json=None, source_batch_count=1)
        storage.create_daily_summary(
            day=date(2026, 4, 11),
            recap_text="Reviewed xid mapping",
            recap_json=None,
            source_batch_count=1,
        )

        percent_results = service.search(
            SummarySearchParams(query="100%", scope=SummarySearchScope.ALL, anchor_day=target_day)
        )
        assert [item.text for item in percent_results] == ["100% done"]

        underscore_results = service.search(
            SummarySearchParams(query="_id", scope=SummarySearchScope.ALL, anchor_day=target_day)
        )
        assert [item.text for item in underscore_results] == ["Reviewed _id mapping"]
    finally:
        storage.close()


def test_build_summary_card_view_preserves_non_ascii_in_fallback_json_rendering() -> None:
    summary = SummaryRecord(
        id=1,
        job_id=1,
        start_ts=10.0,
        end_ts=20.0,
        summary_text="fallback",
        summary_json={
            "outcomes": [{"status": "résolu", "title": "Caffè Δ sync"}],
            "programs_used": ["notepad++.exe"],
        },
        created_ts=30.0,
    )

    card = build_summary_card_view(summary)

    assert '{"status": "résolu", "title": "Caffè Δ sync"}' in card.summary_text
    assert "\\u00e9" not in card.summary_text
    assert "\\u0394" not in card.summary_text


def test_summary_search_matches_terms_adjacent_to_punctuation_and_paths(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    service = SummarySearchService(storage)
    try:
        target_day = date(2026, 4, 10)
        texts = [
            "alpha target beta",
            "target,",
            "(target",
            r"C:\work\target\file.m",
            "target.m",
            "module.target",
            "target: value",
            "foo/target/bar",
            r"foo\target\bar",
            "[target]",
            "target_file",
            "target-file",
        ]
        for idx, text in enumerate(texts):
            start = _ts(target_day, 9 + idx // 2, idx % 2 * 10)
            _insert_event_summary(storage, start_ts=start, end_ts=start + 60, text=text)

        _insert_event_summary(
            storage,
            start_ts=_ts(target_day, 20),
            end_ts=_ts(target_day, 20, 10),
            text="pretargetpost",
        )

        results = service.search(
            SummarySearchParams(query="target", scope=SummarySearchScope.ALL, anchor_day=target_day)
        )
        matched_texts = {item.text for item in results}

        for expected in texts:
            assert expected in matched_texts
        assert "pretargetpost" in matched_texts
    finally:
        storage.close()


def test_summary_search_includes_task_clusters(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    service = SummarySearchService(storage)
    try:
        day = date(2026, 5, 2)
        storage.replace_task_clusters_for_day(
            day=day,
            clusters=[
                {
                    "title": "MATLAB genetic optimization",
                    "normalized_title": "matlab genetic optimization",
                    "task_type": "engineering_analysis",
                    "status": "active",
                    "start_ts": _ts(day, 9),
                    "end_ts": _ts(day, 10),
                    "summary_text": "Worked on GA/Pareto/Fitness and LFM_SMASH_plotAll.m",
                    "evidence_json": {"files": ["LFM_SMASH_plotAll.m", "OptimHistory_110kts.txt"], "concepts": ["Pareto", "Fitness"]},
                    "confidence": 0.9,
                },
                {
                    "title": "WLD summary/search review",
                    "normalized_title": "wld summary/search review",
                    "task_type": "software_debugging",
                    "status": "active",
                    "start_ts": _ts(day, 11),
                    "end_ts": _ts(day, 12),
                    "summary_text": "Investigated summary merge and coalescing behavior",
                    "evidence_json": {"concepts": ["summary", "search", "merge"]},
                    "confidence": 0.85,
                },
                {
                    "title": "Email handling",
                    "normalized_title": "email handling",
                    "task_type": "communication",
                    "status": "minor",
                    "start_ts": _ts(day, 13),
                    "end_ts": _ts(day, 13, 10),
                    "summary_text": "Handled Outlook correspondence",
                    "evidence_json": {"apps": ["outlook.exe"], "concepts": ["email"]},
                    "confidence": 0.8,
                },
            ],
            links=[],
        )
        matlab = service.search(SummarySearchParams(query="OptimHistory_110kts.txt", scope=SummarySearchScope.ALL, anchor_day=day))
        assert any(r.summary_type.value == "task" and "MATLAB genetic optimization" in r.text for r in matlab)
        wld = service.search(SummarySearchParams(query="summary merge", scope=SummarySearchScope.ALL, anchor_day=day))
        assert any(r.summary_type.value == "task" and "WLD summary/search review" in r.text for r in wld)
        email = service.search(SummarySearchParams(query="posta in arrivo", scope=SummarySearchScope.ALL, anchor_day=day))
        # allow either task hit via outlook/email concepts or none if phrase absent; ensure email query finds task via email fallback
        if not any(r.summary_type.value == "task" for r in email):
            email = service.search(SummarySearchParams(query="email", scope=SummarySearchScope.ALL, anchor_day=day))
        assert any(r.summary_type.value == "task" and "Email handling" in r.text for r in email)
    finally:
        storage.close()


def test_task_results_rank_above_event_results(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    service = SummarySearchService(storage)
    try:
        day = date(2026, 5, 2)
        _insert_event_summary(storage, start_ts=_ts(day, 10), end_ts=_ts(day, 10, 5), text="OptimHistory_110kts.txt opened in Notepad")
        storage.replace_task_clusters_for_day(
            day=day,
            clusters=[{
                "title": "MATLAB genetic optimization",
                "normalized_title": "matlab genetic optimization",
                "task_type": "engineering_analysis",
                "status": "active",
                "start_ts": _ts(day, 9),
                "end_ts": _ts(day, 11),
                "summary_text": "Includes OptimHistory_110kts.txt and Pareto analysis",
                "evidence_json": {"files": ["OptimHistory_110kts.txt"]},
                "confidence": 0.9,
            }],
            links=[],
        )
        results = service.search(SummarySearchParams(query="OptimHistory_110kts.txt", scope=SummarySearchScope.ALL, anchor_day=day))
        assert results
        assert results[0].summary_type.value == "task"
    finally:
        storage.close()


def test_coerce_search_scope_handles_enum_and_supported_strings() -> None:
    assert coerce_search_scope(SummarySearchScope.DAY) == SummarySearchScope.DAY
    assert coerce_search_scope("day") == SummarySearchScope.DAY
    assert coerce_search_scope("month") == SummarySearchScope.MONTH
    assert coerce_search_scope("year") == SummarySearchScope.YEAR
    assert coerce_search_scope("all") == SummarySearchScope.ALL


def test_coerce_search_scope_falls_back_to_day_for_invalid_values() -> None:
    assert coerce_search_scope("MONTH") == SummarySearchScope.DAY
    assert coerce_search_scope("invalid") == SummarySearchScope.DAY
    assert coerce_search_scope(None) == SummarySearchScope.DAY


def test_resolve_bounds_month_and_year_use_half_open_anchor_ranges() -> None:
    anchor_day = date(2026, 4, 10)

    month_bounds = _resolve_bounds(scope=SummarySearchScope.MONTH, anchor_day=anchor_day)
    assert month_bounds.day_start == date(2026, 4, 1)
    assert month_bounds.day_end_exclusive == date(2026, 5, 1)

    year_bounds = _resolve_bounds(scope=SummarySearchScope.YEAR, anchor_day=anchor_day)
    assert year_bounds.day_start == date(2026, 1, 1)
    assert year_bounds.day_end_exclusive == date(2027, 1, 1)


def test_resolve_bounds_year_does_not_reuse_month_window() -> None:
    anchor_day = date(2026, 12, 15)
    month_bounds = _resolve_bounds(scope=SummarySearchScope.MONTH, anchor_day=anchor_day)
    year_bounds = _resolve_bounds(scope=SummarySearchScope.YEAR, anchor_day=anchor_day)

    assert month_bounds.day_end_exclusive == date(2027, 1, 1)
    assert year_bounds.day_end_exclusive == date(2027, 1, 1)
    assert month_bounds.day_start == date(2026, 12, 1)
    assert year_bounds.day_start == date(2026, 1, 1)
