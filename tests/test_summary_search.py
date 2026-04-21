from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path

from worklog_diary.core.storage import SQLiteStorage
from worklog_diary.core.summary_search import SummarySearchParams, SummarySearchScope, SummarySearchService
from worklog_diary.ui.summaries_view_model import format_summary_html


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
