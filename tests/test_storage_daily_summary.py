from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path

from worklog_diary.core.storage import SQLiteStorage


def _ts(day: date, hour: int, minute: int = 0) -> float:
    return datetime.combine(day, time(hour=hour, minute=minute)).timestamp()


def _insert_summary(storage: SQLiteStorage, *, start_ts: float, end_ts: float, text: str) -> None:
    job_id = storage.create_summary_job(start_ts=start_ts, end_ts=end_ts, status="succeeded")
    storage.insert_summary(
        job_id=job_id,
        start_ts=start_ts,
        end_ts=end_ts,
        summary_text=text,
        summary_json={"summary_text": text, "key_points": []},
    )


def test_list_summary_days_and_summaries_for_selected_day(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    try:
        day_a = date(2026, 4, 9)
        day_b = date(2026, 4, 10)
        _insert_summary(storage, start_ts=_ts(day_a, 9), end_ts=_ts(day_a, 9, 10), text="A1")
        _insert_summary(storage, start_ts=_ts(day_a, 15), end_ts=_ts(day_a, 15, 30), text="A2")
        _insert_summary(storage, start_ts=_ts(day_b, 11), end_ts=_ts(day_b, 11, 15), text="B1")

        days = storage.list_summary_days()
        assert days[:2] == [day_b, day_a]

        summaries_day_a = storage.list_summaries_for_day(day_a)
        assert [item.summary_text for item in summaries_day_a] == ["A1", "A2"]
        assert storage.count_batch_summaries_for_day(day_a) == 2

        empty = storage.list_summaries_for_day(date(2026, 4, 11))
        assert empty == []
    finally:
        storage.close()


def test_create_daily_summary_overwrites_existing_day_deterministically(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    try:
        target_day = date(2026, 4, 10)
        first, replaced_first = storage.create_daily_summary(
            day=target_day,
            recap_text="- initial recap",
            recap_json={"recap_text": "- initial recap", "major_activities": ["initial"]},
            source_batch_count=2,
        )
        assert replaced_first is False
        assert first.source_batch_count == 2

        second, replaced_second = storage.create_daily_summary(
            day=target_day,
            recap_text="- regenerated recap",
            recap_json={"recap_text": "- regenerated recap", "major_activities": ["regen"]},
            source_batch_count=4,
        )
        assert replaced_second is True
        assert second.day == target_day
        assert second.source_batch_count == 4

        fetched = storage.get_daily_summary_for_day(target_day)
        assert fetched is not None
        assert fetched.recap_text == "- regenerated recap"
        assert fetched.source_batch_count == 4
    finally:
        storage.close()
