from __future__ import annotations

from datetime import date

from worklog_diary.core.models import DailySummaryRecord, SummaryRecord
from worklog_diary.ui.summaries_view_model import (
    build_calendar_highlight_days,
    build_day_summary_view,
    build_summary_card_view,
)


def test_build_calendar_highlight_days_deduplicates_days() -> None:
    day = date(2026, 4, 10)
    highlighted = build_calendar_highlight_days([day, day, date(2026, 4, 11)])
    assert highlighted == {date(2026, 4, 10), date(2026, 4, 11)}


def test_build_summary_card_view_extracts_structured_fields() -> None:
    record = SummaryRecord(
        id=1,
        job_id=11,
        start_ts=100.0,
        end_ts=200.0,
        summary_text="worked on recap feature",
        summary_json={
            "key_points": ["implemented calendar view", "added recap button"],
            "blocked_activity": ["browser activity excluded"],
            "notes": "minor uncertainty around model verbosity",
        },
        created_ts=300.0,
    )
    card = build_summary_card_view(record)

    assert card.summary_text == "worked on recap feature"
    assert card.major_activities == ["implemented calendar view", "added recap button"]
    assert card.blocked_notes == ["browser activity excluded"]
    assert card.uncertainty_notes == ["minor uncertainty around model verbosity"]


def test_build_day_summary_view_combines_day_cards_and_recap_state() -> None:
    target_day = date(2026, 4, 10)
    summaries = [
        SummaryRecord(
            id=10,
            job_id=1,
            start_ts=100.0,
            end_ts=120.0,
            summary_text="older",
            summary_json={"key_points": ["x"]},
            created_ts=130.0,
        ),
        SummaryRecord(
            id=11,
            job_id=1,
            start_ts=200.0,
            end_ts=220.0,
            summary_text="newer",
            summary_json={"key_points": ["y"]},
            created_ts=230.0,
        ),
    ]
    daily = DailySummaryRecord(
        id=5,
        day=target_day,
        recap_text="- did x",
        recap_json={"major_activities": ["did x"]},
        source_batch_count=1,
        created_ts=140.0,
    )
    view = build_day_summary_view(day=target_day, summaries=summaries, daily_summary=daily)

    assert view.day == target_day
    assert len(view.cards) == 2
    assert [card.summary_text for card in view.cards] == ["newer", "older"]
    assert view.has_daily_recap is True
    assert view.daily_recap_text == "- did x"
    assert view.daily_recap_created_label is not None
