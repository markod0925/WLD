from __future__ import annotations

import json
from datetime import date, datetime, time
from pathlib import Path

from worklog_diary.core.storage import SQLiteStorage


def _ts(day: date, hour: int, minute: int = 0) -> float:
    return datetime.combine(day, time(hour=hour, minute=minute)).timestamp()


def test_task_centric_schema_round_trip(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    try:
        day = date(2026, 5, 1)
        start_ts = _ts(day, 9)
        end_ts = _ts(day, 9, 15)
        job_id = storage.create_summary_job(start_ts=start_ts, end_ts=end_ts, status="succeeded")
        summary_id = storage.insert_summary(
            job_id=job_id,
            start_ts=start_ts,
            end_ts=end_ts,
            summary_text="MATLAB optimization analysis",
            summary_json={"summary_text": "MATLAB optimization analysis"},
        )

        payload = {"task_candidates": [{"label": "MATLAB genetic optimization", "confidence": 0.92}]}
        storage.update_event_summary_structured_fields(
            summary_id,
            structured_payload_json=payload,
            primary_task_label="MATLAB genetic optimization",
            primary_activity_type="result_analysis",
            is_blocked=False,
            is_low_value=False,
            noise_reason=None,
            confidence=0.92,
        )

        row = storage._conn.execute(
            "SELECT structured_payload_json, primary_task_label, primary_activity_type, confidence FROM summaries WHERE id = ?",
            (summary_id,),
        ).fetchone()
        assert row is not None
        assert json.loads(str(row["structured_payload_json"]))["task_candidates"][0]["label"] == "MATLAB genetic optimization"
        assert row["primary_task_label"] == "MATLAB genetic optimization"

        daily, _ = storage.create_daily_summary(
            day=day,
            recap_text="Daily Summary — 2026-05-01",
            recap_json={"main_tasks": ["MATLAB genetic optimization"]},
            source_batch_count=1,
            structured_payload_json={"main_tasks": [{"title": "MATLAB genetic optimization"}]},
            generated_from_task_clusters=True,
        )
        assert daily.day == day

        daily_row = storage._conn.execute(
            "SELECT generated_from_task_clusters, structured_payload_json FROM daily_summaries WHERE day = ?",
            (day.isoformat(),),
        ).fetchone()
        assert daily_row is not None
        assert int(daily_row["generated_from_task_clusters"]) == 1
        assert "main_tasks" in json.loads(str(daily_row["structured_payload_json"]))
    finally:
        storage.close()
