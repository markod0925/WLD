from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path
import time as time_module

from worklog_diary.core.batching import BatchBuilder
from worklog_diary.core.models import ForegroundInfo, KeyEvent, ScreenshotRecord, TextSegment
from worklog_diary.core.llm_job_queue import LLMJobMetadata
from worklog_diary.core.storage import SQLiteStorage
from worklog_diary.core.summarizer import Summarizer


class SuccessfulClient:
    def summarize_batch(self, *_args: object, **_kwargs: object) -> tuple[str, dict]:
        _emit_started(_kwargs)
        return "done", {"summary_text": "done", "key_points": [], "blocked_activity": []}

    def summarize_daily_recap(self, *_args: object, **_kwargs: object) -> tuple[str, dict]:
        _emit_started(_kwargs)
        return "daily done", {"summary_text": "daily done", "major_activities": []}


class FailingClient:
    def summarize_batch(self, *_args: object, **_kwargs: object) -> tuple[str, dict]:
        _emit_started(_kwargs)
        raise RuntimeError("LM Studio unavailable")

    def summarize_daily_recap(self, *_args: object, **_kwargs: object) -> tuple[str, dict]:
        _emit_started(_kwargs)
        raise RuntimeError("LM Studio unavailable")


def _emit_started(kwargs: dict[str, object]) -> None:
    on_started = kwargs.get("on_started")
    if not callable(on_started):
        return
    metadata = LLMJobMetadata(
        job_id=kwargs.get("job_id", "test-job"),
        job_type=str(kwargs.get("job_type", "event_summary")),
        queued_at=time_module.time() - 0.001,
        started_at=time_module.time(),
        timeout_s=float(kwargs.get("timeout_s", 600)),
        attempt=int(kwargs.get("attempt", 1)),
        input_chars=int(kwargs.get("input_chars", 0)),
        input_token_estimate=kwargs.get("input_token_estimate"),
        priority=int(kwargs.get("priority", 100)),
    )
    on_started(metadata)


def _seed_raw_data(storage: SQLiteStorage, screenshot_path: Path) -> None:
    info = ForegroundInfo(
        timestamp=10.0,
        hwnd=100,
        pid=200,
        process_name="code.exe",
        window_title="Editor",
    )
    interval_id = storage.start_interval(info, blocked=False)
    storage.close_interval(interval_id, end_ts=20.0)

    key_id = storage.insert_key_event(
        KeyEvent(
            id=None,
            ts=12.0,
            key="a",
            event_type="down",
            modifiers=[],
            process_name="code.exe",
            window_title="Editor",
            hwnd=100,
            active_interval_id=interval_id,
            processed=False,
        )
    )
    storage.mark_key_events_processed([key_id])

    storage.insert_text_segments(
        [
            TextSegment(
                id=None,
                start_ts=12.0,
                end_ts=13.0,
                process_name="code.exe",
                window_title="Editor",
                text="a",
                hotkeys=[],
                raw_key_count=1,
            )
        ]
    )

    screenshot_path.parent.mkdir(parents=True, exist_ok=True)
    screenshot_path.write_bytes(b"fake-image")
    storage.insert_screenshot(
        ScreenshotRecord(
            id=None,
            ts=14.0,
            file_path=str(screenshot_path),
            process_name="code.exe",
            window_title="Editor",
            active_interval_id=interval_id,
        )
    )


def _seed_daily_summary_source(storage: SQLiteStorage, *, day_start_ts: float, day_end_ts: float, text: str) -> None:
    job_id = storage.create_summary_job(start_ts=day_start_ts, end_ts=day_end_ts, status="completed")
    storage.insert_summary(
        job_id=job_id,
        start_ts=day_start_ts,
        end_ts=day_end_ts,
        summary_text=text,
        summary_json={"summary_text": text, "key_points": []},
    )


def _latest_summary_job(storage: SQLiteStorage) -> dict[str, object]:
    row = storage._conn.execute("SELECT MAX(id) AS id FROM summary_jobs").fetchone()  # noqa: SLF001
    assert row is not None and row["id"] is not None
    job = storage.get_summary_job(int(row["id"]))
    assert job is not None
    return job


def test_successful_summary_purges_db_rows_and_screenshot_files(tmp_path: Path) -> None:
    db_path = tmp_path / "worklog.db"
    shot_path = tmp_path / "screens" / "shot.png"
    storage = SQLiteStorage(str(db_path))
    _seed_raw_data(storage, shot_path)

    summarizer = Summarizer(
        storage=storage,
        batch_builder=BatchBuilder(storage=storage, max_text_segments=200, max_screenshots=3),
        lm_client=SuccessfulClient(),
    )
    summary_id = summarizer.flush_pending(reason="test")

    diagnostics = storage.get_diagnostics_snapshot()
    assert summary_id is not None
    assert diagnostics["table_counts"]["key_events"] == 0
    assert diagnostics["table_counts"]["text_segments"] == 0
    assert diagnostics["table_counts"]["screenshots"] == 0
    assert diagnostics["pending_counts"]["intervals"] == 0
    assert shot_path.exists() is False
    summarizer.stop()
    storage.close()


def test_successful_summary_records_worker_timestamps(tmp_path: Path) -> None:
    db_path = tmp_path / "worklog.db"
    shot_path = tmp_path / "screens" / "shot.png"
    storage = SQLiteStorage(str(db_path))
    _seed_raw_data(storage, shot_path)

    summarizer = Summarizer(
        storage=storage,
        batch_builder=BatchBuilder(storage=storage, max_text_segments=200, max_screenshots=3),
        lm_client=SuccessfulClient(),
    )
    try:
        assert summarizer.flush_pending(reason="test") is not None
        job = _latest_summary_job(storage)
        assert job["status"] == "completed"
        assert job["queued_at"] <= job["started_at"] <= job["finished_at"]
        assert job["finished_at"] >= job["started_at"]
    finally:
        summarizer.stop()
        storage.close()


def test_failed_summary_keeps_raw_data_retryable(tmp_path: Path) -> None:
    db_path = tmp_path / "worklog.db"
    shot_path = tmp_path / "screens" / "shot.png"
    storage = SQLiteStorage(str(db_path))
    _seed_raw_data(storage, shot_path)

    summarizer = Summarizer(
        storage=storage,
        batch_builder=BatchBuilder(storage=storage, max_text_segments=200, max_screenshots=3),
        lm_client=FailingClient(),
    )
    summary_id = summarizer.flush_pending(reason="test")

    diagnostics = storage.get_diagnostics_snapshot()
    assert summary_id is None
    assert diagnostics["table_counts"]["key_events"] == 1
    assert diagnostics["table_counts"]["text_segments"] == 1
    assert diagnostics["table_counts"]["screenshots"] == 1
    assert diagnostics["summary_jobs"]["failed"] == 1
    assert shot_path.exists() is True
    summarizer.stop()
    storage.close()


def test_failed_summary_marks_terminal_state_and_finished_at(tmp_path: Path) -> None:
    db_path = tmp_path / "worklog.db"
    shot_path = tmp_path / "screens" / "shot.png"
    storage = SQLiteStorage(str(db_path))
    _seed_raw_data(storage, shot_path)

    summarizer = Summarizer(
        storage=storage,
        batch_builder=BatchBuilder(storage=storage, max_text_segments=200, max_screenshots=3),
        lm_client=FailingClient(),
    )
    try:
        assert summarizer.flush_pending(reason="test") is None
        job = _latest_summary_job(storage)
        assert job["status"] == "failed"
        assert job["finished_at"] is not None
        assert job["started_at"] is not None
        assert job["finished_at"] >= job["started_at"]
    finally:
        summarizer.stop()
        storage.close()


def test_restart_recovery_marks_stale_jobs_terminal(tmp_path: Path) -> None:
    db_path = tmp_path / "worklog.db"
    storage = SQLiteStorage(str(db_path))

    blocked_info = ForegroundInfo(
        timestamp=100.0,
        hwnd=500,
        pid=600,
        process_name="chrome.exe",
        window_title="Private",
    )
    storage.start_interval(blocked_info, blocked=True)
    storage.create_summary_job(start_ts=100.0, end_ts=101.0, status="running")
    storage.create_summary_job(start_ts=102.0, end_ts=103.0, status="queued")
    storage.close()

    reopened = SQLiteStorage(str(db_path))
    diagnostics = reopened.get_diagnostics_snapshot()

    assert diagnostics["summary_jobs"]["running"] == 0
    assert diagnostics["summary_jobs"]["abandoned"] == 1
    assert diagnostics["summary_jobs"]["cancelled"] == 1
    assert len(reopened.fetch_unsummarized_intervals()) == 1
    assert len(reopened.fetch_unsummarized_blocked_intervals()) == 1
    reopened.close()


def test_daily_summary_is_idempotent_per_day(tmp_path: Path) -> None:
    db_path = tmp_path / "worklog.db"
    storage = SQLiteStorage(str(db_path))
    day = date(2026, 4, 10)
    day_start_ts = datetime.combine(day, time()).timestamp()
    day_end_ts = day_start_ts + 86400.0
    _seed_daily_summary_source(storage, day_start_ts=day_start_ts, day_end_ts=day_end_ts, text="A1")
    _seed_daily_summary_source(storage, day_start_ts=day_start_ts + 10, day_end_ts=day_end_ts, text="A2")

    summarizer = Summarizer(
        storage=storage,
        batch_builder=BatchBuilder(storage=storage, max_text_segments=200, max_screenshots=3),
        lm_client=SuccessfulClient(),
    )
    try:
        first_id, first_replaced = summarizer.generate_daily_recap_for_day(day)
        second_id, second_replaced = summarizer.generate_daily_recap_for_day(day)

        assert first_id == second_id
        assert first_replaced is False
        assert second_replaced is False
        job = storage.get_daily_summary_job_for_day(day)
        assert job is not None
        assert job["status"] == "completed"
        assert job["attempt"] == 1
        assert storage.get_diagnostics_snapshot()["daily_summaries"] == 1
    finally:
        summarizer.stop()
        storage.close()
