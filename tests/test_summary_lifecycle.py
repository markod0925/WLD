from __future__ import annotations

from pathlib import Path

from worklog_diary.core.batching import BatchBuilder
from worklog_diary.core.models import ForegroundInfo, KeyEvent, ScreenshotRecord, TextSegment
from worklog_diary.core.storage import SQLiteStorage
from worklog_diary.core.summarizer import Summarizer


class SuccessfulClient:
    def summarize_batch(self, _batch: object) -> tuple[str, dict]:
        return "done", {"summary_text": "done", "key_points": [], "blocked_activity": []}


class FailingClient:
    def summarize_batch(self, _batch: object) -> tuple[str, dict]:
        raise RuntimeError("LM Studio unavailable")


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
    storage.close()


def test_restart_recovery_closes_open_intervals_and_marks_running_jobs_failed(tmp_path: Path) -> None:
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
    storage.close()

    reopened = SQLiteStorage(str(db_path))
    diagnostics = reopened.get_diagnostics_snapshot()

    assert diagnostics["summary_jobs"]["running"] == 0
    assert diagnostics["summary_jobs"]["failed"] == 1
    assert len(reopened.fetch_unsummarized_intervals()) == 1
    assert len(reopened.fetch_unsummarized_blocked_intervals()) == 1
    reopened.close()
