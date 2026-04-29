from __future__ import annotations

import os
from pathlib import Path
from datetime import date

import worklog_diary.core.activity_repository as activity_repository_module
import worklog_diary.core.capture_repository as capture_repository_module
import worklog_diary.core.storage_cleanup as storage_cleanup_module
from worklog_diary.core.models import ForegroundInfo, KeyEvent, ScreenshotRecord, TextSegment
from worklog_diary.core.storage import SQLiteStorage
import worklog_diary.core.summary_repository as summary_repository_module


def test_storage_bootstrap_enables_wal(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    try:
        row = storage._conn.execute("PRAGMA journal_mode").fetchone()
        assert row is not None
        assert str(row[0]).lower() == "wal"
    finally:
        storage.close()


def test_storage_diagnostics_delegates_to_repository(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    try:
        assert storage.get_pending_counts() == storage.diagnostics_repository.get_pending_counts()
        assert storage.get_diagnostics_snapshot() == storage.diagnostics_repository.get_diagnostics_snapshot()
    finally:
        storage.close()


def test_storage_capture_methods_delegate_to_repository(tmp_path: Path, monkeypatch) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    captured: dict[str, object] = {}

    def fake_insert_key_event(self, event):
        captured["insert_key_event_self"] = self
        captured["insert_key_event_arg"] = event
        return 321

    def fake_fetch_unsummarized_screenshots(self, limit: int = 20):
        captured["fetch_unsummarized_screenshots_self"] = self
        captured["fetch_unsummarized_screenshots_limit"] = limit
        return []

    monkeypatch.setattr(capture_repository_module.CaptureRepository, "insert_key_event", fake_insert_key_event)
    monkeypatch.setattr(
        capture_repository_module.CaptureRepository,
        "fetch_unsummarized_screenshots",
        fake_fetch_unsummarized_screenshots,
    )
    try:
        key_event_id = storage.insert_key_event(
            KeyEvent(
                id=None,
                ts=1.0,
                key="a",
                event_type="down",
                modifiers=[],
                process_name="code.exe",
                window_title="Editor",
                hwnd=42,
                active_interval_id=None,
            )
        )
        screenshots = storage.fetch_unsummarized_screenshots(limit=7)

        assert key_event_id == 321
        assert captured["insert_key_event_self"] is storage.capture_repository
        assert captured["insert_key_event_arg"].key == "a"
        assert captured["fetch_unsummarized_screenshots_self"] is storage.capture_repository
        assert captured["fetch_unsummarized_screenshots_limit"] == 7
        assert screenshots == []
    finally:
        storage.close()


def test_storage_activity_methods_delegate_to_repository(tmp_path: Path, monkeypatch) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    captured: dict[str, object] = {}

    def fake_start_interval(self, info, blocked: bool):
        captured["start_self"] = self
        captured["start_info"] = info
        captured["start_blocked"] = blocked
        return 123

    def fake_close_interval(self, interval_id: int, end_ts: float) -> None:
        captured["close_self"] = self
        captured["close_interval_id"] = interval_id
        captured["close_end_ts"] = end_ts

    def fake_fetch_unsummarized_intervals(self, limit: int = 10000):
        captured["fetch_self"] = self
        captured["fetch_limit"] = limit
        return []

    def fake_fetch_unsummarized_blocked_intervals(self, limit: int = 10000):
        captured["fetch_blocked_self"] = self
        captured["fetch_blocked_limit"] = limit
        return []

    def fake_mark_intervals_summarized(self, start_ts: float, end_ts: float) -> None:
        captured["mark_self"] = self
        captured["mark_args"] = (start_ts, end_ts)

    monkeypatch.setattr(activity_repository_module.SQLiteActivityRepository, "start_interval", fake_start_interval)
    monkeypatch.setattr(activity_repository_module.SQLiteActivityRepository, "close_interval", fake_close_interval)
    monkeypatch.setattr(
        activity_repository_module.SQLiteActivityRepository,
        "fetch_unsummarized_intervals",
        fake_fetch_unsummarized_intervals,
    )
    monkeypatch.setattr(
        activity_repository_module.SQLiteActivityRepository,
        "fetch_unsummarized_blocked_intervals",
        fake_fetch_unsummarized_blocked_intervals,
    )
    monkeypatch.setattr(
        activity_repository_module.SQLiteActivityRepository,
        "mark_intervals_summarized",
        fake_mark_intervals_summarized,
    )
    try:
        interval_id = storage.start_interval(
            ForegroundInfo(
                timestamp=1.0,
                hwnd=2,
                pid=3,
                process_name="code.exe",
                window_title="Editor",
            ),
            blocked=True,
        )
        storage.close_interval(7, end_ts=9.5)
        intervals = storage.fetch_unsummarized_intervals(limit=4)
        blocked_intervals = storage.fetch_unsummarized_blocked_intervals(limit=5)
        storage.mark_intervals_summarized(10.0, 20.0)

        assert interval_id == 123
        assert captured["start_self"] is storage.activity_repository
        assert captured["start_blocked"] is True
        assert captured["close_self"] is storage.activity_repository
        assert captured["close_interval_id"] == 7
        assert captured["close_end_ts"] == 9.5
        assert captured["fetch_self"] is storage.activity_repository
        assert captured["fetch_limit"] == 4
        assert captured["fetch_blocked_self"] is storage.activity_repository
        assert captured["fetch_blocked_limit"] == 5
        assert captured["mark_self"] is storage.activity_repository
        assert captured["mark_args"] == (10.0, 20.0)
        assert intervals == []
        assert blocked_intervals == []
    finally:
        storage.close()


def test_storage_summary_methods_delegate_to_summary_repository(
    tmp_path: Path, monkeypatch
) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    captured: dict[str, object] = {}

    def fake_create_summary_job(
        self,
        start_ts: float,
        end_ts: float,
        status: str = "queued",
        *,
        job_type: str = "event_summary",
        target_day: date | str | None = None,
        timeout_s: float = 0,
        attempt: int = 1,
        input_chars: int = 0,
        input_token_estimate: int | None = None,
        priority: int = 100,
    ) -> int:
        captured["create_self"] = self
        captured["create_args"] = (
            start_ts,
            end_ts,
            status,
            job_type,
            target_day,
            timeout_s,
            attempt,
            input_chars,
            input_token_estimate,
            priority,
        )
        return 987

    def fake_get_daily_summary_for_day(self, day: date):
        captured["daily_self"] = self
        captured["daily_day"] = day
        return None

    monkeypatch.setattr(summary_repository_module.SummaryRepository, "create_summary_job", fake_create_summary_job)
    monkeypatch.setattr(
        summary_repository_module.SummaryRepository,
        "get_daily_summary_for_day",
        fake_get_daily_summary_for_day,
    )
    try:
        assert storage.create_summary_job(
            1.0,
            2.0,
            status="queued",
            target_day=date(2026, 4, 10),
        ) == 987
        assert captured["create_self"] is storage.summary_repository
        assert captured["create_args"] == (
            1.0,
            2.0,
            "queued",
            "event_summary",
            date(2026, 4, 10),
            0,
            1,
            0,
            None,
            100,
        )

        assert storage.get_daily_summary_for_day(date(2026, 4, 10)) is None
        assert captured["daily_self"] is storage.summary_repository
        assert captured["daily_day"] == date(2026, 4, 10)
    finally:
        storage.close()


def test_storage_cleanup_service_purges_screenshot_files(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    screenshot_path = tmp_path / "shot.png"
    screenshot_path.write_bytes(b"fake image")
    try:
        storage.insert_screenshot(
            ScreenshotRecord(
                id=None,
                ts=10.0,
                file_path=str(screenshot_path),
                process_name="code.exe",
                window_title="Editor",
                active_interval_id=None,
            )
        )

        removed = storage.cleanup_service.purge_raw_data(0.0, 20.0)

        assert removed == [str(screenshot_path)]
        assert not screenshot_path.exists()
        assert storage.fetch_unsummarized_screenshots() == []
    finally:
        storage.close()


def test_storage_roundtrip_persists_screenshot_metadata(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    try:
        screenshot_id = storage.insert_screenshot(
            ScreenshotRecord(
                id=None,
                ts=10.0,
                file_path="shot.png",
                process_name="code.exe",
                window_title="Editor",
                active_interval_id=42,
                window_hwnd=100,
                fingerprint="0123456789abcdef",
            )
        )

        screenshots = storage.fetch_unsummarized_screenshots()

        assert screenshot_id > 0
        assert len(screenshots) == 1
        assert screenshots[0].window_hwnd == 100
        assert screenshots[0].fingerprint == "0123456789abcdef"
    finally:
        storage.close()


def test_storage_cleanup_service_compares_paths_case_insensitively(
    tmp_path: Path, monkeypatch
) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    screenshot_dir = tmp_path / "Shots"
    screenshot_dir.mkdir()
    actual_path = screenshot_dir / "Shot.PNG"
    actual_path.write_bytes(b"fake image")
    try:
        storage.insert_screenshot(
            ScreenshotRecord(
                id=None,
                ts=10.0,
                file_path=str(screenshot_dir / "shot.png"),
                process_name="code.exe",
                window_title="Editor",
                active_interval_id=None,
            )
        )
        monkeypatch.setattr(storage_cleanup_module.os.path, "normcase", lambda value: value.lower())

        removed = storage.cleanup_service._cleanup_orphaned_screenshot_files({screenshot_dir})

        assert removed == 0
        assert actual_path.exists()
    finally:
        storage.close()


def test_storage_roundtrip_persists_counts_across_reopen(tmp_path: Path) -> None:
    db_path = tmp_path / "worklog.db"
    storage = SQLiteStorage(str(db_path))
    info = ForegroundInfo(
        timestamp=10.0,
        hwnd=50,
        pid=60,
        process_name="code.exe",
        window_title="Editor",
    )
    try:
        interval_id = storage.start_interval(info, blocked=False)
        storage.close_interval(interval_id, end_ts=20.0)
        storage.insert_text_segments(
            [
                TextSegment(
                    id=None,
                    start_ts=12.0,
                    end_ts=13.0,
                    process_name="code.exe",
                    window_title="Editor",
                    text="hello",
                    hotkeys=[],
                    raw_key_count=1,
                )
            ]
        )
        storage.create_summary_job(start_ts=10.0, end_ts=20.0, status="running")
    finally:
        storage.close()

    reopened = SQLiteStorage(str(db_path))
    try:
        diagnostics = reopened.get_diagnostics_snapshot()
        assert diagnostics["table_counts"]["active_intervals"] == 1
        assert diagnostics["table_counts"]["text_segments"] == 1
        assert diagnostics["summary_jobs"]["running"] == 0
        assert diagnostics["summary_jobs"]["abandoned"] == 1
    finally:
        reopened.close()


def test_orphan_cleanup_deletes_only_unreferenced_candidates(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    screenshot_dir = tmp_path / "shots"
    other_dir = tmp_path / "other"
    screenshot_dir.mkdir()
    other_dir.mkdir()
    keep = screenshot_dir / "keep.png"
    remove = screenshot_dir / "remove.png"
    outside = other_dir / "outside.png"
    keep.write_bytes(b"img")
    remove.write_bytes(b"img")
    outside.write_bytes(b"img")
    try:
        storage.insert_screenshot(
            ScreenshotRecord(
                id=None,
                ts=10.0,
                file_path=str(keep),
                process_name="code.exe",
                window_title="Editor",
                active_interval_id=None,
            )
        )
        removed = storage.cleanup_service._cleanup_orphaned_screenshot_files({screenshot_dir})
        assert removed == 1
        assert keep.exists()
        assert not remove.exists()
        assert outside.exists()
    finally:
        storage.close()


def test_orphan_cleanup_queries_by_candidate_paths_only(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    screenshot_dir = tmp_path / "shots"
    screenshot_dir.mkdir()
    candidate = screenshot_dir / "candidate.png"
    candidate.write_bytes(b"img")
    executed_sql: list[str] = []

    class _ConnProxy:
        def __init__(self, conn) -> None:
            self._conn = conn
        def execute(self, sql: str, params=()):
            executed_sql.append(sql)
            return self._conn.execute(sql, params)

    storage.cleanup_service._conn = _ConnProxy(storage._conn)  # type: ignore[assignment]
    try:
        storage.cleanup_service._cleanup_orphaned_screenshot_files({screenshot_dir})
    finally:
        storage.close()
    assert any("WHERE lower(file_path) IN" in sql for sql in executed_sql)
    assert not any(sql.strip() == "SELECT file_path FROM screenshots" for sql in executed_sql)


def test_orphan_cleanup_preserves_referenced_file_with_relative_db_path(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    screenshot_dir = tmp_path / "shots"
    screenshot_dir.mkdir()
    keep = screenshot_dir / "keep.png"
    keep.write_bytes(b"img")

    relative_path = Path("shots") / "keep.png"
    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        storage.insert_screenshot(
            ScreenshotRecord(
                id=None,
                ts=10.0,
                file_path=str(relative_path),
                process_name="code.exe",
                window_title="Editor",
                active_interval_id=None,
            )
        )
        removed = storage.cleanup_service._cleanup_orphaned_screenshot_files({screenshot_dir})
        assert removed == 0
        assert keep.exists()
    finally:
        os.chdir(cwd)
        storage.close()
