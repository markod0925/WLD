from __future__ import annotations

from pathlib import Path

from worklog_diary.core.models import ForegroundInfo, TextSegment
from worklog_diary.core.storage import SQLiteStorage
from worklog_diary.core.models import ScreenshotRecord
import worklog_diary.core.storage_cleanup as storage_cleanup_module


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
