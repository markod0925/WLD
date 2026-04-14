from __future__ import annotations

from pathlib import Path

from worklog_diary.core.storage import SQLiteStorage
from worklog_diary.core.models import ScreenshotRecord


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
