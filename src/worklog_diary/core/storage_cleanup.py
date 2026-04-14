from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from pathlib import Path


class StorageCleanupService:
    def __init__(self, conn: sqlite3.Connection, lock: threading.Lock, logger: logging.Logger) -> None:
        self._conn = conn
        self._lock = lock
        self._logger = logger

    def purge_raw_data(self, start_ts: float, end_ts: float) -> list[str]:
        started_at = time.perf_counter()
        screenshot_paths: list[str] = []
        with self._lock:
            rows = self._conn.execute(
                "SELECT file_path FROM screenshots WHERE ts >= ? AND ts <= ?",
                (start_ts, end_ts),
            ).fetchall()
            screenshot_paths = [str(row["file_path"]) for row in rows]

            deleted_key_events = self._conn.execute(
                "DELETE FROM key_events WHERE processed = 1 AND ts >= ? AND ts <= ?",
                (start_ts, end_ts),
            ).rowcount
            deleted_segments = self._conn.execute(
                "DELETE FROM text_segments WHERE start_ts >= ? AND end_ts <= ?",
                (start_ts, end_ts),
            ).rowcount
            deleted_screenshots = self._conn.execute(
                "DELETE FROM screenshots WHERE ts >= ? AND ts <= ?",
                (start_ts, end_ts),
            ).rowcount
            self._conn.commit()

        removed_count = 0
        missing_count = 0
        failed_paths: list[str] = []
        for path in screenshot_paths:
            try:
                os.remove(path)
                removed_count += 1
            except FileNotFoundError:
                missing_count += 1
            except Exception:
                failed_paths.append(path)

        orphan_removed = self._cleanup_orphaned_screenshot_files({Path(path).parent for path in screenshot_paths})
        self._logger.info(
            (
                "event=purge_actions start_ts=%.3f end_ts=%.3f "
                "deleted_key_events=%s deleted_text_segments=%s deleted_screenshots=%s "
                "removed_files=%s missing_files=%s failed_file_deletes=%s orphan_files_removed=%s"
            ),
            start_ts,
            end_ts,
            deleted_key_events,
            deleted_segments,
            deleted_screenshots,
            removed_count,
            missing_count,
            len(failed_paths),
            orphan_removed,
        )
        if failed_paths:
            self._logger.warning("event=purge_file_delete_failed paths=%s", failed_paths)
        self._log_db_query_timing(
            "purge_raw_data",
            started_at,
            rows=deleted_key_events + deleted_segments + deleted_screenshots,
        )
        return screenshot_paths

    def _cleanup_orphaned_screenshot_files(self, candidate_dirs: set[Path]) -> int:
        if not candidate_dirs:
            return 0
        with self._lock:
            rows = self._conn.execute("SELECT file_path FROM screenshots").fetchall()
            referenced_paths = {Path(str(row["file_path"])) for row in rows}

        removed = 0
        for directory in candidate_dirs:
            if not directory.exists():
                continue
            for file_path in directory.glob("*"):
                if file_path.suffix.lower() not in {".png", ".jpg", ".jpeg"}:
                    continue
                if file_path in referenced_paths:
                    continue
                try:
                    os.remove(file_path)
                    removed += 1
                except Exception:
                    continue
        return removed

    def _log_db_query_timing(self, operation: str, started_at: float, *, rows: int | None = None) -> None:
        duration_ms = (time.perf_counter() - started_at) * 1000.0
        if rows is None:
            self._logger.info("event=db_query_timing operation=%s duration_ms=%.3f", operation, duration_ms)
        else:
            self._logger.info(
                "event=db_query_timing operation=%s duration_ms=%.3f rows=%s",
                operation,
                duration_ms,
                rows,
            )
