from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from pathlib import Path

from .storage_logging import log_db_query_timing


class StorageCleanupService:
    """Clean up raw activity rows and associated screenshot files."""

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
        candidate_files: list[Path] = []
        candidate_keys_to_path: dict[str, Path] = {}
        for directory in candidate_dirs:
            if not directory.exists():
                continue
            for file_path in directory.glob("*"):
                if file_path.suffix.lower() not in {".png", ".jpg", ".jpeg"}:
                    continue
                candidate_files.append(file_path)
                candidate_keys_to_path[_normalized_path_key(str(file_path))] = file_path
        if not candidate_files:
            return 0

        with self._lock:
            referenced_paths: set[str] = set()
            key_values = list(candidate_keys_to_path.keys())
            chunk_size = 500
            for start in range(0, len(key_values), chunk_size):
                chunk = key_values[start : start + chunk_size]
                placeholders = ",".join(["?"] * len(chunk))
                chunk_basenames = sorted({Path(key).name.lower() for key in chunk})
                basename_filters = " OR ".join(["lower(file_path) LIKE ? OR lower(file_path) LIKE ?" for _ in chunk_basenames])
                where_clause = f"lower(file_path) IN ({placeholders})"
                if basename_filters:
                    where_clause = f"{where_clause} OR {basename_filters}"
                params: list[str] = [key.lower() for key in chunk]
                for basename in chunk_basenames:
                    params.append(f"%/{basename}")
                    params.append(f"%\\{basename}")
                rows = self._conn.execute(
                    (
                        "SELECT file_path FROM screenshots "
                        f"WHERE {where_clause}"
                    ),
                    tuple(params),
                ).fetchall()
                referenced_paths.update(_normalized_path_key(str(row["file_path"])) for row in rows)

        removed = 0
        failed = 0
        for file_path in candidate_files:
            if _normalized_path_key(str(file_path)) in referenced_paths:
                continue
            try:
                os.remove(file_path)
                removed += 1
            except Exception as exc:
                failed += 1
                self._logger.warning(
                    "[CRASH] stage=orphan_file_delete status=error pid=%s thread=%s error_type=%s error=%s path=%s",
                    os.getpid(),
                    threading.current_thread().name,
                    exc.__class__.__name__,
                    exc,
                    file_path,
                )
                continue
        if failed:
            self._logger.warning("[CRASH] stage=orphan_cleanup status=error pid=%s failed_deletes=%s", os.getpid(), failed)
        return removed

    def _log_db_query_timing(self, operation: str, started_at: float, *, rows: int | None = None) -> None:
        log_db_query_timing(self._logger, operation, started_at, rows=rows)


def _normalized_path_key(path: str) -> str:
    return os.path.normcase(os.path.abspath(path))
