from __future__ import annotations

import logging
import sqlite3
import threading
import time

from .capture_repository import CaptureRepository


class StorageDiagnosticsRepository:
    def __init__(
        self,
        conn: sqlite3.Connection,
        lock: threading.Lock,
        logger: logging.Logger,
        capture_repository: CaptureRepository,
    ) -> None:
        self._conn = conn
        self._lock = lock
        self._logger = logger
        self._capture_repository = capture_repository

    def get_pending_counts(self) -> dict[str, int]:
        started_at = time.perf_counter()
        with self._lock:
            intervals = int(
                self._conn.execute(
                    "SELECT COUNT(1) AS c FROM active_intervals WHERE summarized = 0 AND end_ts IS NOT NULL"
                ).fetchone()["c"]
            )
            capture_counts = self._capture_repository._pending_counts_locked()

        result = {"intervals": intervals, **capture_counts}
        self._log_db_query_timing("get_pending_counts", started_at, rows=5)
        return result

    def count_unprocessed_key_events(self) -> int:
        started_at = time.perf_counter()
        with self._lock:
            count = self._capture_repository._count_unprocessed_key_events_locked()
        self._log_db_query_timing("count_unprocessed_key_events", started_at, rows=1)
        return count

    def get_diagnostics_snapshot(self) -> dict:
        started_at = time.perf_counter()
        with self._lock:
            table_counts = {
                "active_intervals": int(self._conn.execute("SELECT COUNT(1) AS c FROM active_intervals").fetchone()["c"]),
                "blocked_intervals": int(self._conn.execute("SELECT COUNT(1) AS c FROM blocked_intervals").fetchone()["c"]),
                "key_events": int(self._conn.execute("SELECT COUNT(1) AS c FROM key_events").fetchone()["c"]),
                "text_segments": int(self._conn.execute("SELECT COUNT(1) AS c FROM text_segments").fetchone()["c"]),
                "screenshots": int(self._conn.execute("SELECT COUNT(1) AS c FROM screenshots").fetchone()["c"]),
                "summary_jobs": int(self._conn.execute("SELECT COUNT(1) AS c FROM summary_jobs").fetchone()["c"]),
                "summaries": int(self._conn.execute("SELECT COUNT(1) AS c FROM summaries").fetchone()["c"]),
                "daily_summaries": int(self._conn.execute("SELECT COUNT(1) AS c FROM daily_summaries").fetchone()["c"]),
            }
            pending_ranges = {
                "active_intervals_unsummarized": _query_range(
                    self._conn,
                    "FROM active_intervals WHERE summarized = 0 AND end_ts IS NOT NULL",
                ),
                "blocked_intervals_unsummarized": _query_range(
                    self._conn,
                    "FROM blocked_intervals WHERE summarized = 0 AND end_ts IS NOT NULL",
                ),
                "key_events_unprocessed": _query_range(
                    self._conn,
                    "FROM key_events WHERE processed = 0",
                    start_column="ts",
                    end_column="ts",
                ),
                "text_segments_pending": _query_range(
                    self._conn,
                    "FROM text_segments",
                ),
                "screenshots_pending": _query_range(
                    self._conn,
                    "FROM screenshots",
                    start_column="ts",
                    end_column="ts",
                ),
            }
            summary_jobs = {
                "queued": int(
                    self._conn.execute("SELECT COUNT(1) AS c FROM summary_jobs WHERE status = 'queued'").fetchone()["c"]
                ),
                "running": int(
                    self._conn.execute("SELECT COUNT(1) AS c FROM summary_jobs WHERE status = 'running'").fetchone()["c"]
                ),
                "completed": int(
                    self._conn.execute(
                        "SELECT COUNT(1) AS c FROM summary_jobs WHERE status IN ('completed', 'succeeded')"
                    ).fetchone()["c"]
                ),
                "failed": int(
                    self._conn.execute("SELECT COUNT(1) AS c FROM summary_jobs WHERE status = 'failed'").fetchone()["c"]
                ),
                "timed_out": int(
                    self._conn.execute("SELECT COUNT(1) AS c FROM summary_jobs WHERE status = 'timed_out'").fetchone()["c"]
                ),
                "succeeded": int(
                    self._conn.execute(
                        "SELECT COUNT(1) AS c FROM summary_jobs WHERE status IN ('completed', 'succeeded')"
                    ).fetchone()["c"]
                ),
                "cancelled": int(
                    self._conn.execute("SELECT COUNT(1) AS c FROM summary_jobs WHERE status = 'cancelled'").fetchone()["c"]
                ),
                "abandoned": int(
                    self._conn.execute("SELECT COUNT(1) AS c FROM summary_jobs WHERE status = 'abandoned'").fetchone()["c"]
                ),
            }

        result = {
            "table_counts": table_counts,
            "pending_counts": self.get_pending_counts(),
            "pending_ranges": pending_ranges,
            "summary_jobs": summary_jobs,
            "daily_summaries": table_counts["daily_summaries"],
        }
        self._log_db_query_timing("get_diagnostics_snapshot", started_at)
        return result

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


def _query_range(
    conn: sqlite3.Connection,
    from_clause: str,
    *,
    start_column: str = "start_ts",
    end_column: str = "end_ts",
) -> dict[str, float | int] | None:
    row = conn.execute(
        f"SELECT MIN({start_column}) AS start_ts, MAX(COALESCE({end_column}, {start_column})) AS end_ts, COUNT(1) AS c {from_clause}"
    ).fetchone()
    if row is None or int(row["c"]) == 0:
        return None
    return {
        "count": int(row["c"]),
        "start_ts": float(row["start_ts"]),
        "end_ts": float(row["end_ts"]),
    }
