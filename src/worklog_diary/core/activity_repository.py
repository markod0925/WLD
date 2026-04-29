from __future__ import annotations

import logging
import sqlite3
import threading
import time
from datetime import date
from typing import Protocol

from .models import ActiveInterval, BlockedInterval, ForegroundInfo, ScreenshotRecord, SummaryRecord, TextSegment
from .summary_repository import _LogDbQueryTiming


class ActivityRepository(Protocol):
    def fetch_unsummarized_intervals(self, limit: int = 10000) -> list[ActiveInterval]:
        ...

    def fetch_unsummarized_blocked_intervals(self, limit: int = 10000) -> list[BlockedInterval]:
        ...

    def fetch_unsummarized_text_segments(self, limit: int = 200) -> list[TextSegment]:
        ...

    def fetch_unsummarized_screenshots(self, limit: int = 20) -> list[ScreenshotRecord]:
        ...

    def list_summaries_for_day(self, day: date, limit: int = 500) -> list[SummaryRecord]:
        ...


class SQLiteActivityRepository:
    """SQLite-backed persistence for activity intervals."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        lock: threading.Lock,
        logger: logging.Logger,
        log_db_query_timing: _LogDbQueryTiming,
    ) -> None:
        self._conn = conn
        self._lock = lock
        self._logger = logger
        self._log_db_query_timing = log_db_query_timing

    def start_interval(self, info: ForegroundInfo, blocked: bool) -> int:
        started_at = time.perf_counter()
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO active_intervals(
                    start_ts, end_ts, hwnd, pid, process_name, window_title, blocked, summarized
                ) VALUES (?, NULL, ?, ?, ?, ?, ?, 0)
                """,
                (
                    info.timestamp,
                    info.hwnd,
                    info.pid,
                    info.process_name,
                    info.window_title,
                    int(blocked),
                ),
            )
            self._conn.commit()
            interval_id = int(cursor.lastrowid)
        self._log_db_query_timing("start_interval", started_at, rows=1)
        return interval_id

    def close_interval(self, interval_id: int, end_ts: float) -> None:
        started_at = time.perf_counter()
        with self._lock:
            self._conn.execute(
                "UPDATE active_intervals SET end_ts = ? WHERE id = ? AND end_ts IS NULL",
                (end_ts, interval_id),
            )
            row = self._conn.execute(
                "SELECT start_ts, process_name, window_title, blocked FROM active_intervals WHERE id = ?",
                (interval_id,),
            ).fetchone()
            if row and int(row["blocked"]) == 1:
                self._conn.execute(
                    """
                    INSERT OR IGNORE INTO blocked_intervals(
                        active_interval_id, start_ts, end_ts, process_name, window_title, summarized
                    ) VALUES (?, ?, ?, ?, ?, 0)
                    """,
                    (
                        interval_id,
                        float(row["start_ts"]),
                        end_ts,
                        str(row["process_name"]),
                        str(row["window_title"]),
                    ),
                )
            self._conn.commit()
        self._log_db_query_timing("close_interval", started_at, rows=1)

    def fetch_unsummarized_intervals(self, limit: int = 10000) -> list[ActiveInterval]:
        started_at = time.perf_counter()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, start_ts, end_ts, hwnd, pid, process_name, window_title, blocked, summarized
                FROM active_intervals
                WHERE summarized = 0 AND end_ts IS NOT NULL
                ORDER BY start_ts ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        result = [
            ActiveInterval(
                id=int(row["id"]),
                start_ts=float(row["start_ts"]),
                end_ts=float(row["end_ts"]) if row["end_ts"] is not None else None,
                hwnd=int(row["hwnd"]),
                pid=int(row["pid"]),
                process_name=str(row["process_name"]),
                window_title=str(row["window_title"]),
                blocked=bool(row["blocked"]),
                summarized=bool(row["summarized"]),
            )
            for row in rows
        ]
        self._log_db_query_timing("fetch_unsummarized_intervals", started_at, rows=len(result))
        return result

    def fetch_unsummarized_blocked_intervals(self, limit: int = 10000) -> list[BlockedInterval]:
        started_at = time.perf_counter()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, active_interval_id, start_ts, end_ts, process_name, window_title, summarized
                FROM blocked_intervals
                WHERE summarized = 0 AND end_ts IS NOT NULL
                ORDER BY start_ts ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        result = [
            BlockedInterval(
                id=int(row["id"]),
                active_interval_id=int(row["active_interval_id"]) if row["active_interval_id"] is not None else None,
                start_ts=float(row["start_ts"]),
                end_ts=float(row["end_ts"]) if row["end_ts"] is not None else None,
                process_name=str(row["process_name"]),
                window_title=str(row["window_title"]),
                summarized=bool(row["summarized"]),
            )
            for row in rows
        ]
        self._log_db_query_timing("fetch_unsummarized_blocked_intervals", started_at, rows=len(result))
        return result

    def mark_intervals_summarized(self, start_ts: float, end_ts: float) -> None:
        started_at = time.perf_counter()
        with self._lock:
            self._conn.execute(
                """
                UPDATE active_intervals
                SET summarized = 1
                WHERE start_ts >= ? AND COALESCE(end_ts, start_ts) <= ?
                """,
                (start_ts, end_ts),
            )
            self._conn.execute(
                """
                UPDATE blocked_intervals
                SET summarized = 1
                WHERE start_ts >= ? AND COALESCE(end_ts, start_ts) <= ?
                """,
                (start_ts, end_ts),
            )
            self._conn.commit()
        self._log_db_query_timing("mark_intervals_summarized", started_at)
