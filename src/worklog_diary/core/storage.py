from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import date as Day, datetime, time as DateTimeTime, timedelta
from pathlib import Path

from .models import (
    ActiveInterval,
    BlockedInterval,
    DailySummaryRecord,
    ForegroundInfo,
    KeyEvent,
    ScreenshotRecord,
    SummaryRecord,
    TextSegment,
)


class SQLiteStorage:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._db_write_window_started = time.perf_counter()
        self._db_write_window_count = 0
        self.logger = logging.getLogger(__name__)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self.bootstrap()
        self._recover_incomplete_state()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _log_db_query_timing(self, operation: str, started_at: float, *, rows: int | None = None) -> None:
        duration_ms = (time.perf_counter() - started_at) * 1000.0
        if rows is None:
            self.logger.info("event=db_query_timing operation=%s duration_ms=%.3f", operation, duration_ms)
        else:
            self.logger.info(
                "event=db_query_timing operation=%s duration_ms=%.3f rows=%s",
                operation,
                duration_ms,
                rows,
            )

    def _record_db_write(self) -> None:
        now = time.perf_counter()
        with self._metrics_lock:
            self._db_write_window_count += 1
            elapsed = now - self._db_write_window_started
            if elapsed < 1.0:
                return
            writes = self._db_write_window_count
            self._db_write_window_started = now
            self._db_write_window_count = 0

        writes_per_second = writes / elapsed if elapsed > 0 else float(writes)
        self.logger.info(
            "event=db_write_rate writes_per_second=%.2f writes_in_window=%s window_seconds=%.3f",
            writes_per_second,
            writes,
            elapsed,
        )

    def bootstrap(self) -> None:
        schema = """
        CREATE TABLE IF NOT EXISTS active_intervals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_ts REAL NOT NULL,
            end_ts REAL,
            hwnd INTEGER NOT NULL,
            pid INTEGER NOT NULL,
            process_name TEXT NOT NULL,
            window_title TEXT NOT NULL,
            blocked INTEGER NOT NULL,
            summarized INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS blocked_intervals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            active_interval_id INTEGER UNIQUE,
            start_ts REAL NOT NULL,
            end_ts REAL,
            process_name TEXT NOT NULL,
            window_title TEXT NOT NULL,
            summarized INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(active_interval_id) REFERENCES active_intervals(id)
        );

        CREATE TABLE IF NOT EXISTS key_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            key TEXT NOT NULL,
            event_type TEXT NOT NULL,
            modifiers TEXT NOT NULL,
            process_name TEXT NOT NULL,
            window_title TEXT NOT NULL,
            hwnd INTEGER NOT NULL,
            active_interval_id INTEGER,
            processed INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(active_interval_id) REFERENCES active_intervals(id)
        );

        CREATE TABLE IF NOT EXISTS text_segments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_ts REAL NOT NULL,
            end_ts REAL NOT NULL,
            process_name TEXT NOT NULL,
            window_title TEXT NOT NULL,
            text TEXT NOT NULL,
            hotkeys TEXT NOT NULL,
            raw_key_count INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS screenshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            file_path TEXT NOT NULL,
            process_name TEXT NOT NULL,
            window_title TEXT NOT NULL,
            active_interval_id INTEGER,
            FOREIGN KEY(active_interval_id) REFERENCES active_intervals(id)
        );

        CREATE TABLE IF NOT EXISTS summary_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_ts REAL NOT NULL,
            end_ts REAL NOT NULL,
            status TEXT NOT NULL,
            error TEXT,
            created_ts REAL NOT NULL,
            updated_ts REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            start_ts REAL NOT NULL,
            end_ts REAL NOT NULL,
            summary_text TEXT NOT NULL,
            summary_json TEXT NOT NULL,
            created_ts REAL NOT NULL,
            FOREIGN KEY(job_id) REFERENCES summary_jobs(id)
        );

        CREATE TABLE IF NOT EXISTS daily_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day TEXT NOT NULL UNIQUE,
            created_ts REAL NOT NULL,
            recap_text TEXT NOT NULL,
            recap_json TEXT,
            source_batch_count INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_active_intervals_time ON active_intervals(start_ts, end_ts);
        CREATE INDEX IF NOT EXISTS idx_active_intervals_summarized ON active_intervals(summarized, end_ts);
        CREATE INDEX IF NOT EXISTS idx_blocked_intervals_time ON blocked_intervals(start_ts, end_ts);
        CREATE INDEX IF NOT EXISTS idx_blocked_intervals_summarized ON blocked_intervals(summarized, end_ts);
        CREATE INDEX IF NOT EXISTS idx_key_events_ts_processed ON key_events(processed, ts);
        CREATE INDEX IF NOT EXISTS idx_text_segments_time ON text_segments(start_ts, end_ts);
        CREATE INDEX IF NOT EXISTS idx_screenshots_ts ON screenshots(ts);
        CREATE INDEX IF NOT EXISTS idx_summaries_created ON summaries(created_ts DESC);
        CREATE INDEX IF NOT EXISTS idx_summaries_start ON summaries(start_ts);
        CREATE INDEX IF NOT EXISTS idx_daily_summaries_day ON daily_summaries(day);
        """
        with self._lock:
            self._conn.executescript(schema)
            self._ensure_daily_summaries_schema()
            self._conn.commit()

    def _ensure_daily_summaries_schema(self) -> None:
        row = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'daily_summaries'"
        ).fetchone()
        if row is None:
            return

        columns = {
            str(item["name"])
            for item in self._conn.execute("PRAGMA table_info(daily_summaries)").fetchall()
        }
        if "source_batch_count" not in columns:
            self._conn.execute(
                "ALTER TABLE daily_summaries ADD COLUMN source_batch_count INTEGER NOT NULL DEFAULT 0"
            )

    def _recover_incomplete_state(self) -> None:
        now = time.time()
        started_at = time.perf_counter()
        with self._lock:
            open_rows = self._conn.execute(
                """
                SELECT id, start_ts, process_name, window_title, blocked
                FROM active_intervals
                WHERE end_ts IS NULL
                """
            ).fetchall()

            if open_rows:
                self._conn.execute("UPDATE active_intervals SET end_ts = ? WHERE end_ts IS NULL", (now,))

            blocked_rows = [row for row in open_rows if int(row["blocked"]) == 1]
            if blocked_rows:
                self._conn.executemany(
                    """
                    INSERT OR IGNORE INTO blocked_intervals(
                        active_interval_id, start_ts, end_ts, process_name, window_title, summarized
                    ) VALUES (?, ?, ?, ?, ?, 0)
                    """,
                    [
                        (
                            int(row["id"]),
                            float(row["start_ts"]),
                            now,
                            str(row["process_name"]),
                            str(row["window_title"]),
                        )
                        for row in blocked_rows
                    ],
                )

            interrupted_jobs = self._conn.execute(
                "SELECT COUNT(1) AS c FROM summary_jobs WHERE status IN ('running', 'queued')"
            ).fetchone()
            interrupted_count = int(interrupted_jobs["c"]) if interrupted_jobs else 0
            if interrupted_count:
                self._conn.execute(
                    """
                    UPDATE summary_jobs
                    SET status = 'failed',
                        error = COALESCE(error, 'Interrupted during application shutdown'),
                        updated_ts = ?
                    WHERE status IN ('running', 'queued')
                    """,
                    (now,),
                )

            self._conn.commit()

        if open_rows or interrupted_count:
            self.logger.warning(
                "event=startup_recovery closed_open_intervals=%s interrupted_jobs=%s",
                len(open_rows),
                interrupted_count,
            )
        self._log_db_query_timing("startup_recovery", started_at, rows=len(open_rows) + interrupted_count)

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
        self._record_db_write()
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
        self._record_db_write()
        self._log_db_query_timing("close_interval", started_at, rows=1)

    def insert_key_event(self, event: KeyEvent) -> int:
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO key_events(
                    ts, key, event_type, modifiers, process_name, window_title, hwnd, active_interval_id, processed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.ts,
                    event.key,
                    event.event_type,
                    json.dumps(event.modifiers),
                    event.process_name,
                    event.window_title,
                    event.hwnd,
                    event.active_interval_id,
                    int(event.processed),
                ),
            )
            self._conn.commit()
            key_event_id = int(cursor.lastrowid)
        self._record_db_write()
        return key_event_id

    def fetch_unprocessed_key_events(self, limit: int = 5000) -> list[KeyEvent]:
        started_at = time.perf_counter()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, ts, key, event_type, modifiers, process_name, window_title, hwnd, active_interval_id, processed
                FROM key_events
                WHERE processed = 0
                ORDER BY ts ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        result = [
            KeyEvent(
                id=int(row["id"]),
                ts=float(row["ts"]),
                key=str(row["key"]),
                event_type=str(row["event_type"]),
                modifiers=json.loads(str(row["modifiers"])),
                process_name=str(row["process_name"]),
                window_title=str(row["window_title"]),
                hwnd=int(row["hwnd"]),
                active_interval_id=int(row["active_interval_id"]) if row["active_interval_id"] is not None else None,
                processed=bool(row["processed"]),
            )
            for row in rows
        ]
        self._log_db_query_timing("fetch_unprocessed_key_events", started_at, rows=len(result))
        return result

    def mark_key_events_processed(self, ids: list[int]) -> None:
        if not ids:
            return
        started_at = time.perf_counter()
        placeholders = ",".join("?" for _ in ids)
        with self._lock:
            self._conn.execute(f"UPDATE key_events SET processed = 1 WHERE id IN ({placeholders})", ids)
            self._conn.commit()
        self._record_db_write()
        self._log_db_query_timing("mark_key_events_processed", started_at, rows=len(ids))

    def insert_text_segments(self, segments: list[TextSegment]) -> None:
        if not segments:
            return
        started_at = time.perf_counter()
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO text_segments(
                    start_ts, end_ts, process_name, window_title, text, hotkeys, raw_key_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        segment.start_ts,
                        segment.end_ts,
                        segment.process_name,
                        segment.window_title,
                        segment.text,
                        json.dumps(segment.hotkeys),
                        segment.raw_key_count,
                    )
                    for segment in segments
                ],
            )
            self._conn.commit()
        for _ in segments:
            self._record_db_write()
        self._log_db_query_timing("insert_text_segments", started_at, rows=len(segments))

    def fetch_unsummarized_text_segments(self, limit: int = 200) -> list[TextSegment]:
        started_at = time.perf_counter()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, start_ts, end_ts, process_name, window_title, text, hotkeys, raw_key_count
                FROM text_segments
                ORDER BY start_ts ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        result = [
            TextSegment(
                id=int(row["id"]),
                start_ts=float(row["start_ts"]),
                end_ts=float(row["end_ts"]),
                process_name=str(row["process_name"]),
                window_title=str(row["window_title"]),
                text=str(row["text"]),
                hotkeys=json.loads(str(row["hotkeys"])),
                raw_key_count=int(row["raw_key_count"]),
            )
            for row in rows
        ]
        self._log_db_query_timing("fetch_unsummarized_text_segments", started_at, rows=len(result))
        return result

    def insert_screenshot(self, screenshot: ScreenshotRecord) -> int:
        started_at = time.perf_counter()
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO screenshots(ts, file_path, process_name, window_title, active_interval_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    screenshot.ts,
                    screenshot.file_path,
                    screenshot.process_name,
                    screenshot.window_title,
                    screenshot.active_interval_id,
                ),
            )
            self._conn.commit()
            screenshot_id = int(cursor.lastrowid)
        self._record_db_write()
        self._log_db_query_timing("insert_screenshot", started_at, rows=1)
        return screenshot_id

    def fetch_unsummarized_screenshots(self, limit: int = 20) -> list[ScreenshotRecord]:
        started_at = time.perf_counter()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, ts, file_path, process_name, window_title, active_interval_id
                FROM screenshots
                ORDER BY ts ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        result = [
            ScreenshotRecord(
                id=int(row["id"]),
                ts=float(row["ts"]),
                file_path=str(row["file_path"]),
                process_name=str(row["process_name"]),
                window_title=str(row["window_title"]),
                active_interval_id=int(row["active_interval_id"]) if row["active_interval_id"] is not None else None,
            )
            for row in rows
        ]
        self._log_db_query_timing("fetch_unsummarized_screenshots", started_at, rows=len(result))
        return result

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
        self._record_db_write()
        self._log_db_query_timing("mark_intervals_summarized", started_at)

    def create_summary_job(self, start_ts: float, end_ts: float, status: str = "running") -> int:
        now = time.time()
        started_at = time.perf_counter()
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO summary_jobs(start_ts, end_ts, status, error, created_ts, updated_ts)
                VALUES (?, ?, ?, NULL, ?, ?)
                """,
                (start_ts, end_ts, status, now, now),
            )
            self._conn.commit()
            job_id = int(cursor.lastrowid)
        self._record_db_write()
        self._log_db_query_timing("create_summary_job", started_at, rows=1)
        return job_id

    def update_summary_job(self, job_id: int, status: str, error: str | None = None) -> None:
        now = time.time()
        started_at = time.perf_counter()
        with self._lock:
            self._conn.execute(
                "UPDATE summary_jobs SET status = ?, error = ?, updated_ts = ? WHERE id = ?",
                (status, error, now, job_id),
            )
            self._conn.commit()
        self._record_db_write()
        self._log_db_query_timing("update_summary_job", started_at, rows=1)

    def get_summary_job_status_counts(self) -> dict[str, int]:
        started_at = time.perf_counter()
        counts = {
            "queued": 0,
            "running": 0,
            "succeeded": 0,
            "failed": 0,
            "cancelled": 0,
        }
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(1) AS c FROM summary_jobs GROUP BY status"
            ).fetchall()
        for row in rows:
            status = str(row["status"])
            if status in counts:
                counts[status] = int(row["c"])
        self._log_db_query_timing("get_summary_job_status_counts", started_at, rows=len(rows))
        return counts

    def insert_summary(
        self,
        job_id: int,
        start_ts: float,
        end_ts: float,
        summary_text: str,
        summary_json: dict,
    ) -> int:
        now = time.time()
        started_at = time.perf_counter()
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO summaries(job_id, start_ts, end_ts, summary_text, summary_json, created_ts)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (job_id, start_ts, end_ts, summary_text, json.dumps(summary_json), now),
            )
            self._conn.commit()
            summary_id = int(cursor.lastrowid)
        self._record_db_write()
        self._log_db_query_timing("insert_summary", started_at, rows=1)
        return summary_id

    def list_summaries(self, limit: int = 100) -> list[SummaryRecord]:
        started_at = time.perf_counter()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, job_id, start_ts, end_ts, summary_text, summary_json, created_ts
                FROM summaries
                ORDER BY created_ts DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        result = [
            SummaryRecord(
                id=int(row["id"]),
                job_id=int(row["job_id"]),
                start_ts=float(row["start_ts"]),
                end_ts=float(row["end_ts"]),
                summary_text=str(row["summary_text"]),
                summary_json=json.loads(str(row["summary_json"])),
                created_ts=float(row["created_ts"]),
            )
            for row in rows
        ]
        self._log_db_query_timing("list_summaries", started_at, rows=len(result))
        return result

    def list_summary_days(self, limit: int = 366) -> list[Day]:
        started_at = time.perf_counter()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT DISTINCT date(start_ts, 'unixepoch', 'localtime') AS day
                FROM summaries
                ORDER BY day DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        result = [
            datetime.strptime(str(row["day"]), "%Y-%m-%d").date()
            for row in rows
            if row["day"] is not None
        ]
        self._log_db_query_timing("list_summary_days", started_at, rows=len(result))
        return result

    def list_summaries_for_day(self, day: Day, limit: int = 500) -> list[SummaryRecord]:
        start_ts, end_ts = _day_epoch_bounds(day)
        started_at = time.perf_counter()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, job_id, start_ts, end_ts, summary_text, summary_json, created_ts
                FROM summaries
                WHERE start_ts >= ? AND start_ts < ?
                ORDER BY start_ts ASC, id ASC
                LIMIT ?
                """,
                (start_ts, end_ts, limit),
            ).fetchall()
        result = [
            SummaryRecord(
                id=int(row["id"]),
                job_id=int(row["job_id"]),
                start_ts=float(row["start_ts"]),
                end_ts=float(row["end_ts"]),
                summary_text=str(row["summary_text"]),
                summary_json=json.loads(str(row["summary_json"])),
                created_ts=float(row["created_ts"]),
            )
            for row in rows
        ]
        self._log_db_query_timing("list_summaries_for_day", started_at, rows=len(result))
        return result

    def count_batch_summaries_for_day(self, day: Day) -> int:
        start_ts, end_ts = _day_epoch_bounds(day)
        started_at = time.perf_counter()
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(1) AS c FROM summaries WHERE start_ts >= ? AND start_ts < ?",
                (start_ts, end_ts),
            ).fetchone()
        count = int(row["c"])
        self._log_db_query_timing("count_batch_summaries_for_day", started_at, rows=1)
        return count

    def create_daily_summary(
        self,
        day: Day,
        recap_text: str,
        recap_json: dict | None,
        source_batch_count: int,
    ) -> tuple[DailySummaryRecord, bool]:
        day_key = day.isoformat()
        now = time.time()
        started_at = time.perf_counter()
        recap_json_str = json.dumps(recap_json) if recap_json is not None else None
        with self._lock:
            existing = self._conn.execute("SELECT id FROM daily_summaries WHERE day = ?", (day_key,)).fetchone()
            self._conn.execute(
                """
                INSERT INTO daily_summaries(day, created_ts, recap_text, recap_json, source_batch_count)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(day) DO UPDATE SET
                    created_ts = excluded.created_ts,
                    recap_text = excluded.recap_text,
                    recap_json = excluded.recap_json,
                    source_batch_count = excluded.source_batch_count
                """,
                (day_key, now, recap_text, recap_json_str, int(source_batch_count)),
            )
            row = self._conn.execute(
                """
                SELECT id, day, created_ts, recap_text, recap_json, source_batch_count
                FROM daily_summaries
                WHERE day = ?
                """,
                (day_key,),
            ).fetchone()
            self._conn.commit()

        if row is None:
            raise RuntimeError(f"Failed to create daily summary for {day_key}")
        record = _row_to_daily_summary_record(row)
        self._record_db_write()
        self._log_db_query_timing("create_daily_summary", started_at, rows=1)
        return record, existing is not None

    def get_daily_summary_for_day(self, day: Day) -> DailySummaryRecord | None:
        day_key = day.isoformat()
        started_at = time.perf_counter()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT id, day, created_ts, recap_text, recap_json, source_batch_count
                FROM daily_summaries
                WHERE day = ?
                """,
                (day_key,),
            ).fetchone()
        if row is None:
            self._log_db_query_timing("get_daily_summary_for_day", started_at, rows=0)
            return None
        record = _row_to_daily_summary_record(row)
        self._log_db_query_timing("get_daily_summary_for_day", started_at, rows=1)
        return record

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
        self.logger.info(
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
            self.logger.warning("event=purge_file_delete_failed paths=%s", failed_paths)
        self._log_db_query_timing(
            "purge_raw_data",
            started_at,
            rows=deleted_key_events + deleted_segments + deleted_screenshots,
        )
        return screenshot_paths

    def get_pending_counts(self) -> dict[str, int]:
        started_at = time.perf_counter()
        with self._lock:
            intervals = int(
                self._conn.execute(
                    "SELECT COUNT(1) AS c FROM active_intervals WHERE summarized = 0 AND end_ts IS NOT NULL"
                ).fetchone()["c"]
            )
            keys = int(self._conn.execute("SELECT COUNT(1) AS c FROM key_events WHERE processed = 0").fetchone()["c"])
            processed_keys = int(
                self._conn.execute("SELECT COUNT(1) AS c FROM key_events WHERE processed = 1").fetchone()["c"]
            )
            segments = int(self._conn.execute("SELECT COUNT(1) AS c FROM text_segments").fetchone()["c"])
            screenshots = int(self._conn.execute("SELECT COUNT(1) AS c FROM screenshots").fetchone()["c"])

        result = {
            "intervals": intervals,
            "key_events": keys,
            "processed_key_events": processed_keys,
            "text_segments": segments,
            "screenshots": screenshots,
        }
        self._log_db_query_timing("get_pending_counts", started_at, rows=5)
        return result

    def count_unprocessed_key_events(self) -> int:
        started_at = time.perf_counter()
        with self._lock:
            row = self._conn.execute("SELECT COUNT(1) AS c FROM key_events WHERE processed = 0").fetchone()
        count = int(row["c"])
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
                "failed": int(
                    self._conn.execute("SELECT COUNT(1) AS c FROM summary_jobs WHERE status = 'failed'").fetchone()["c"]
                ),
                "succeeded": int(
                    self._conn.execute("SELECT COUNT(1) AS c FROM summary_jobs WHERE status = 'succeeded'").fetchone()["c"]
                ),
                "cancelled": int(
                    self._conn.execute("SELECT COUNT(1) AS c FROM summary_jobs WHERE status = 'cancelled'").fetchone()["c"]
                ),
            }

        result = {
            "table_counts": table_counts,
            "pending_counts": self.get_pending_counts(),
            "pending_ranges": pending_ranges,
            "summary_jobs": summary_jobs,
        }
        self._log_db_query_timing("get_diagnostics_snapshot", started_at)
        return result

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


def _row_to_daily_summary_record(row: sqlite3.Row) -> DailySummaryRecord:
    raw_json = row["recap_json"]
    parsed_json = json.loads(str(raw_json)) if raw_json else None
    return DailySummaryRecord(
        id=int(row["id"]),
        day=datetime.strptime(str(row["day"]), "%Y-%m-%d").date(),
        recap_text=str(row["recap_text"]),
        recap_json=parsed_json if isinstance(parsed_json, dict) else None,
        source_batch_count=int(row["source_batch_count"]),
        created_ts=float(row["created_ts"]),
    )


def _day_epoch_bounds(day: Day) -> tuple[float, float]:
    start_dt = datetime.combine(day, DateTimeTime.min)
    end_dt = start_dt + timedelta(days=1)
    return start_dt.timestamp(), end_dt.timestamp()
