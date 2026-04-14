from __future__ import annotations

import logging
import sqlite3
import threading
import time


class StorageSchemaManager:
    def __init__(self, conn: sqlite3.Connection, lock: threading.Lock, logger: logging.Logger) -> None:
        self._conn = conn
        self._lock = lock
        self._logger = logger

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
            window_hwnd INTEGER,
            fingerprint TEXT,
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
            journal_mode = self._conn.execute("PRAGMA journal_mode=WAL").fetchone()
            self._conn.executescript(schema)
            self.ensure_daily_summaries_schema()
            self.ensure_screenshots_schema()
            self._conn.commit()

        if journal_mode is not None:
            self._logger.info("event=storage_journal_mode mode=%s", str(journal_mode[0]))

    def recover_incomplete_state(self) -> None:
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
            self._logger.warning(
                "event=startup_recovery closed_open_intervals=%s interrupted_jobs=%s",
                len(open_rows),
                interrupted_count,
            )
        self._log_db_query_timing("startup_recovery", started_at, rows=len(open_rows) + interrupted_count)

    def ensure_daily_summaries_schema(self) -> None:
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

    def ensure_screenshots_schema(self) -> None:
        row = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'screenshots'"
        ).fetchone()
        if row is None:
            return

        columns = {
            str(item["name"])
            for item in self._conn.execute("PRAGMA table_info(screenshots)").fetchall()
        }
        if "window_hwnd" not in columns:
            self._conn.execute("ALTER TABLE screenshots ADD COLUMN window_hwnd INTEGER")
        if "fingerprint" not in columns:
            self._conn.execute("ALTER TABLE screenshots ADD COLUMN fingerprint TEXT")

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
