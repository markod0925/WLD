from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from datetime import date as Day, datetime, time as DateTimeTime, timedelta
from pathlib import Path

from .activity_repository import ActivityRepository
from .security.db_key_manager import (
    DatabaseKeyCorruptedError,
    DatabaseKeyMissingError,
    DatabaseKeyProtectionError,
    DatabaseKeyUnprotectError,
    ensure_database_key,
)
from .security.sqlcipher import (
    SqlCipherKeyMismatchError,
    SqlCipherOpenError,
    SqlCipherUnavailableError,
    open_sqlcipher_connection,
)
from .storage_cleanup import StorageCleanupService
from .storage_diagnostics import StorageDiagnosticsRepository
from .storage_schema import StorageSchemaManager
from .models import (
    ActiveInterval,
    BlockedInterval,
    DailySummaryRecord,
    ForegroundInfo,
    KeyEvent,
    CoalescingDiagnosticRecord,
    ScreenshotRecord,
    SummaryRecord,
    TextSegment,
)


_SUMMARY_JOB_TERMINAL_STATUSES = {
    "completed",
    "succeeded",
    "failed",
    "timed_out",
    "cancelled",
    "abandoned",
}

_SUMMARY_JOB_ACTIVE_STATUSES = {"queued", "running"}


class SQLiteStorage(ActivityRepository):
    """SQLite-backed repository for activity capture and summary records."""

    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path))
        self.db_key_path = str(Path(self.db_path).with_name("db_key.bin"))
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._db_write_window_started = time.perf_counter()
        self._db_write_window_count = 0
        self.logger = logging.getLogger(__name__)
        self._conn = None
        db_exists = Path(self.db_path).exists()
        key_exists = Path(self.db_key_path).exists()
        try:
            if not key_exists and not db_exists:
                self.logger.info(
                    "event=db_key_bootstrap status=creating db_path=%s key_path=%s",
                    self.db_path,
                    self.db_key_path,
                )
            elif not key_exists:
                self.logger.error(
                    "event=db_key_missing status=error db_path=%s key_path=%s",
                    self.db_path,
                    self.db_key_path,
                )
            key_bytes = ensure_database_key(self.db_path, self.db_key_path)
            if not key_exists and not db_exists:
                self.logger.info(
                    "event=db_key_generated status=ok db_path=%s key_path=%s",
                    self.db_path,
                    self.db_key_path,
                )
            self._conn = open_sqlcipher_connection(self.db_path, key_bytes)
            self.schema_manager = StorageSchemaManager(self._conn, self._lock, self.logger)
            self.diagnostics_repository = StorageDiagnosticsRepository(self._conn, self._lock, self.logger)
            self.cleanup_service = StorageCleanupService(self._conn, self._lock, self.logger)
            self.bootstrap()
            self._recover_incomplete_state()
            self.logger.info(
                "event=db_open status=ok db_path=%s key_path=%s encrypted=true",
                self.db_path,
                self.db_key_path,
            )
        except (
            DatabaseKeyMissingError,
            DatabaseKeyCorruptedError,
            DatabaseKeyProtectionError,
            DatabaseKeyUnprotectError,
            SqlCipherUnavailableError,
            SqlCipherKeyMismatchError,
            SqlCipherOpenError,
        ) as exc:
            self.logger.error(
                "event=db_open status=error db_path=%s key_path=%s error_type=%s error=%s",
                self.db_path,
                self.db_key_path,
                exc.__class__.__name__,
                exc,
            )
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
            raise
        except Exception:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
            raise

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
        self.schema_manager.bootstrap()

    def _ensure_daily_summaries_schema(self) -> None:
        self.schema_manager.ensure_daily_summaries_schema()

    def _recover_incomplete_state(self) -> None:
        self.schema_manager.recover_incomplete_state()

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
        started_at = time.perf_counter()
        values = self._key_event_values(event)
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO key_events(
                    ts, key, event_type, modifiers, process_name, window_title, hwnd, active_interval_id, processed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
            self._conn.commit()
            key_event_id = int(cursor.lastrowid)
        self._record_db_write()
        self._log_db_query_timing("insert_key_event", started_at, rows=1)
        return key_event_id

    def insert_key_events(self, events: list[KeyEvent]) -> int:
        if not events:
            return 0

        started_at = time.perf_counter()
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO key_events(
                    ts, key, event_type, modifiers, process_name, window_title, hwnd, active_interval_id, processed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [self._key_event_values(event) for event in events],
            )
            self._conn.commit()
        self._record_db_write()
        self._log_db_query_timing("insert_key_events", started_at, rows=len(events))
        return len(events)

    def _key_event_values(self, event: KeyEvent) -> tuple[object, ...]:
        return (
            event.ts,
            event.key,
            event.event_type,
            json.dumps(event.modifiers),
            event.process_name,
            event.window_title,
            event.hwnd,
            event.active_interval_id,
            int(event.processed),
        )

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
        perceptual_hash = screenshot.perceptual_hash or screenshot.fingerprint
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO screenshots(
                    ts, file_path, process_name, window_title, active_interval_id, window_hwnd,
                    fingerprint, exact_hash, perceptual_hash, image_width, image_height,
                    nearest_phash_distance, nearest_ssim, dedup_reason, visual_context_streak
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    screenshot.ts,
                    screenshot.file_path,
                    screenshot.process_name,
                    screenshot.window_title,
                    screenshot.active_interval_id,
                    screenshot.window_hwnd,
                    perceptual_hash,
                    screenshot.exact_hash,
                    perceptual_hash,
                    screenshot.image_width,
                    screenshot.image_height,
                    screenshot.nearest_phash_distance,
                    screenshot.nearest_ssim,
                    screenshot.dedup_reason,
                    int(screenshot.visual_context_streak),
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
                SELECT
                    id, ts, file_path, process_name, window_title, active_interval_id, window_hwnd,
                    fingerprint, exact_hash, perceptual_hash, image_width, image_height,
                    nearest_phash_distance, nearest_ssim, dedup_reason, visual_context_streak
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
                window_hwnd=int(row["window_hwnd"]) if row["window_hwnd"] is not None else None,
                fingerprint=str(row["fingerprint"]) if row["fingerprint"] is not None else None,
                exact_hash=str(row["exact_hash"]) if row["exact_hash"] is not None else None,
                perceptual_hash=str(row["perceptual_hash"]) if row["perceptual_hash"] is not None else None,
                image_width=int(row["image_width"]) if row["image_width"] is not None else None,
                image_height=int(row["image_height"]) if row["image_height"] is not None else None,
                nearest_phash_distance=int(row["nearest_phash_distance"])
                if row["nearest_phash_distance"] is not None
                else None,
                nearest_ssim=float(row["nearest_ssim"]) if row["nearest_ssim"] is not None else None,
                dedup_reason=str(row["dedup_reason"]) if row["dedup_reason"] is not None else None,
                visual_context_streak=int(row["visual_context_streak"]) if row["visual_context_streak"] is not None else 0,
            )
            for row in rows
        ]
        self._log_db_query_timing("fetch_unsummarized_screenshots", started_at, rows=len(result))
        return result

    def fetch_recent_screenshots(self, limit: int = 20) -> list[ScreenshotRecord]:
        started_at = time.perf_counter()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT
                    id, ts, file_path, process_name, window_title, active_interval_id, window_hwnd,
                    fingerprint, exact_hash, perceptual_hash, image_width, image_height,
                    nearest_phash_distance, nearest_ssim, dedup_reason, visual_context_streak
                FROM screenshots
                ORDER BY ts DESC
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
                window_hwnd=int(row["window_hwnd"]) if row["window_hwnd"] is not None else None,
                fingerprint=str(row["fingerprint"]) if row["fingerprint"] is not None else None,
                exact_hash=str(row["exact_hash"]) if row["exact_hash"] is not None else None,
                perceptual_hash=str(row["perceptual_hash"]) if row["perceptual_hash"] is not None else None,
                image_width=int(row["image_width"]) if row["image_width"] is not None else None,
                image_height=int(row["image_height"]) if row["image_height"] is not None else None,
                nearest_phash_distance=int(row["nearest_phash_distance"])
                if row["nearest_phash_distance"] is not None
                else None,
                nearest_ssim=float(row["nearest_ssim"]) if row["nearest_ssim"] is not None else None,
                dedup_reason=str(row["dedup_reason"]) if row["dedup_reason"] is not None else None,
                visual_context_streak=int(row["visual_context_streak"]) if row["visual_context_streak"] is not None else 0,
            )
            for row in rows
        ]
        self._log_db_query_timing("fetch_recent_screenshots", started_at, rows=len(result))
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

    def create_summary_job(
        self,
        start_ts: float,
        end_ts: float,
        status: str = "queued",
        *,
        job_type: str = "event_summary",
        target_day: Day | str | None = None,
        timeout_s: float = 0,
        attempt: int = 1,
        input_chars: int = 0,
        input_token_estimate: int | None = None,
        priority: int = 100,
    ) -> int:
        now = time.time()
        query_started_at = time.perf_counter()
        stored_status = _normalize_summary_job_status(status)
        day_key = _normalize_day_key(target_day)
        with self._lock:
            if day_key is not None:
                existing = self._conn.execute(
                    """
                    SELECT id
                    FROM summary_jobs
                    WHERE job_type = 'day_summary' AND target_day = ?
                    """,
                    (day_key,),
                ).fetchone()
                if existing is not None:
                    job_id = int(existing["id"])
                    self._log_db_query_timing("create_summary_job", query_started_at, rows=1)
                    return job_id

            started_at = now if stored_status in _SUMMARY_JOB_TERMINAL_STATUSES or stored_status == "running" else None
            finished_at = now if stored_status in _SUMMARY_JOB_TERMINAL_STATUSES else None
            cursor = self._conn.execute(
                """
                INSERT INTO summary_jobs(
                    start_ts, end_ts, status, error, job_type, target_day, queued_at, created_at, started_at,
                    finished_at, timeout_s, attempt, input_chars, input_token_estimate, priority, created_ts, updated_ts
                )
                VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    start_ts,
                    end_ts,
                    stored_status,
                    job_type,
                    day_key,
                    now,
                    now,
                    started_at,
                    finished_at,
                    timeout_s,
                    max(1, int(attempt)),
                    max(0, int(input_chars)),
                    input_token_estimate,
                    priority,
                    now,
                    now,
                ),
            )
            self._conn.commit()
            job_id = int(cursor.lastrowid)
        self._record_db_write()
        self._log_db_query_timing("create_summary_job", query_started_at, rows=1)
        return job_id

    def update_summary_job(
        self,
        job_id: int,
        status: str,
        error: str | None = None,
        *,
        job_type: str | None = None,
        started_at: float | None = None,
        finished_at: float | None = None,
        timeout_s: float | None = None,
        attempt: int | None = None,
        input_chars: int | None = None,
        input_token_estimate: int | None = None,
        priority: int | None = None,
    ) -> None:
        now = time.time()
        query_started_at = time.perf_counter()
        stored_status = _normalize_summary_job_status(status)
        updates = ["status = ?", "error = ?", "updated_ts = ?"]
        values: list[object] = [stored_status, error, now]
        if job_type is not None:
            updates.append("job_type = ?")
            values.append(job_type)
        if started_at is not None:
            updates.append("started_at = ?")
            values.append(started_at)
        elif stored_status == "running":
            updates.append("started_at = ?")
            values.append(now)
        if finished_at is not None:
            updates.append("finished_at = ?")
            values.append(finished_at)
        elif stored_status in _SUMMARY_JOB_TERMINAL_STATUSES:
            updates.append("finished_at = ?")
            values.append(now)
        if timeout_s is not None:
            updates.append("timeout_s = ?")
            values.append(timeout_s)
        if attempt is not None:
            updates.append("attempt = ?")
            values.append(max(1, int(attempt)))
        if input_chars is not None:
            updates.append("input_chars = ?")
            values.append(max(0, int(input_chars)))
        if input_token_estimate is not None:
            updates.append("input_token_estimate = ?")
            values.append(input_token_estimate)
        if priority is not None:
            updates.append("priority = ?")
            values.append(priority)
        with self._lock:
            self._conn.execute(
                f"UPDATE summary_jobs SET {', '.join(updates)} WHERE id = ?",
                [*values, job_id],
            )
            self._conn.commit()
        self._record_db_write()
        self._log_db_query_timing("update_summary_job", query_started_at, rows=1)

    def create_or_reuse_daily_summary_job(
        self,
        day: Day,
        start_ts: float,
        end_ts: float,
        *,
        status: str = "queued",
        timeout_s: float = 0,
        attempt: int = 1,
        input_chars: int = 0,
        input_token_estimate: int | None = None,
        priority: int = 100,
    ) -> tuple[int, bool]:
        day_key = day.isoformat()
        now = time.time()
        stored_status = _normalize_summary_job_status(status)
        query_started_at = time.perf_counter()
        with self._lock:
            existing = self._conn.execute(
                """
                SELECT id, status, attempt
                FROM summary_jobs
                WHERE job_type = 'day_summary' AND target_day = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (day_key,),
            ).fetchone()
            if existing is None:
                cursor = self._conn.execute(
                    """
                    INSERT INTO summary_jobs(
                        start_ts, end_ts, status, error, job_type, target_day, queued_at, created_at, started_at,
                        finished_at, timeout_s, attempt, input_chars, input_token_estimate, priority, created_ts, updated_ts
                    )
                    VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        start_ts,
                        end_ts,
                        stored_status,
                        "day_summary",
                        day_key,
                        now,
                        now,
                        now if stored_status == "running" or stored_status in _SUMMARY_JOB_TERMINAL_STATUSES else None,
                        now if stored_status in _SUMMARY_JOB_TERMINAL_STATUSES else None,
                        timeout_s,
                        max(1, int(attempt)),
                        max(0, int(input_chars)),
                        input_token_estimate,
                        priority,
                        now,
                        now,
                    ),
                )
                self._conn.commit()
                job_id = int(cursor.lastrowid)
                self._record_db_write()
                self._log_db_query_timing("create_or_reuse_daily_summary_job", query_started_at, rows=1)
                return job_id, False

            job_id = int(existing["id"])
            existing_status = _normalize_summary_job_status(str(existing["status"]))
            if existing_status in _SUMMARY_JOB_ACTIVE_STATUSES:
                self._log_db_query_timing("create_or_reuse_daily_summary_job", query_started_at, rows=1)
                return job_id, True

            next_attempt = max(1, int(existing["attempt"]) + 1, int(attempt))
            self._conn.execute(
                """
                UPDATE summary_jobs
                SET status = 'queued',
                    error = NULL,
                    start_ts = ?,
                    end_ts = ?,
                    queued_at = ?,
                    started_at = NULL,
                    finished_at = NULL,
                    timeout_s = ?,
                    attempt = ?,
                    input_chars = ?,
                    input_token_estimate = ?,
                    priority = ?,
                    updated_ts = ?
                WHERE id = ?
                """,
                (
                    start_ts,
                    end_ts,
                    now,
                    timeout_s,
                    next_attempt,
                    max(0, int(input_chars)),
                    input_token_estimate,
                    priority,
                    now,
                    job_id,
                ),
            )
            self._conn.commit()
        self._record_db_write()
        self._log_db_query_timing("create_or_reuse_daily_summary_job", query_started_at, rows=1)
        return job_id, True

    def get_summary_job(self, job_id: int) -> dict[str, object] | None:
        started_at = time.perf_counter()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT id, start_ts, end_ts, status, error, job_type, target_day, queued_at, created_at, started_at,
                       finished_at, timeout_s, attempt, input_chars, input_token_estimate, priority, created_ts, updated_ts
                FROM summary_jobs
                WHERE id = ?
                """,
                (job_id,),
            ).fetchone()
        if row is None:
            self._log_db_query_timing("get_summary_job", started_at, rows=0)
            return None
        result = _row_to_summary_job_dict(row)
        self._log_db_query_timing("get_summary_job", started_at, rows=1)
        return result

    def get_daily_summary_job_for_day(self, day: Day) -> dict[str, object] | None:
        day_key = day.isoformat()
        started_at = time.perf_counter()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT id, start_ts, end_ts, status, error, job_type, target_day, queued_at, created_at, started_at,
                       finished_at, timeout_s, attempt, input_chars, input_token_estimate, priority, created_ts, updated_ts
                FROM summary_jobs
                WHERE job_type = 'day_summary' AND target_day = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (day_key,),
            ).fetchone()
        if row is None:
            self._log_db_query_timing("get_daily_summary_job_for_day", started_at, rows=0)
            return None
        result = _row_to_summary_job_dict(row)
        self._log_db_query_timing("get_daily_summary_job_for_day", started_at, rows=1)
        return result

    def get_summary_job_status_counts(self) -> dict[str, int]:
        started_at = time.perf_counter()
        counts = {
            "queued": 0,
            "running": 0,
            "completed": 0,
            "succeeded": 0,
            "failed": 0,
            "timed_out": 0,
            "cancelled": 0,
            "abandoned": 0,
        }
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(1) AS c FROM summary_jobs GROUP BY status"
            ).fetchall()
        for row in rows:
            status = str(row["status"])
            count = int(row["c"])
            if status == "succeeded":
                counts["succeeded"] += count
                counts["completed"] += count
            elif status == "completed":
                counts["completed"] += count
                counts["succeeded"] += count
            elif status in counts:
                counts[status] += count
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
        query_started_at = time.perf_counter()
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
        self._log_db_query_timing("insert_summary", query_started_at, rows=1)
        return summary_id

    def update_summary_record(
        self,
        summary_id: int,
        *,
        start_ts: float | None = None,
        end_ts: float | None = None,
        summary_text: str | None = None,
        summary_json: dict | None = None,
    ) -> None:
        started_at = time.perf_counter()
        updates: list[str] = []
        values: list[object] = []
        if start_ts is not None:
            updates.append("start_ts = ?")
            values.append(start_ts)
        if end_ts is not None:
            updates.append("end_ts = ?")
            values.append(end_ts)
        if summary_text is not None:
            updates.append("summary_text = ?")
            values.append(summary_text)
        if summary_json is not None:
            updates.append("summary_json = ?")
            values.append(json.dumps(summary_json))
        if not updates:
            return

        values.append(summary_id)
        with self._lock:
            self._conn.execute(f"UPDATE summaries SET {', '.join(updates)} WHERE id = ?", values)
            self._conn.commit()
        self._record_db_write()
        self._log_db_query_timing("update_summary_record", started_at, rows=1)

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

    def search_event_summaries(
        self,
        *,
        query: str,
        start_ts: float | None = None,
        end_ts: float | None = None,
        limit: int = 1000,
    ) -> list[SummaryRecord]:
        started_at = time.perf_counter()
        lowered_query = f"%{_escape_like_pattern(query.strip().lower())}%"
        clauses = ["LOWER(summary_text) LIKE ? ESCAPE '\\'"]
        values: list[object] = [lowered_query]
        if start_ts is not None:
            clauses.append("start_ts >= ?")
            values.append(start_ts)
        if end_ts is not None:
            clauses.append("start_ts < ?")
            values.append(end_ts)
        values.append(limit)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT id, job_id, start_ts, end_ts, summary_text, summary_json, created_ts
                FROM summaries
                WHERE {' AND '.join(clauses)}
                ORDER BY start_ts DESC, id DESC
                LIMIT ?
                """,
                values,
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
        self._log_db_query_timing("search_event_summaries", started_at, rows=len(result))
        return result

    def search_daily_summaries(
        self,
        *,
        query: str,
        start_day: Day | None = None,
        end_day_exclusive: Day | None = None,
        limit: int = 1000,
    ) -> list[DailySummaryRecord]:
        started_at = time.perf_counter()
        lowered_query = f"%{_escape_like_pattern(query.strip().lower())}%"
        clauses = ["LOWER(recap_text) LIKE ? ESCAPE '\\'"]
        values: list[object] = [lowered_query]
        if start_day is not None:
            clauses.append("day >= ?")
            values.append(start_day.isoformat())
        if end_day_exclusive is not None:
            clauses.append("day < ?")
            values.append(end_day_exclusive.isoformat())
        values.append(limit)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT id, day, created_ts, recap_text, recap_json, source_batch_count
                FROM daily_summaries
                WHERE {' AND '.join(clauses)}
                ORDER BY day DESC, id DESC
                LIMIT ?
                """,
                values,
            ).fetchall()
        result = [_row_to_daily_summary_record(row) for row in rows]
        self._log_db_query_timing("search_daily_summaries", started_at, rows=len(result))
        return result

    def create_daily_summary(
        self,
        day: Day,
        recap_text: str,
        recap_json: dict | None,
        source_batch_count: int,
    ) -> tuple[DailySummaryRecord, bool]:
        day_key = day.isoformat()
        now = time.time()
        query_started_at = time.perf_counter()
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
        self._log_db_query_timing("create_daily_summary", query_started_at, rows=1)
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


    def get_summary_embedding(self, summary_id: int) -> dict[str, object] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT canonical_hash, embedding_json, embedding_model, embedding_base_url
                FROM summary_embeddings
                WHERE summary_id = ?
                """,
                (summary_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "canonical_hash": str(row["canonical_hash"]),
            "embedding": json.loads(str(row["embedding_json"])),
            "model": str(row["embedding_model"]),
            "base_url": str(row["embedding_base_url"]),
        }

    def upsert_summary_embedding(
        self,
        *,
        summary_id: int,
        canonical_hash: str,
        embedding: list[float],
        model: str,
        base_url: str,
    ) -> None:
        now = time.time()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO summary_embeddings(summary_id, canonical_hash, embedding_json, embedding_model, embedding_base_url, created_ts, updated_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(summary_id) DO UPDATE SET
                    canonical_hash = excluded.canonical_hash,
                    embedding_json = excluded.embedding_json,
                    embedding_model = excluded.embedding_model,
                    embedding_base_url = excluded.embedding_base_url,
                    updated_ts = excluded.updated_ts
                """,
                (summary_id, canonical_hash, json.dumps(embedding), model, base_url, now, now),
            )
            self._conn.commit()

    def replace_coalesced_summaries_for_day(self, day: Day, plans: list[object]) -> list[int]:
        day_key = day.isoformat()
        now = time.time()
        inserted_ids: list[int] = []
        with self._lock:
            existing_rows = self._conn.execute(
                "SELECT id FROM coalesced_summaries WHERE day = ?",
                (day_key,),
            ).fetchall()
            existing_ids = [int(row["id"]) for row in existing_rows]
            if existing_ids:
                placeholders = ",".join("?" for _ in existing_ids)
                self._conn.execute(
                    f"DELETE FROM coalesced_summary_members WHERE coalesced_summary_id IN ({placeholders})",
                    existing_ids,
                )
            self._conn.execute("DELETE FROM coalesced_summaries WHERE day = ?", (day_key,))

            for plan in plans:
                cursor = self._conn.execute(
                    """
                    INSERT INTO coalesced_summaries(day, start_ts, end_ts, summary_text, summary_json, created_ts)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (day_key, plan.start_ts, plan.end_ts, plan.summary_text, json.dumps(plan.summary_json), now),
                )
                coalesced_id = int(cursor.lastrowid)
                self._conn.executemany(
                    """
                    INSERT INTO coalesced_summary_members(coalesced_summary_id, summary_id, member_index)
                    VALUES (?, ?, ?)
                    """,
                    [
                        (coalesced_id, int(summary_id), idx)
                        for idx, summary_id in enumerate(plan.source_summary_ids)
                    ],
                )
                inserted_ids.append(coalesced_id)
            self._conn.commit()
        return inserted_ids

    def replace_coalescing_diagnostics_for_day(self, day: Day, diagnostics: list[object]) -> None:
        day_key = day.isoformat()
        now = time.time()
        with self._lock:
            self._conn.execute("DELETE FROM semantic_merge_diagnostics WHERE day = ?", (day_key,))
            self._conn.executemany(
                """
                INSERT INTO semantic_merge_diagnostics(
                    day, left_summary_id, right_summary_id, embedding_cosine_similarity,
                    app_similarity_score, window_similarity_score, keyword_overlap_score,
                    temporal_gap_seconds, blockers_json, final_merge_score, decision, reasons_json, created_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        day_key,
                        int(item.left_summary_id),
                        int(item.right_summary_id),
                        float(item.semantic_similarity),
                        float(item.app_similarity),
                        float(item.window_similarity),
                        float(item.keyword_overlap),
                        float(item.gap_seconds),
                        json.dumps(item.blockers),
                        float(item.final_score),
                        item.decision,
                        json.dumps(item.reasons),
                        now,
                    )
                    for item in diagnostics
                ],
            )
            self._conn.commit()

    def list_effective_summaries_for_day(self, day: Day, *, use_coalesced: bool) -> list[SummaryRecord]:
        if not use_coalesced:
            return self.list_summaries_for_day(day)
        day_key = day.isoformat()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, start_ts, end_ts, summary_text, summary_json, created_ts
                FROM coalesced_summaries
                WHERE day = ?
                ORDER BY start_ts ASC, id ASC
                """,
                (day_key,),
            ).fetchall()
        if not rows:
            return self.list_summaries_for_day(day)
        return [
            SummaryRecord(
                id=int(row["id"]),
                job_id=-1,
                start_ts=float(row["start_ts"]),
                end_ts=float(row["end_ts"]),
                summary_text=str(row["summary_text"]),
                summary_json=json.loads(str(row["summary_json"])),
                created_ts=float(row["created_ts"]),
            )
            for row in rows
        ]

    def list_semantic_merge_diagnostics(
        self,
        day: Day | None,
        *,
        decision: str | None = None,
        text_query: str | None = None,
        summary_ids: list[int] | None = None,
        max_merge_score: float | None = None,
        limit: int = 200,
    ) -> list[CoalescingDiagnosticRecord]:
        clauses: list[str] = []
        values: list[object] = []
        if day is not None:
            clauses.append("day = ?")
            values.append(day.isoformat())
        if decision:
            clauses.append("decision = ?")
            values.append(decision.strip().lower())
        if text_query:
            like = f"%{_escape_like_pattern(text_query.strip().lower())}%"
            clauses.append(
                "(LOWER(blockers_json) LIKE ? ESCAPE '\\' OR LOWER(reasons_json) LIKE ? ESCAPE '\\')"
            )
            values.extend([like, like])
        if summary_ids:
            normalized_ids = sorted({int(item) for item in summary_ids if int(item) > 0})
            if normalized_ids:
                placeholders = ",".join("?" for _ in normalized_ids)
                clauses.append(
                    f"(left_summary_id IN ({placeholders}) OR right_summary_id IN ({placeholders}))"
                )
                values.extend([*normalized_ids, *normalized_ids])
        if max_merge_score is not None:
            clauses.append("final_merge_score < ?")
            values.append(float(max_merge_score))

        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT
                    id, day, left_summary_id, right_summary_id, embedding_cosine_similarity,
                    app_similarity_score, window_similarity_score, keyword_overlap_score,
                    temporal_gap_seconds, blockers_json, final_merge_score, decision, reasons_json, created_ts
                FROM semantic_merge_diagnostics
                """
                + where_clause
                + """
                ORDER BY id DESC
                LIMIT ?
                """,
                [*values, max(1, int(limit))],
            ).fetchall()
        return [
            CoalescingDiagnosticRecord(
                id=int(row["id"]),
                day=datetime.strptime(str(row["day"]), "%Y-%m-%d").date(),
                left_summary_id=int(row["left_summary_id"]),
                right_summary_id=int(row["right_summary_id"]),
                embedding_cosine_similarity=float(row["embedding_cosine_similarity"]),
                app_similarity_score=float(row["app_similarity_score"]),
                window_similarity_score=float(row["window_similarity_score"]),
                keyword_overlap_score=float(row["keyword_overlap_score"]),
                temporal_gap_seconds=float(row["temporal_gap_seconds"]),
                blockers_json=json.loads(str(row["blockers_json"])),
                final_merge_score=float(row["final_merge_score"]),
                decision=str(row["decision"]),
                reasons_json=json.loads(str(row["reasons_json"])),
                created_ts=float(row["created_ts"]),
            )
            for row in rows
        ]

    def get_coalesced_member_count(self, coalesced_summary_id: int) -> int:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COUNT(1) AS c
                FROM coalesced_summary_members
                WHERE coalesced_summary_id = ?
                """,
                (coalesced_summary_id,),
            ).fetchone()
        return int(row["c"]) if row is not None else 0

    def purge_raw_data(self, start_ts: float, end_ts: float) -> list[str]:
        return self.cleanup_service.purge_raw_data(start_ts, end_ts)

    def get_pending_counts(self) -> dict[str, int]:
        return self.diagnostics_repository.get_pending_counts()

    def count_unprocessed_key_events(self) -> int:
        return self.diagnostics_repository.count_unprocessed_key_events()

    def get_diagnostics_snapshot(self) -> dict:
        return self.diagnostics_repository.get_diagnostics_snapshot()


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


def _row_to_summary_job_dict(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "start_ts": float(row["start_ts"]),
        "end_ts": float(row["end_ts"]),
        "status": str(row["status"]),
        "error": None if row["error"] is None else str(row["error"]),
        "job_type": str(row["job_type"]),
        "target_day": None if row["target_day"] is None else str(row["target_day"]),
        "queued_at": float(row["queued_at"]),
        "created_at": float(row["created_at"]),
        "started_at": None if row["started_at"] is None else float(row["started_at"]),
        "finished_at": None if row["finished_at"] is None else float(row["finished_at"]),
        "timeout_s": float(row["timeout_s"]),
        "attempt": int(row["attempt"]),
        "input_chars": int(row["input_chars"]),
        "input_token_estimate": None if row["input_token_estimate"] is None else int(row["input_token_estimate"]),
        "priority": int(row["priority"]),
        "created_ts": float(row["created_ts"]),
        "updated_ts": float(row["updated_ts"]),
    }


def _normalize_summary_job_status(status: str) -> str:
    normalized = status.strip().lower()
    if normalized == "succeeded":
        return "completed"
    return normalized


def _normalize_day_key(value: Day | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, Day):
        return value.isoformat()
    return str(value)


def _day_epoch_bounds(day: Day) -> tuple[float, float]:
    local_tz = datetime.now().astimezone().tzinfo
    start_dt = datetime.combine(day, DateTimeTime.min, tzinfo=local_tz)
    end_dt = start_dt + timedelta(days=1)
    return start_dt.timestamp(), end_dt.timestamp()


def _escape_like_pattern(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
