from __future__ import annotations

import logging
import threading
import time
from datetime import date as Day
from pathlib import Path

from .capture_repository import CaptureRepository
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
from .summary_repository import SummaryRepository
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
            self.capture_repository = CaptureRepository(
                self._conn,
                self._lock,
                self.logger,
                self._log_db_query_timing,
            )
            self.diagnostics_repository = StorageDiagnosticsRepository(
                self._conn,
                self._lock,
                self.logger,
                self.capture_repository,
            )
            self.cleanup_service = StorageCleanupService(self._conn, self._lock, self.logger)
            self.summary_repository = SummaryRepository(
                self._conn,
                self._lock,
                self.logger,
                self._log_db_query_timing,
            )
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
        key_event_id = self.capture_repository.insert_key_event(event)
        self._record_db_write()
        return key_event_id

    def insert_key_events(self, events: list[KeyEvent]) -> int:
        count = self.capture_repository.insert_key_events(events)
        if count:
            self._record_db_write()
        return count

    def fetch_unprocessed_key_events(self, limit: int = 5000) -> list[KeyEvent]:
        return self.capture_repository.fetch_unprocessed_key_events(limit)

    def mark_key_events_processed(self, ids: list[int]) -> None:
        if not ids:
            return
        self.capture_repository.mark_key_events_processed(ids)
        self._record_db_write()

    def insert_text_segments(self, segments: list[TextSegment]) -> None:
        if not segments:
            return
        self.capture_repository.insert_text_segments(segments)
        for _ in segments:
            self._record_db_write()

    def fetch_unsummarized_text_segments(self, limit: int = 200) -> list[TextSegment]:
        return self.capture_repository.fetch_unsummarized_text_segments(limit)

    def insert_screenshot(self, screenshot: ScreenshotRecord) -> int:
        screenshot_id = self.capture_repository.insert_screenshot(screenshot)
        self._record_db_write()
        return screenshot_id

    def fetch_unsummarized_screenshots(self, limit: int = 20) -> list[ScreenshotRecord]:
        return self.capture_repository.fetch_unsummarized_screenshots(limit)

    def fetch_recent_screenshots(self, limit: int = 20) -> list[ScreenshotRecord]:
        return self.capture_repository.fetch_recent_screenshots(limit)

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
        return self.summary_repository.create_summary_job(
            start_ts,
            end_ts,
            status,
            job_type=job_type,
            target_day=target_day,
            timeout_s=timeout_s,
            attempt=attempt,
            input_chars=input_chars,
            input_token_estimate=input_token_estimate,
            priority=priority,
        )

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
        self.summary_repository.update_summary_job(
            job_id,
            status,
            error,
            job_type=job_type,
            started_at=started_at,
            finished_at=finished_at,
            timeout_s=timeout_s,
            attempt=attempt,
            input_chars=input_chars,
            input_token_estimate=input_token_estimate,
            priority=priority,
        )

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
        return self.summary_repository.create_or_reuse_daily_summary_job(
            day,
            start_ts,
            end_ts,
            status=status,
            timeout_s=timeout_s,
            attempt=attempt,
            input_chars=input_chars,
            input_token_estimate=input_token_estimate,
            priority=priority,
        )

    def get_summary_job(self, job_id: int) -> dict[str, object] | None:
        return self.summary_repository.get_summary_job(job_id)

    def get_daily_summary_job_for_day(self, day: Day) -> dict[str, object] | None:
        return self.summary_repository.get_daily_summary_job_for_day(day)

    def get_summary_job_status_counts(self) -> dict[str, int]:
        return self.summary_repository.get_summary_job_status_counts()

    def insert_summary(
        self,
        job_id: int,
        start_ts: float,
        end_ts: float,
        summary_text: str,
        summary_json: dict,
    ) -> int:
        return self.summary_repository.insert_summary(job_id, start_ts, end_ts, summary_text, summary_json)

    def update_summary_record(
        self,
        summary_id: int,
        *,
        start_ts: float | None = None,
        end_ts: float | None = None,
        summary_text: str | None = None,
        summary_json: dict | None = None,
    ) -> None:
        self.summary_repository.update_summary_record(
            summary_id,
            start_ts=start_ts,
            end_ts=end_ts,
            summary_text=summary_text,
            summary_json=summary_json,
        )

    def list_summaries(self, limit: int = 100) -> list[SummaryRecord]:
        return self.summary_repository.list_summaries(limit)

    def list_summary_days(self, limit: int = 366) -> list[Day]:
        return self.summary_repository.list_summary_days(limit)

    def list_summaries_for_day(self, day: Day, limit: int = 500) -> list[SummaryRecord]:
        return self.summary_repository.list_summaries_for_day(day, limit)

    def count_batch_summaries_for_day(self, day: Day) -> int:
        return self.summary_repository.count_batch_summaries_for_day(day)

    def search_event_summaries(
        self,
        *,
        query: str,
        start_ts: float | None = None,
        end_ts: float | None = None,
        limit: int = 1000,
    ) -> list[SummaryRecord]:
        return self.summary_repository.search_event_summaries(
            query=query,
            start_ts=start_ts,
            end_ts=end_ts,
            limit=limit,
        )

    def search_daily_summaries(
        self,
        *,
        query: str,
        start_day: Day | None = None,
        end_day_exclusive: Day | None = None,
        limit: int = 1000,
    ) -> list[DailySummaryRecord]:
        return self.summary_repository.search_daily_summaries(
            query=query,
            start_day=start_day,
            end_day_exclusive=end_day_exclusive,
            limit=limit,
        )

    def create_daily_summary(
        self,
        day: Day,
        recap_text: str,
        recap_json: dict | None,
        source_batch_count: int,
    ) -> tuple[DailySummaryRecord, bool]:
        return self.summary_repository.create_daily_summary(day, recap_text, recap_json, source_batch_count)

    def get_daily_summary_for_day(self, day: Day) -> DailySummaryRecord | None:
        return self.summary_repository.get_daily_summary_for_day(day)

    def get_summary_embedding(self, summary_id: int) -> dict[str, object] | None:
        return self.summary_repository.get_summary_embedding(summary_id)

    def upsert_summary_embedding(
        self,
        *,
        summary_id: int,
        canonical_hash: str,
        embedding: list[float],
        model: str,
        base_url: str,
    ) -> None:
        self.summary_repository.upsert_summary_embedding(
            summary_id=summary_id,
            canonical_hash=canonical_hash,
            embedding=embedding,
            model=model,
            base_url=base_url,
        )

    def replace_coalesced_summaries_for_day(self, day: Day, plans: list[object]) -> list[int]:
        return self.summary_repository.replace_coalesced_summaries_for_day(day, plans)

    def replace_coalescing_diagnostics_for_day(self, day: Day, diagnostics: list[object]) -> None:
        self.summary_repository.replace_coalescing_diagnostics_for_day(day, diagnostics)

    def list_effective_summaries_for_day(self, day: Day, *, use_coalesced: bool) -> list[SummaryRecord]:
        return self.summary_repository.list_effective_summaries_for_day(day, use_coalesced=use_coalesced)

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
        return self.summary_repository.list_semantic_merge_diagnostics(
            day,
            decision=decision,
            text_query=text_query,
            summary_ids=summary_ids,
            max_merge_score=max_merge_score,
            limit=limit,
        )

    def list_audit_summaries(
        self,
        *,
        start_ts: float | None = None,
        end_ts: float | None = None,
    ) -> list[dict[str, object]]:
        return self.summary_repository.list_audit_summaries(start_ts=start_ts, end_ts=end_ts)

    def list_audit_daily_summaries(
        self,
        *,
        start_day: Day | None = None,
        end_day_exclusive: Day | None = None,
    ) -> list[dict[str, object]]:
        return self.summary_repository.list_audit_daily_summaries(
            start_day=start_day,
            end_day_exclusive=end_day_exclusive,
        )

    def list_audit_coalesced_summaries(
        self,
        *,
        start_day: Day | None = None,
        end_day_exclusive: Day | None = None,
    ) -> list[dict[str, object]]:
        return self.summary_repository.list_audit_coalesced_summaries(
            start_day=start_day,
            end_day_exclusive=end_day_exclusive,
        )

    def list_audit_merge_diagnostics(
        self,
        *,
        start_day: Day | None = None,
        end_day_exclusive: Day | None = None,
    ) -> list[dict[str, object]]:
        return self.summary_repository.list_audit_merge_diagnostics(
            start_day=start_day,
            end_day_exclusive=end_day_exclusive,
        )

    def get_coalesced_member_count(self, coalesced_summary_id: int) -> int:
        return self.summary_repository.get_coalesced_member_count(coalesced_summary_id)

    def purge_raw_data(self, start_ts: float, end_ts: float) -> list[str]:
        return self.cleanup_service.purge_raw_data(start_ts, end_ts)

    def get_pending_counts(self) -> dict[str, int]:
        return self.diagnostics_repository.get_pending_counts()

    def count_unprocessed_key_events(self) -> int:
        return self.capture_repository.count_unprocessed_key_events()

    def get_diagnostics_snapshot(self) -> dict:
        return self.diagnostics_repository.get_diagnostics_snapshot()
