from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from datetime import date as Day, datetime, time as DateTimeTime, timedelta
from typing import Protocol

from .models import CoalescingDiagnosticRecord, DailySummaryRecord, SummaryRecord


_SUMMARY_JOB_TERMINAL_STATUSES = {
    "completed",
    "succeeded",
    "failed",
    "timed_out",
    "cancelled",
    "abandoned",
}

_SUMMARY_JOB_ACTIVE_STATUSES = {"queued", "running"}


class _LogDbQueryTiming(Protocol):
    def __call__(self, operation: str, started_at: float, *, rows: int | None = None) -> None: ...


class SummaryRepository:
    """SQLite-backed summary persistence for batch and daily recap records."""

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
            rows = self._conn.execute("SELECT status, COUNT(1) AS c FROM summary_jobs GROUP BY status").fetchall()
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
        result = [_row_to_summary_record(row) for row in rows]
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
        result = [_row_to_summary_record(row) for row in rows]
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
        result = [_row_to_summary_record(row) for row in rows]
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
        return [_row_to_summary_record(row, job_id=-1) for row in rows]

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
        return [_row_to_coalescing_diagnostic_record(row) for row in rows]

    def list_audit_summaries(
        self,
        *,
        start_ts: float | None = None,
        end_ts: float | None = None,
    ) -> list[dict[str, object]]:
        clauses: list[str] = []
        values: list[object] = []
        if start_ts is not None:
            clauses.append("s.start_ts >= ?")
            values.append(float(start_ts))
        if end_ts is not None:
            clauses.append("s.start_ts < ?")
            values.append(float(end_ts))
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT
                    s.id,
                    s.job_id,
                    s.start_ts,
                    s.end_ts,
                    s.summary_text,
                    s.summary_json,
                    s.created_ts,
                    sj.job_type,
                    sj.target_day
                FROM summaries AS s
                LEFT JOIN summary_jobs AS sj ON sj.id = s.job_id
                """
                + where_clause
                + """
                ORDER BY s.start_ts ASC, s.id ASC
                """,
                values,
            ).fetchall()
        return [
            {
                "summary_id": int(row["id"]),
                "job_id": int(row["job_id"]),
                "start_ts": float(row["start_ts"]),
                "end_ts": float(row["end_ts"]),
                "summary_text": str(row["summary_text"]),
                "summary_json": json.loads(str(row["summary_json"])),
                "created_ts": float(row["created_ts"]),
                "job_type": None if row["job_type"] is None else str(row["job_type"]),
                "target_day": None if row["target_day"] is None else str(row["target_day"]),
            }
            for row in rows
        ]

    def list_audit_daily_summaries(
        self,
        *,
        start_day: Day | None = None,
        end_day_exclusive: Day | None = None,
    ) -> list[dict[str, object]]:
        clauses: list[str] = []
        values: list[object] = []
        if start_day is not None:
            clauses.append("day >= ?")
            values.append(start_day.isoformat())
        if end_day_exclusive is not None:
            clauses.append("day < ?")
            values.append(end_day_exclusive.isoformat())
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, day, created_ts, recap_text, recap_json, source_batch_count
                FROM daily_summaries
                """
                + where_clause
                + """
                ORDER BY day ASC, id ASC
                """,
                values,
            ).fetchall()
        return [
            {
                "daily_summary_id": int(row["id"]),
                "day": str(row["day"]),
                "created_ts": float(row["created_ts"]),
                "recap_text": str(row["recap_text"]),
                "recap_json": json.loads(str(row["recap_json"])) if row["recap_json"] else None,
                "source_batch_count": int(row["source_batch_count"]),
            }
            for row in rows
        ]

    def list_audit_coalesced_summaries(
        self,
        *,
        start_day: Day | None = None,
        end_day_exclusive: Day | None = None,
    ) -> list[dict[str, object]]:
        clauses: list[str] = []
        values: list[object] = []
        if start_day is not None:
            clauses.append("cs.day >= ?")
            values.append(start_day.isoformat())
        if end_day_exclusive is not None:
            clauses.append("cs.day < ?")
            values.append(end_day_exclusive.isoformat())
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT
                    cs.id,
                    cs.day,
                    cs.start_ts,
                    cs.end_ts,
                    cs.summary_text,
                    cs.summary_json,
                    cs.created_ts
                FROM coalesced_summaries AS cs
                """
                + where_clause
                + """
                ORDER BY cs.day ASC, cs.start_ts ASC, cs.id ASC
                """,
                values,
            ).fetchall()
            members = self._conn.execute(
                """
                SELECT csm.coalesced_summary_id, csm.summary_id, csm.member_index
                FROM coalesced_summary_members AS csm
                INNER JOIN coalesced_summaries AS cs
                    ON cs.id = csm.coalesced_summary_id
                """
                + where_clause
                + """
                ORDER BY csm.coalesced_summary_id ASC, csm.member_index ASC
                """,
                values,
            ).fetchall()
        members_by_id: dict[int, list[int]] = {}
        for item in members:
            coalesced_id = int(item["coalesced_summary_id"])
            members_by_id.setdefault(coalesced_id, []).append(int(item["summary_id"]))
        return [
            {
                "coalesced_id": int(row["id"]),
                "day": str(row["day"]),
                "start_ts": float(row["start_ts"]),
                "end_ts": float(row["end_ts"]),
                "summary_text": str(row["summary_text"]),
                "summary_json": json.loads(str(row["summary_json"])),
                "created_ts": float(row["created_ts"]),
                "member_summary_ids": members_by_id.get(int(row["id"]), []),
            }
            for row in rows
        ]

    def list_audit_merge_diagnostics(
        self,
        *,
        start_day: Day | None = None,
        end_day_exclusive: Day | None = None,
    ) -> list[dict[str, object]]:
        clauses: list[str] = []
        values: list[object] = []
        if start_day is not None:
            clauses.append("day >= ?")
            values.append(start_day.isoformat())
        if end_day_exclusive is not None:
            clauses.append("day < ?")
            values.append(end_day_exclusive.isoformat())
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
                ORDER BY day ASC, id ASC
                """,
                values,
            ).fetchall()
        return [
            {
                "diagnostic_id": int(row["id"]),
                "day": str(row["day"]),
                "left_summary_id": int(row["left_summary_id"]),
                "right_summary_id": int(row["right_summary_id"]),
                "embedding_cosine_similarity": float(row["embedding_cosine_similarity"]),
                "app_similarity_score": float(row["app_similarity_score"]),
                "window_similarity_score": float(row["window_similarity_score"]),
                "keyword_overlap_score": float(row["keyword_overlap_score"]),
                "temporal_gap_seconds": float(row["temporal_gap_seconds"]),
                "blockers_json": json.loads(str(row["blockers_json"])),
                "final_merge_score": float(row["final_merge_score"]),
                "decision": str(row["decision"]),
                "reasons_json": json.loads(str(row["reasons_json"])),
                "created_ts": float(row["created_ts"]),
            }
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


def _row_to_summary_record(row: sqlite3.Row, *, job_id: int | None = None) -> SummaryRecord:
    return SummaryRecord(
        id=int(row["id"]),
        job_id=int(row["job_id"]) if job_id is None else job_id,
        start_ts=float(row["start_ts"]),
        end_ts=float(row["end_ts"]),
        summary_text=str(row["summary_text"]),
        summary_json=json.loads(str(row["summary_json"])),
        created_ts=float(row["created_ts"]),
    )


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


def _row_to_coalescing_diagnostic_record(row: sqlite3.Row) -> CoalescingDiagnosticRecord:
    return CoalescingDiagnosticRecord(
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
