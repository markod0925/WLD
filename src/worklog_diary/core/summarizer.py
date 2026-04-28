from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from .batching import BatchBuilder, SummaryBatch
from .error_notifications import ErrorNotificationManager
from .errors import LMStudioConnectionError, LMStudioServiceUnavailableError, LMStudioTimeoutError
from .lmstudio_client import LMStudioClient
from .lmstudio_logging import get_failed_stage, llm_job_context, log_llm_stage, safe_error
from .llm_job_queue import LLMJobCancelledError, LLMJobMetadata
from .storage import SQLiteStorage
from .summary_dedup import SummaryDeduplicator
from .semantic_coalescing import SemanticCoalescer


@dataclass(slots=True)
class _QueuedSummaryJob:
    job_id: int
    batch: SummaryBatch
    reason: str


@dataclass(slots=True)
class _WorkerHandle:
    thread: threading.Thread
    stop_event: threading.Event


class Summarizer:
    def __init__(
        self,
        storage: SQLiteStorage,
        batch_builder: BatchBuilder,
        lm_client: LMStudioClient,
        max_parallel_jobs: int = 2,
        error_notifier: ErrorNotificationManager | None = None,
        shutdown_event: threading.Event | None = None,
        summary_deduplicator: SummaryDeduplicator | None = None,
        semantic_coalescer: SemanticCoalescer | None = None,
        process_backlog_only_while_locked: bool = True,
    ) -> None:
        self.storage = storage
        self.batch_builder = batch_builder
        self.lm_client = lm_client
        self.logger = logging.getLogger(__name__)
        self.error_notifier = error_notifier or ErrorNotificationManager()
        self.summary_deduplicator = summary_deduplicator or SummaryDeduplicator()
        self.semantic_coalescer = semantic_coalescer

        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._queue: deque[_QueuedSummaryJob] = deque()
        self._reserved_ranges: dict[int, tuple[float, float]] = {}
        self._running_jobs: set[int] = set()
        self._daily_recap_inflight: set[str] = set()
        self._workers: list[_WorkerHandle] = []
        self._retired_workers: list[_WorkerHandle] = []
        self._shutdown_event = shutdown_event or threading.Event()
        self._stop_event = threading.Event()
        self._max_parallel_jobs = max(1, int(max_parallel_jobs))
        self._unrecoverable_error: str | None = None
        self._process_backlog_only_while_locked = bool(process_backlog_only_while_locked)
        self._session_locked: bool | None = None
        self._admission_paused = False
        self._accepting_new_jobs = True
        self._last_admission_state: str | None = None

        self._ensure_worker_count_locked()
        self.logger.info(
            "event=summary_dispatcher_started max_concurrent_summary_llm_requests=%s",
            self._max_parallel_jobs,
        )
        self._emit_admission_state_if_changed(reason="startup")

    def stop(self) -> dict[str, int]:
        self.logger.info("event=summary_workers_join_start")
        self._stop_event.set()
        cancelled = 0
        with self._condition:
            self._accepting_new_jobs = False
            while self._queue:
                item = self._queue.popleft()
                cancelled += 1
                self._reserved_ranges.pop(item.job_id, None)
                self.storage.update_summary_job(item.job_id, status="cancelled", error="shutdown")
        with self._condition:
            for handle in self._workers:
                handle.stop_event.set()
            for handle in self._retired_workers:
                handle.stop_event.set()
            self._condition.notify_all()
        job_queue = self._get_lmstudio_job_queue()
        if job_queue is not None:
            job_queue.stop()
        for handle in self._workers + self._retired_workers:
            handle.thread.join(timeout=2)
        runtime = self.get_runtime_status()
        self.logger.info(
            "event=summary_workers_joined running=%s queued=%s cancelled=%s failed=%s",
            runtime["running_jobs"],
            runtime["queued_jobs"],
            cancelled,
            runtime["failed_jobs"],
        )
        self._workers.clear()
        self._retired_workers.clear()
        return {"cancelled": int(cancelled)}

    def stop_accepting_new_jobs(self) -> None:
        with self._condition:
            self._accepting_new_jobs = False
            self._condition.notify_all()
        self.logger.info("event=summary_admission_stopped")
        self.logger.info("event=summary_admission_decision allowed=false reason=shutdown lock_state=%s trigger=shutdown", self._lock_state_label())
        self._emit_admission_state_if_changed(reason="shutdown")

    def update_max_parallel_jobs(self, max_parallel_jobs: int) -> None:
        with self._condition:
            self._max_parallel_jobs = max(1, int(max_parallel_jobs))
            if len(self._workers) > self._max_parallel_jobs:
                retired = self._workers[self._max_parallel_jobs :]
                self._workers = self._workers[: self._max_parallel_jobs]
                for handle in retired:
                    handle.stop_event.set()
                self._retired_workers.extend(retired)
            self._ensure_worker_count_locked()
            self._condition.notify_all()
            effective = self._max_parallel_jobs
        self.logger.info("event=summary_dispatcher_concurrency_updated max_concurrent_summary_llm_requests=%s", effective)

    def set_process_backlog_only_while_locked(self, enabled: bool) -> None:
        with self._condition:
            self._process_backlog_only_while_locked = bool(enabled)
            self._condition.notify_all()
            paused_now = self._is_summary_admission_paused_locked()
            self._log_admission_config_locked()
        self._emit_admission_state_if_changed(reason="config")
        if not paused_now and self._admission_paused:
            self._admission_paused = False
            self.logger.info("event=summary_admission_resumed reason=config_disabled")

    def handle_session_lock_state_change(self, is_locked: bool) -> None:
        with self._condition:
            previous = self._session_locked
            self._session_locked = bool(is_locked)
            self._condition.notify_all()
            paused_now = self._is_summary_admission_paused_locked()
            self._log_admission_config_locked()
        self._emit_admission_state_if_changed(reason="lock_change")

        if not self._process_backlog_only_while_locked:
            return
        if is_locked:
            if self._admission_paused:
                self._admission_paused = False
                self.logger.info("event=summary_admission_resumed reason=pc_locked message=\"summary processing resumed because PC locked\"")
            elif previous is False:
                self.logger.info("event=summary_admission_state reason=pc_locked")
        else:
            if previous is True:
                self.logger.info(
                    "event=summary_admission_paused reason=pc_unlocked message=\"summary processing stopped admitting new jobs because PC unlocked\""
                )
                self._admission_paused = paused_now

    def clear_unrecoverable_error(self) -> None:
        with self._lock:
            self._unrecoverable_error = None

    def get_unrecoverable_error(self) -> str | None:
        with self._lock:
            return self._unrecoverable_error

    def has_unrecoverable_error(self) -> bool:
        return self.get_unrecoverable_error() is not None

    def dispatch_pending_jobs(
        self,
        reason: str = "manual",
        max_new_jobs: int | None = None,
        *,
        force_flush: bool = False,
    ) -> int:
        if self._stop_event.is_set() or self._shutdown_event.is_set() or not self._accepting_new_jobs:
            return 0
        with self._condition:
            available_slots = self._max_parallel_jobs - (len(self._queue) + len(self._running_jobs))
            if max_new_jobs is not None:
                available_slots = min(available_slots, max_new_jobs)
            if available_slots <= 0:
                return 0

            created = 0
            for _ in range(available_slots):
                excluded_ranges = list(self._reserved_ranges.values())
                batch = self.batch_builder.build_pending_batch(
                    excluded_ranges=excluded_ranges,
                    force_flush=force_flush or reason != "scheduled",
                )
                if batch is None:
                    log_llm_stage(
                        self.logger,
                        "submission_decision",
                        "skip",
                        job_id="none",
                        job_type="event_summary",
                        reason="no_content",
                        detail="no_pending_data",
                    )
                    break

                input_chars = sum(len(segment.text) for segment in batch.text_segments)
                timeout_s = self._lmstudio_timeout_seconds()
                job_id = self.storage.create_summary_job(
                    batch.start_ts,
                    batch.end_ts,
                    status="queued",
                    job_type="event_summary",
                    timeout_s=timeout_s,
                    attempt=1,
                    input_chars=input_chars,
                    input_token_estimate=_estimate_token_count(input_chars),
                )
                self._queue.append(_QueuedSummaryJob(job_id=job_id, batch=batch, reason=reason))
                self._reserved_ranges[job_id] = (batch.start_ts, batch.end_ts)
                created += 1

                log_llm_stage(
                    self.logger,
                    "job_created",
                    "ok",
                    job_id=job_id,
                    job_type="event_summary",
                    timeout_s=timeout_s,
                    attempt=1,
                    reason=reason,
                    start_ts=batch.start_ts,
                    end_ts=batch.end_ts,
                    screenshots=len(batch.screenshots),
                    input_chars=input_chars,
                    input_token_estimate=_estimate_token_count(input_chars),
                    queue_size=len(self._queue),
                )

            if created:
                self._condition.notify_all()
            return created

    def cancel_queued_jobs(self, reason: str = "cancelled") -> int:
        with self._condition:
            if not self._queue:
                return 0
            cancelled = list(self._queue)
            self._queue.clear()
            for item in cancelled:
                self.storage.update_summary_job(item.job_id, status="cancelled", error=reason)
                input_chars = sum(len(segment.text) for segment in item.batch.text_segments)
                log_llm_stage(
                    self.logger,
                    "job_cancelled",
                    "skip",
                    job_id=item.job_id,
                    job_type="event_summary",
                    timeout_s=self._lmstudio_timeout_seconds(),
                    attempt=1,
                    input_chars=input_chars,
                    input_token_estimate=_estimate_token_count(input_chars),
                    reason=reason,
                    queue_size=len(self._queue),
                )
                self._reserved_ranges.pop(item.job_id, None)
            self._condition.notify_all()
            return len(cancelled)

    def wait_for_activity(self, timeout_seconds: float = 0.5) -> None:
        if self._shutdown_event.is_set():
            return
        with self._condition:
            self._condition.wait(timeout=timeout_seconds)

    def get_runtime_status(self) -> dict[str, int | bool | str | None]:
        with self._lock:
            queued_jobs = len(self._queue)
            running_jobs = len(self._running_jobs)
            max_parallel = self._max_parallel_jobs
            unrecoverable_error = self._unrecoverable_error
            admission_paused = self._is_summary_admission_paused_locked()
            session_locked = self._session_locked

        try:
            persisted_counts = self.storage.get_summary_job_status_counts()
        except Exception:
            persisted_counts = {}
        llm_queue = self._get_lmstudio_queue_snapshot()
        return {
            "queued_jobs": queued_jobs,
            "running_jobs": running_jobs,
            "pending_summary_jobs": queued_jobs + running_jobs,
            "completed_jobs": int(persisted_counts.get("completed", persisted_counts.get("succeeded", 0))),
            "failed_jobs": int(persisted_counts.get("failed", 0)),
            "timed_out_jobs": int(persisted_counts.get("timed_out", 0)),
            "cancelled_jobs": int(persisted_counts.get("cancelled", 0)),
            "abandoned_jobs": int(persisted_counts.get("abandoned", 0)),
            "max_concurrent_summary_llm_requests": max_parallel,
            "has_unrecoverable_error": unrecoverable_error is not None,
            "unrecoverable_error": unrecoverable_error,
            "llm_queue_queued_jobs": int(llm_queue["queued_jobs"]),
            "llm_queue_running_jobs": int(llm_queue["running_jobs"]),
            "llm_queue_pending_jobs": int(llm_queue["pending_jobs"]),
            "llm_queue_max_concurrent_jobs": int(llm_queue["max_concurrent_jobs"]),
            "llm_queue_accepting_jobs": bool(llm_queue["accepting_jobs"]),
            "llm_queue_closing": bool(llm_queue["closing"]),
            "llm_queue_closed": bool(llm_queue["closed"]),
            "summary_admission_paused": admission_paused,
            "process_backlog_only_while_locked": self._process_backlog_only_while_locked,
            "session_locked": session_locked,
        }

    def flush_pending(self, reason: str = "manual", *, force_flush: bool = False) -> int | None:
        if self._stop_event.is_set() or self._shutdown_event.is_set():
            return None
        batch = self.batch_builder.build_pending_batch(
            excluded_ranges=list(self._reserved_ranges.values()),
            force_flush=force_flush or reason != "scheduled",
        )
        if batch is None:
            log_llm_stage(
                self.logger,
                "submission_decision",
                "skip",
                job_id="none",
                job_type="event_summary",
                reason="no_content",
                detail="no_pending_data",
            )
            return None

        input_chars = sum(len(segment.text) for segment in batch.text_segments)
        timeout_s = self._lmstudio_timeout_seconds()
        job_id = self.storage.create_summary_job(
            batch.start_ts,
            batch.end_ts,
            status="queued",
            job_type="event_summary",
            timeout_s=timeout_s,
            attempt=1,
            input_chars=input_chars,
            input_token_estimate=_estimate_token_count(input_chars),
        )
        log_llm_stage(
            self.logger,
            "job_created",
            "ok",
            job_id=job_id,
            job_type="event_summary",
            timeout_s=timeout_s,
            attempt=1,
            reason=reason,
            start_ts=batch.start_ts,
            end_ts=batch.end_ts,
            screenshots=len(batch.screenshots),
            input_chars=input_chars,
            input_token_estimate=_estimate_token_count(input_chars),
        )
        return self._run_summary_job(job_id=job_id, batch=batch, reason=reason)

    def generate_daily_recap_for_day(self, day: date) -> tuple[int, bool]:
        day_key = day.isoformat()
        with self._condition:
            while day_key in self._daily_recap_inflight:
                self._condition.wait(timeout=0.5)
            self._daily_recap_inflight.add(day_key)

        try:
            existing_daily_summary = self.storage.get_daily_summary_for_day(day)
            if existing_daily_summary is not None:
                existing_job = self.storage.get_daily_summary_job_for_day(day)
                if existing_job is not None and str(existing_job["status"]) != "completed":
                    started_at = existing_job["started_at"] if existing_job["started_at"] is not None else existing_job["queued_at"]
                    self.storage.update_summary_job(
                        int(existing_job["id"]),
                        status="completed",
                        job_type="day_summary",
                        started_at=float(started_at) if started_at is not None else existing_daily_summary.created_ts,
                        finished_at=existing_daily_summary.created_ts,
                        timeout_s=float(existing_job["timeout_s"]),
                        attempt=int(existing_job["attempt"]),
                        input_chars=int(existing_job["input_chars"]),
                        input_token_estimate=existing_job["input_token_estimate"],
                        priority=int(existing_job["priority"]),
                    )
                    self.logger.info(
                        "event=daily_summary_job_reconciled day=%s job_id=%s daily_summary_id=%s",
                        day_key,
                        int(existing_job["id"]),
                        int(existing_daily_summary.id or 0),
                    )
                else:
                    self.logger.info(
                        "event=daily_summary_job_reused day=%s daily_summary_id=%s",
                        day_key,
                        int(existing_daily_summary.id or 0),
                    )
                return int(existing_daily_summary.id or 0), False

            use_coalesced = bool(self.semantic_coalescer and self.semantic_coalescer.enabled)
            summaries = self.storage.list_effective_summaries_for_day(day, use_coalesced=use_coalesced)
            if not summaries:
                raise ValueError(f"No summaries available for day {day.isoformat()}")

            input_chars = sum(len(summary.summary_text) for summary in summaries)
            timeout_s = self._lmstudio_daily_timeout_seconds()
            day_start = datetime.combine(day, datetime.min.time())
            day_end = day_start + timedelta(days=1)
            job_id, reused = self.storage.create_or_reuse_daily_summary_job(
                day=day,
                start_ts=day_start.timestamp(),
                end_ts=day_end.timestamp(),
                status="queued",
                timeout_s=timeout_s,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=_estimate_token_count(input_chars),
            )
            log_llm_stage(
                self.logger,
                "job_created",
                "ok",
                job_id=job_id,
                job_type="day_summary",
                timeout_s=timeout_s,
                attempt=1,
                day=day_key,
                source_summaries=len(summaries),
                input_chars=input_chars,
                input_token_estimate=_estimate_token_count(input_chars),
                reused=reused,
            )

            def _on_started(metadata: LLMJobMetadata) -> None:
                queue_wait_s = max(0.0, metadata.started_at - metadata.queued_at) if metadata.started_at is not None else 0.0
                self.storage.update_summary_job(
                    job_id,
                    status="running",
                    job_type="day_summary",
                    started_at=metadata.started_at,
                    timeout_s=timeout_s,
                    attempt=metadata.attempt,
                    input_chars=metadata.input_chars,
                    input_token_estimate=metadata.input_token_estimate,
                    priority=metadata.priority,
                )
                log_llm_stage(
                    self.logger,
                    "daily_summary_job_started",
                    "ok",
                    job_id=job_id,
                    job_type="day_summary",
                    timeout_s=timeout_s,
                    attempt=metadata.attempt,
                    day=day_key,
                    source_summaries=len(summaries),
                    input_chars=metadata.input_chars,
                    input_token_estimate=metadata.input_token_estimate,
                    queue_wait_s=queue_wait_s,
                )

            def _on_cancelled(metadata: LLMJobMetadata, cancel_reason: str) -> None:
                self.storage.update_summary_job(
                    job_id,
                    status="cancelled",
                    error=cancel_reason,
                    job_type="day_summary",
                    finished_at=metadata.finished_at,
                    timeout_s=timeout_s,
                    attempt=metadata.attempt,
                    input_chars=metadata.input_chars,
                    input_token_estimate=metadata.input_token_estimate,
                    priority=metadata.priority,
                )
                log_llm_stage(
                    self.logger,
                    "daily_summary_job_cancelled",
                    "skip",
                    job_id=job_id,
                    job_type="day_summary",
                    timeout_s=timeout_s,
                    attempt=metadata.attempt,
                    day=day_key,
                    source_summaries=len(summaries),
                    reason=cancel_reason,
                )

            try:
                with llm_job_context(
                    f"daily_recap:{day.isoformat()}",
                    job_type="day_summary",
                    timeout_s=timeout_s,
                    attempt=1,
                    input_chars=input_chars,
                    input_token_estimate=_estimate_token_count(input_chars),
                ):
                    recap_text, recap_json = self.lm_client.summarize_daily_recap(
                        day=day,
                        summaries=summaries,
                        job_id=job_id,
                        job_type="day_summary",
                        on_started=_on_started,
                        on_cancelled=_on_cancelled,
                    )
            except LLMJobCancelledError as exc:
                raise
            except LMStudioTimeoutError as exc:
                self._notify_lmstudio_error(
                    "lmstudio_timeout",
                    str(exc),
                    key=f"{self._lmstudio_identity()}|timeout",
                )
                self.storage.update_summary_job(
                    job_id,
                    status="timed_out",
                    error=str(exc),
                    job_type="day_summary",
                    finished_at=time.time(),
                    timeout_s=timeout_s,
                    attempt=1,
                    input_chars=input_chars,
                    input_token_estimate=_estimate_token_count(input_chars),
                )
                raise
            except LMStudioConnectionError as exc:
                self._notify_lmstudio_error("lmstudio_connection", str(exc), key=f"{self._lmstudio_identity()}|connection")
                self.storage.update_summary_job(
                    job_id,
                    status="failed",
                    error=str(exc),
                    job_type="day_summary",
                    finished_at=time.time(),
                    timeout_s=timeout_s,
                    attempt=1,
                    input_chars=input_chars,
                    input_token_estimate=_estimate_token_count(input_chars),
                )
                raise
            except LMStudioServiceUnavailableError as exc:
                self._notify_lmstudio_error(
                    "lmstudio_service_unavailable",
                    str(exc),
                    key=f"{self._lmstudio_identity()}|unavailable",
                )
                self.storage.update_summary_job(
                    job_id,
                    status="failed",
                    error=str(exc),
                    job_type="day_summary",
                    finished_at=time.time(),
                    timeout_s=timeout_s,
                    attempt=1,
                    input_chars=input_chars,
                    input_token_estimate=_estimate_token_count(input_chars),
                )
                raise
            else:
                self.error_notifier.resolve_many("lmstudio_connection", "lmstudio_service_unavailable", "lmstudio_timeout")

            log_llm_stage(
                self.logger,
                "daily_summary_store",
                "start",
                job_id=job_id,
                job_type="day_summary",
                timeout_s=timeout_s,
                attempt=1,
                day=day_key,
                source_summaries=len(summaries),
            )
            try:
                daily_summary, replaced = self.storage.create_daily_summary(
                    day=day,
                    recap_text=recap_text,
                    recap_json=recap_json if isinstance(recap_json, dict) else None,
                    source_batch_count=len(summaries),
                )
            except Exception as exc:
                self.storage.update_summary_job(
                    job_id,
                    status="failed",
                    error="Daily summary persistence failed.",
                    job_type="day_summary",
                    finished_at=time.time(),
                    timeout_s=timeout_s,
                    attempt=1,
                    input_chars=input_chars,
                    input_token_estimate=_estimate_token_count(input_chars),
                )
                log_llm_stage(
                    self.logger,
                    "daily_summary_store",
                    "error",
                    level=logging.ERROR,
                    job_id=job_id,
                    job_type="day_summary",
                    timeout_s=timeout_s,
                    attempt=1,
                    day=day_key,
                    source_summaries=len(summaries),
                    error_type=exc.__class__.__name__,
                    error=safe_error(exc),
                    exc_info=True,
                )
                raise
            log_llm_stage(
                self.logger,
                "daily_summary_store",
                "ok",
                job_id=job_id,
                job_type="day_summary",
                timeout_s=timeout_s,
                attempt=1,
                day=day_key,
                source_summaries=len(summaries),
                daily_summary_id=daily_summary.id,
                replaced=replaced,
            )
            self.storage.update_summary_job(
                job_id,
                status="completed",
                job_type="day_summary",
                finished_at=time.time(),
                timeout_s=timeout_s,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=_estimate_token_count(input_chars),
            )
            log_llm_stage(
                self.logger,
                "daily_summary_job_completed",
                "ok",
                job_id=job_id,
                job_type="day_summary",
                timeout_s=timeout_s,
                attempt=1,
                day=day_key,
                source_summaries=len(summaries),
                daily_summary_id=daily_summary.id,
                replaced=replaced,
            )
            return int(daily_summary.id or 0), replaced
        finally:
            with self._condition:
                self._daily_recap_inflight.discard(day_key)
                self._condition.notify_all()

    def _worker_loop(self, worker_stop_event: threading.Event) -> None:
        while not self._stop_event.is_set() and not self._shutdown_event.is_set():
            queued_job: _QueuedSummaryJob | None = None
            with self._condition:
                while (
                    not self._stop_event.is_set()
                    and not self._shutdown_event.is_set()
                    and not worker_stop_event.is_set()
                    and (
                        not self._queue
                        or self._next_startable_index_locked() is None
                    )
                ):
                    if self._queue and self._next_startable_index_locked() is None:
                        self._log_admission_paused_once_locked()
                    self._condition.wait(timeout=0.5)
                if self._stop_event.is_set() or self._shutdown_event.is_set() or worker_stop_event.is_set():
                    return
                startable_index = self._next_startable_index_locked()
                if startable_index is not None:
                    queued_job = self._queue[startable_index]
                    del self._queue[startable_index]
                    self._running_jobs.add(queued_job.job_id)
                    input_chars = sum(len(segment.text) for segment in queued_job.batch.text_segments)
                    self.logger.info(
                        (
                            "event=summary_job_dequeued job_id=%s job_type=event_summary timeout_s=%s "
                            "input_chars=%s input_token_estimate=%s queue_size=%s running_jobs=%s"
                        ),
                        queued_job.job_id,
                    self._lmstudio_timeout_seconds(),
                        input_chars,
                        _estimate_token_count(input_chars),
                        len(self._queue),
                        len(self._running_jobs),
                    )

            if queued_job is None:
                continue

            self._run_summary_job(job_id=queued_job.job_id, batch=queued_job.batch, reason=queued_job.reason)

            with self._condition:
                self._running_jobs.discard(queued_job.job_id)
                self._reserved_ranges.pop(queued_job.job_id, None)
                self._condition.notify_all()

            if worker_stop_event.is_set():
                return

    def _next_startable_index_locked(self) -> int | None:
        for index, queued_job in enumerate(self._queue):
            if self._can_start_summary_job_now_locked(reason=queued_job.reason):
                return index
        return None

    def _can_start_summary_job_now_locked(self, *, reason: str) -> bool:
        if not self._accepting_new_jobs:
            self._log_admission_decision_locked(allowed=False, reason="shutdown", trigger=reason)
            return False
        if reason in {"manual", "summary-window"}:
            self._log_admission_decision_locked(allowed=True, reason="bypass", trigger=reason)
            return True
        if not self._process_backlog_only_while_locked:
            self._log_admission_decision_locked(allowed=True, reason="gate_disabled", trigger=reason)
            return True
        if self._session_locked is None:
            self._log_admission_decision_locked(allowed=True, reason="unknown_fail_open", trigger=reason)
            return True
        allowed = self._session_locked
        self._log_admission_decision_locked(
            allowed=allowed,
            reason="pc_locked" if allowed else "pc_unlocked",
            trigger=reason,
        )
        return allowed

    def _is_summary_admission_paused_locked(self) -> bool:
        if not self._process_backlog_only_while_locked:
            return False
        if self._session_locked is None:
            return False
        return not self._session_locked

    def _effective_admission_state_locked(self) -> tuple[str, str]:
        if not self._accepting_new_jobs:
            return "blocked", "shutdown"
        if not self._process_backlog_only_while_locked:
            return "allowed", "gate_disabled"
        if self._session_locked is None:
            return "allowed", "unknown_fail_open"
        if self._session_locked:
            return "allowed", "pc_locked"
        return "blocked", "pc_unlocked"

    def _emit_admission_state_if_changed(self, *, reason: str) -> None:
        with self._condition:
            state, state_reason = self._effective_admission_state_locked()
            if state == self._last_admission_state:
                return
            self._last_admission_state = state
            lock_state = self._lock_state_label()
            gate = self._process_backlog_only_while_locked
        self.logger.info(
            "event=summary_admission_state state=%s reason=%s lock_state=%s process_backlog_only_while_locked=%s trigger=%s",
            state,
            state_reason,
            lock_state,
            gate,
            reason,
        )

    def _log_admission_paused_once_locked(self) -> None:
        if self._admission_paused:
            return
        self._admission_paused = True
        self.logger.info(
            "event=summary_admission_paused reason=pc_unlocked message=\"summary processing paused until PC lock\""
        )
        self.logger.info("event=backlog_waiting_for_pc_lock")

    def _log_admission_config_locked(self) -> None:
        self.logger.info(
            "event=summary_admission_config process_backlog_only_while_locked=%s lock_state=%s fail_open_when_unknown=true",
            self._process_backlog_only_while_locked,
            self._lock_state_label(),
        )

    def _log_admission_decision_locked(self, *, allowed: bool, reason: str, trigger: str) -> None:
        self.logger.info(
            "event=summary_admission_decision allowed=%s reason=%s lock_state=%s trigger=%s",
            allowed,
            reason,
            self._lock_state_label(),
            trigger,
        )

    def _lock_state_label(self) -> str:
        if self._session_locked is None:
            return "unknown"
        return "locked" if self._session_locked else "unlocked"

    def _run_summary_job(
        self,
        job_id: int,
        batch: SummaryBatch,
        reason: str,
        *,
        job_type: str = "event_summary",
        timeout_s: int | None = None,
    ) -> int | None:
        started_at = time.perf_counter()
        input_chars = sum(len(segment.text) for segment in batch.text_segments)
        request_timeout_s = timeout_s if timeout_s is not None else self._lmstudio_timeout_seconds()
        log_llm_stage(
            self.logger,
            "submission_decision",
            "proceed",
            job_id=job_id,
            job_type=job_type,
            timeout_s=request_timeout_s,
            attempt=1,
            reason="ready",
            job_reason=reason,
            screenshots=len(batch.screenshots),
            input_chars=input_chars,
            input_token_estimate=_estimate_token_count(input_chars),
        )

        def _on_started(metadata: LLMJobMetadata) -> None:
            queue_wait_s = max(0.0, metadata.started_at - metadata.queued_at) if metadata.started_at is not None else 0.0
            self.storage.update_summary_job(
                job_id,
                status="running",
                job_type=job_type,
                started_at=metadata.started_at,
                timeout_s=request_timeout_s,
                attempt=metadata.attempt,
                input_chars=metadata.input_chars,
                input_token_estimate=metadata.input_token_estimate,
                priority=metadata.priority,
            )
            log_llm_stage(
                self.logger,
                "summary_job_started",
                "ok",
                job_id=job_id,
                job_type=job_type,
                timeout_s=request_timeout_s,
                attempt=metadata.attempt,
                reason=reason,
                screenshots=len(batch.screenshots),
                input_chars=metadata.input_chars,
                input_token_estimate=metadata.input_token_estimate,
                queue_wait_s=queue_wait_s,
            )

        def _on_cancelled(metadata: LLMJobMetadata, cancel_reason: str) -> None:
            self.storage.update_summary_job(
                job_id,
                status="cancelled",
                error=cancel_reason,
                job_type=job_type,
                finished_at=metadata.finished_at,
                timeout_s=request_timeout_s,
                attempt=metadata.attempt,
                input_chars=metadata.input_chars,
                input_token_estimate=metadata.input_token_estimate,
                priority=metadata.priority,
            )
            log_llm_stage(
                self.logger,
                "summary_job_cancelled",
                "skip",
                job_id=job_id,
                job_type=job_type,
                timeout_s=request_timeout_s,
                attempt=metadata.attempt,
                reason=cancel_reason,
                screenshots=len(batch.screenshots),
                input_chars=metadata.input_chars,
                input_token_estimate=metadata.input_token_estimate,
            )

        try:
            with llm_job_context(
                job_id,
                job_type=job_type,
                timeout_s=request_timeout_s,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=_estimate_token_count(input_chars),
            ):
                summary_text, summary_json = self.lm_client.summarize_batch(
                    batch,
                    job_id=job_id,
                    job_type=job_type,
                    on_started=_on_started,
                    on_cancelled=_on_cancelled,
                )
            summary_payload = self._enrich_summary_payload(batch, summary_text, summary_json, reason=reason)
            recent_summaries = self.storage.list_summaries(limit=self.summary_deduplicator.recent_compare_count)
            dedup_decision = self.summary_deduplicator.evaluate(
                batch=batch,
                summary_text=summary_text,
                summary_json=summary_payload,
                recent_summaries=recent_summaries,
            )

            summary_id: int | None = None
            if dedup_decision.action == "merge_previous" and dedup_decision.matched_summary_id is not None:
                merged_record = next(
                    (item for item in recent_summaries if item.id == dedup_decision.matched_summary_id),
                    None,
                )
                if merged_record is not None:
                    merged_payload = self._merge_summary_payload(
                        merged_record.summary_json,
                        batch=batch,
                        new_summary_text=summary_text,
                        new_summary_payload=summary_payload,
                        reason=reason,
                        similarity=dedup_decision.similarity,
                    )
                    log_llm_stage(
                        self.logger,
                        "summary_store",
                        "start",
                        job_id=job_id,
                        job_type=job_type,
                        timeout_s=request_timeout_s,
                        attempt=1,
                        action="merge_previous",
                        summary_id=merged_record.id,
                    )
                    self.storage.update_summary_record(
                        merged_record.id or 0,
                        end_ts=max(merged_record.end_ts, batch.end_ts),
                        summary_json=merged_payload,
                    )
                    summary_id = merged_record.id
                    log_llm_stage(
                        self.logger,
                        "summary_store",
                        "ok",
                        job_id=job_id,
                        job_type=job_type,
                        timeout_s=request_timeout_s,
                        attempt=1,
                        action="merge_previous",
                        summary_id=summary_id,
                        matched_summary_id=dedup_decision.matched_summary_id,
                        similarity=dedup_decision.similarity,
                    )
                else:
                    log_llm_stage(
                        self.logger,
                        "summary_store",
                        "start",
                        job_id=job_id,
                        job_type=job_type,
                        timeout_s=request_timeout_s,
                        attempt=1,
                        action="insert",
                    )
                    summary_id = self.storage.insert_summary(
                        job_id=job_id,
                        start_ts=batch.start_ts,
                        end_ts=batch.end_ts,
                        summary_text=summary_text,
                        summary_json=summary_payload,
                    )
                    log_llm_stage(
                        self.logger,
                        "summary_store",
                        "ok",
                        job_id=job_id,
                        job_type=job_type,
                        timeout_s=request_timeout_s,
                        attempt=1,
                        action="insert",
                        summary_id=summary_id,
                    )
            elif dedup_decision.action == "suppress":
                log_llm_stage(
                    self.logger,
                    "summary_store",
                    "skip",
                    job_id=job_id,
                    job_type=job_type,
                    timeout_s=request_timeout_s,
                    attempt=1,
                    reason=dedup_decision.reason,
                    matched_summary_id=dedup_decision.matched_summary_id,
                    similarity=dedup_decision.similarity,
                )
            else:
                log_llm_stage(
                    self.logger,
                    "summary_store",
                    "start",
                    job_id=job_id,
                    job_type=job_type,
                    timeout_s=request_timeout_s,
                    attempt=1,
                    action="insert",
                )
                summary_id = self.storage.insert_summary(
                    job_id=job_id,
                    start_ts=batch.start_ts,
                    end_ts=batch.end_ts,
                    summary_text=summary_text,
                    summary_json=summary_payload,
                )
                log_llm_stage(
                    self.logger,
                    "summary_store",
                    "ok",
                    job_id=job_id,
                    job_type=job_type,
                    timeout_s=request_timeout_s,
                    attempt=1,
                    action="insert",
                    summary_id=summary_id,
                )

            self.storage.mark_intervals_summarized(batch.start_ts, batch.end_ts)
            self.storage.purge_raw_data(batch.start_ts, batch.end_ts)
            self.storage.update_summary_job(
                job_id,
                status="completed",
                job_type=job_type,
                finished_at=time.time(),
                timeout_s=request_timeout_s,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=_estimate_token_count(input_chars),
            )
            if self.semantic_coalescer is not None and self.semantic_coalescer.enabled:
                try:
                    day = date.fromtimestamp(batch.start_ts)
                    self.semantic_coalescer.refresh_day(day)
                except Exception:
                    self.logger.exception("event=semantic_coalescing_failed day=%s", date.fromtimestamp(batch.start_ts).isoformat())
            self.error_notifier.resolve_many(
                "summary_generation_failure",
                "lmstudio_connection",
                "lmstudio_service_unavailable",
                "lmstudio_timeout",
            )
            log_llm_stage(
                self.logger,
                "summary_job_completed",
                "ok",
                job_id=job_id,
                job_type=job_type,
                timeout_s=request_timeout_s,
                attempt=1,
                summary_id=summary_id,
                elapsed_s=time.perf_counter() - started_at,
            )
            return summary_id
        except LLMJobCancelledError as exc:
            return None
        except LMStudioTimeoutError as exc:
            self.storage.update_summary_job(
                job_id,
                status="timed_out",
                error="LM Studio request timed out.",
                job_type=job_type,
                finished_at=time.time(),
                timeout_s=request_timeout_s,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=_estimate_token_count(input_chars),
            )
            self._unrecoverable_error = "Timeout error: LM Studio request timed out."
            self._notify_lmstudio_error("lmstudio_timeout", str(exc), key=f"{self._lmstudio_identity()}|timeout")
            log_llm_stage(
                self.logger,
                "summary_job_failed",
                "error",
                level=logging.ERROR,
                job_id=job_id,
                job_type=job_type,
                timeout_s=request_timeout_s,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=_estimate_token_count(input_chars),
                failed_stage=get_failed_stage(exc, default="http_response"),
                error_type=exc.__class__.__name__,
                error=safe_error(exc),
                exc_info=True,
            )
            return None
        except LMStudioConnectionError as exc:
            self.storage.update_summary_job(
                job_id,
                status="failed",
                error="LM Studio is unreachable.",
                job_type=job_type,
                finished_at=time.time(),
                timeout_s=request_timeout_s,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=_estimate_token_count(input_chars),
            )
            self._unrecoverable_error = "Connection error: Unable to reach LM Studio. Check that it is running."
            self._notify_lmstudio_error(
                "lmstudio_connection",
                str(exc),
                key=f"{self._lmstudio_identity()}|connection",
            )
            log_llm_stage(
                self.logger,
                "summary_job_failed",
                "error",
                level=logging.ERROR,
                job_id=job_id,
                job_type=job_type,
                timeout_s=request_timeout_s,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=_estimate_token_count(input_chars),
                failed_stage=get_failed_stage(exc, default="http_response"),
                error_type=exc.__class__.__name__,
                error=safe_error(exc),
                exc_info=True,
            )
            return None
        except LMStudioServiceUnavailableError as exc:
            self.storage.update_summary_job(
                job_id,
                status="failed",
                error="LM Studio returned an unavailable response.",
                job_type=job_type,
                finished_at=time.time(),
                timeout_s=request_timeout_s,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=_estimate_token_count(input_chars),
            )
            self._unrecoverable_error = "Service unavailable: LM Studio could not generate a response."
            self._notify_lmstudio_error(
                "lmstudio_service_unavailable",
                str(exc),
                key=f"{self._lmstudio_identity()}|unavailable",
            )
            log_llm_stage(
                self.logger,
                "summary_job_failed",
                "error",
                level=logging.ERROR,
                job_id=job_id,
                job_type=job_type,
                timeout_s=request_timeout_s,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=_estimate_token_count(input_chars),
                failed_stage=get_failed_stage(exc, default="response_parse"),
                error_type=exc.__class__.__name__,
                error=safe_error(exc),
                exc_info=True,
            )
            return None
        except Exception as exc:
            self.storage.update_summary_job(
                job_id,
                status="failed",
                error="Summary generation failed.",
                job_type=job_type,
                finished_at=time.time(),
                timeout_s=request_timeout_s,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=_estimate_token_count(input_chars),
            )
            with self._lock:
                self._unrecoverable_error = "Summary generation failed."
            self.error_notifier.notify(
                "summary_generation_failure",
                "Summary generation failed. Check that LM Studio is running and the configured model is available.",
                key=f"{self._lmstudio_identity()}|{exc.__class__.__name__}",
            )
            log_llm_stage(
                self.logger,
                "summary_job_failed",
                "error",
                level=logging.ERROR,
                job_id=job_id,
                job_type=job_type,
                timeout_s=request_timeout_s,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=_estimate_token_count(input_chars),
                failed_stage=get_failed_stage(exc, default="summary_store"),
                error_type=exc.__class__.__name__,
                error=safe_error(exc),
                exc_info=True,
            )
            return None

    def _enrich_summary_payload(
        self,
        batch: SummaryBatch,
        summary_text: str,
        summary_json: dict[str, object] | object,
        *,
        reason: str,
    ) -> dict[str, object]:
        payload: dict[str, object] = dict(summary_json) if isinstance(summary_json, dict) else {}
        segment = batch.activity_segments[0] if batch.activity_segments else None
        payload["summary_text"] = summary_text
        payload["source_batch"] = {
            "start_ts": batch.start_ts,
            "end_ts": batch.end_ts,
            "reason": reason,
        }
        if segment is not None:
            payload["source_context"] = {
                "segment_id": segment.segment_id,
                "process_name": segment.dominant_process_name,
                "window_title": segment.dominant_window_title,
                "closure_reason": segment.closure_reason,
                "blocked": segment.blocked,
            }
        else:
            payload["source_context"] = {}
        payload["activity_segments"] = [segment.to_dict() for segment in batch.activity_segments]
        return payload

    def _merge_summary_payload(
        self,
        existing_payload: dict[str, object] | object,
        *,
        batch: SummaryBatch,
        new_summary_text: str,
        new_summary_payload: dict[str, object],
        reason: str,
        similarity: float,
    ) -> dict[str, object]:
        merged: dict[str, object] = dict(existing_payload) if isinstance(existing_payload, dict) else {}
        history = list(merged.get("merge_history", [])) if isinstance(merged.get("merge_history", []), list) else []
        history.append(
            {
                "start_ts": batch.start_ts,
                "end_ts": batch.end_ts,
                "reason": reason,
                "similarity": similarity,
                "summary_text": new_summary_text,
                "source_context": new_summary_payload.get("source_context", {}),
            }
        )
        merged["merge_history"] = history
        merged["merged_count"] = int(merged.get("merged_count", 0)) + 1
        merged["last_merge"] = history[-1]
        merged["source_batch"] = new_summary_payload.get("source_batch", {})
        merged["activity_segments"] = new_summary_payload.get("activity_segments", [])
        return merged

    def _ensure_worker_count_locked(self) -> None:
        while len(self._workers) < self._max_parallel_jobs:
            index = len(self._workers) + 1
            worker_stop_event = threading.Event()
            worker = threading.Thread(
                target=self._worker_loop,
                args=(worker_stop_event,),
                name=f"SummaryWorker-{index}",
                daemon=True,
            )
            self._workers.append(_WorkerHandle(thread=worker, stop_event=worker_stop_event))
            worker.start()

    def wait_for_idle(self, timeout_seconds: float) -> bool:
        deadline = time.time() + max(0.0, timeout_seconds)
        with self._condition:
            while self._queue or self._running_jobs:
                if self._shutdown_event.is_set():
                    return False
                remaining = deadline - time.time()
                if remaining <= 0:
                    return False
                self._condition.wait(timeout=remaining)
            return True

    def _notify_lmstudio_error(self, category: str, message: str, *, key: str) -> None:
        with self._lock:
            self._unrecoverable_error = message
        self.error_notifier.notify(category, message, key=key)

    def _lmstudio_identity(self) -> str:
        base_url = getattr(self.lm_client, "base_url", "lmstudio")
        model = getattr(self.lm_client, "model", "unknown-model")
        return f"{base_url}|{model}"

    def _lmstudio_timeout_seconds(self) -> int:
        timeout_seconds = getattr(self.lm_client, "timeout_seconds", 600)
        try:
            return max(1, int(timeout_seconds))
        except (TypeError, ValueError):
            return 600

    def _lmstudio_daily_timeout_seconds(self) -> int:
        daily_timeout_seconds = getattr(self.lm_client, "daily_timeout_seconds", None)
        if daily_timeout_seconds is not None:
            try:
                return max(1, int(daily_timeout_seconds))
            except (TypeError, ValueError):
                pass
        timeout_seconds = self._lmstudio_timeout_seconds()
        return max(timeout_seconds, timeout_seconds * 2)

    def _get_lmstudio_job_queue(self):
        return getattr(self.lm_client, "job_queue", None)

    def _get_lmstudio_queue_snapshot(self) -> dict[str, int | bool]:
        job_queue = self._get_lmstudio_job_queue()
        if job_queue is not None:
            return job_queue.snapshot()
        with self._lock:
            queued_jobs = len(self._queue)
            running_jobs = len(self._running_jobs)
            max_parallel_jobs = self._max_parallel_jobs
            accepting_jobs = not (self._stop_event.is_set() or self._shutdown_event.is_set())
            closing = not accepting_jobs
        return {
            "queued_jobs": queued_jobs,
            "running_jobs": running_jobs,
            "pending_jobs": queued_jobs + running_jobs,
            "max_concurrent_jobs": max_parallel_jobs,
            "accepting_jobs": accepting_jobs,
            "closing": closing,
            "closed": closing,
            "stopped": closing,
        }


def _estimate_token_count(chars: int) -> int | None:
    if chars <= 0:
        return None
    return max(1, (chars + 3) // 4)
