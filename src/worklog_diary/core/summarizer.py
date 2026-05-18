from __future__ import annotations

import logging
import re
import threading
import time
from collections import deque
from dataclasses import asdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from .batching import BatchBuilder, SummaryBatch
from .error_notifications import ErrorNotificationManager
from .errors import LMStudioConnectionError, LMStudioServiceUnavailableError, LMStudioTimeoutError
from .evidence_quality import score_daily_evidence_quality, score_event_evidence_quality
from .lmstudio_client import LMStudioClient
from .lmstudio_logging import get_failed_stage, llm_job_context, log_llm_stage, safe_error, set_failed_stage
from .llm_job_queue import LLMJobCancelledError, LLMJobMetadata
from .internal_artifacts import is_internal_artifact_path
from .storage import SQLiteStorage
from .task_evidence_extraction import extract_task_evidence
from .task_clustering import cluster_tasks_for_day
from .task_daily_summary import build_daily_summary_from_task_clusters
from .summary_dedup import SummaryDeduplicator
from .semantic_coalescing import SemanticCoalescer
from .models import SummaryRecord


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
    _EVENT_SUMMARY_PRIORITIES = {
        "manual": 10,
        "summary-window": 20,
        "lock": 30,
        "scheduled": 40,
    }
    _DAILY_RECAP_PRIORITIES = {
        "manual": 80,
        "startup_backfill": 220,
        "auto_backfill": 220,
    }

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
        app_data_dir: str | None = None,
    ) -> None:
        self.storage = storage
        self.batch_builder = batch_builder
        self.lm_client = lm_client
        self.app_data_dir = app_data_dir
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
        self._daily_recap_waiting = 0
        self._daily_recap_running = 0
        self._lmstudio_state = "ok"
        self._lmstudio_last_error_category: str | None = None
        self._lmstudio_last_error_message: str | None = None

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
        deadline = time.monotonic() + 10.0
        for handle in self._workers + self._retired_workers:
            while handle.thread.is_alive() and time.monotonic() < deadline:
                remaining = max(0.0, deadline - time.monotonic())
                handle.thread.join(timeout=min(0.25, remaining))
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

    def reconfigure(self, *, summary_deduplicator: SummaryDeduplicator) -> None:
        self.summary_deduplicator = summary_deduplicator

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
                priority = self._event_summary_priority(reason)
                job_id = self.storage.create_summary_job(
                    batch.start_ts,
                    batch.end_ts,
                    status="queued",
                    job_type="event_summary",
                    timeout_s=timeout_s,
                    attempt=1,
                    input_chars=input_chars,
                    input_token_estimate=_estimate_token_count(input_chars),
                    priority=priority,
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
            daily_recap_waiting = self._daily_recap_waiting
            daily_recap_running = self._daily_recap_running
            lmstudio_state = self._lmstudio_state
            lmstudio_last_error_category = self._lmstudio_last_error_category
            lmstudio_last_error_message = self._lmstudio_last_error_message

        try:
            persisted_counts = self.storage.get_summary_job_status_counts()
        except Exception:
            persisted_counts = {}
        llm_queue = self._get_lmstudio_queue_snapshot()
        queued_by_type = llm_queue.get("queued_by_type", {})
        running_by_type = llm_queue.get("running_by_type", {})
        day_summary_queued = int(queued_by_type.get("day_summary", 0)) if isinstance(queued_by_type, dict) else 0
        day_summary_running = int(running_by_type.get("day_summary", 0)) if isinstance(running_by_type, dict) else 0
        event_summary_queued = int(queued_by_type.get("event_summary", 0)) if isinstance(queued_by_type, dict) else 0
        event_summary_running = int(running_by_type.get("event_summary", 0)) if isinstance(running_by_type, dict) else 0
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
            "daily_recap_waiting_jobs": daily_recap_waiting,
            "daily_recap_running_jobs": daily_recap_running,
            "daily_recap_inflight_jobs": daily_recap_waiting + daily_recap_running,
            "llm_queue_queued_day_summary_jobs": day_summary_queued,
            "llm_queue_running_day_summary_jobs": day_summary_running,
            "llm_queue_queued_event_summary_jobs": event_summary_queued,
            "llm_queue_running_event_summary_jobs": event_summary_running,
            "lmstudio_state": lmstudio_state,
            "lmstudio_last_error_category": lmstudio_last_error_category,
            "lmstudio_last_error_message": lmstudio_last_error_message,
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
        priority = self._event_summary_priority(reason)
        job_id = self.storage.create_summary_job(
            batch.start_ts,
            batch.end_ts,
            status="queued",
            job_type="event_summary",
            timeout_s=timeout_s,
            attempt=1,
            input_chars=input_chars,
            input_token_estimate=_estimate_token_count(input_chars),
            priority=priority,
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
        return self._run_summary_job(job_id=job_id, batch=batch, reason=reason, priority=priority)

    def generate_daily_recap_for_day(self, day: date, *, reason: str = "manual") -> tuple[int, bool]:
        if self._shutdown_event.is_set() or self._stop_event.is_set() or not self._accepting_new_jobs:
            raise LLMJobCancelledError("Daily summary generation skipped during shutdown")
        day_key = day.isoformat()
        priority = self._daily_recap_priority(reason)
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
                priority=priority,
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
                priority=priority,
            )

            daily_started = False

            def _on_started(metadata: LLMJobMetadata) -> None:
                nonlocal daily_started
                daily_started = True
                with self._lock:
                    if self._daily_recap_waiting > 0:
                        self._daily_recap_waiting -= 1
                    self._daily_recap_running += 1
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
                with self._lock:
                    self._daily_recap_waiting += 1
                with self._condition:
                    while not self._can_start_summary_job_now_locked(reason=reason):
                        if self._shutdown_event.is_set() or self._stop_event.is_set() or not self._accepting_new_jobs:
                            raise LLMJobCancelledError("Daily summary generation cancelled during shutdown")
                        self._log_admission_paused_once_locked()
                        self._condition.wait(timeout=0.5)
                with llm_job_context(
                    f"daily_recap:{day.isoformat()}",
                    job_type="day_summary",
                    timeout_s=timeout_s,
                    attempt=1,
                    input_chars=input_chars,
                    input_token_estimate=_estimate_token_count(input_chars),
                ):
                    _on_started(
                        LLMJobMetadata(
                            job_id=job_id,
                            job_type="day_summary",
                            attempt=1,
                            queued_at=time.time(),
                            started_at=time.time(),
                            finished_at=None,
                            timeout_s=timeout_s,
                            input_chars=input_chars,
                            input_token_estimate=_estimate_token_count(input_chars),
                            priority=priority,
                        )
                    )
                    cluster_tasks_for_day(day, storage=self.storage)
                    clusters = self.storage.list_task_clusters_for_day(day)
                    ignored_noise = self.storage.count_low_value_noise_reasons_for_day(day)
                    daily = build_daily_summary_from_task_clusters(day, clusters, summaries, ignored_noise=ignored_noise)
                    self.logger.info(
                        "event=task_daily_generation day=%s source_summary_count=%s low_value_summary_count=%s task_cluster_count=%s task_titles=%s ignored_noise=%s generated_from_task_clusters=%s fallback_reason=%s",
                        day.isoformat(),
                        len(summaries),
                        sum(int(item.get("count", 0)) for item in ignored_noise),
                        len(clusters),
                        [str(c.get("title")) for c in clusters],
                        {str(item.get("reason")): int(item.get("count", 0)) for item in ignored_noise},
                        bool(daily.generated_from_task_clusters),
                        daily.payload.get("fallback_reason") if isinstance(daily.payload, dict) else None,
                    )
                    recap_text, recap_json = daily.summary_text, daily.payload
            except LLMJobCancelledError as exc:
                self.storage.update_summary_job(
                    job_id,
                    status="cancelled",
                    error=str(exc),
                    job_type="day_summary",
                    finished_at=time.time(),
                    timeout_s=timeout_s,
                    attempt=1,
                    input_chars=input_chars,
                    input_token_estimate=_estimate_token_count(input_chars),
                )
                raise
            except LMStudioTimeoutError as exc:
                self._set_lmstudio_state("degraded", "timeout", "LM Studio timed out while generating a daily recap.")
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
                self._set_lmstudio_state("offline", "connection", "LM Studio is unreachable.")
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
                self._set_lmstudio_state("degraded", "service_unavailable", "LM Studio could not generate a daily recap.")
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
            except Exception as exc:
                self._set_lmstudio_state("degraded", "unexpected_error", "LM Studio daily recap generation failed.")
                self.storage.update_summary_job(
                    job_id,
                    status="failed",
                    error=str(exc) or "Daily summary generation failed.",
                    job_type="day_summary",
                    finished_at=time.time(),
                    timeout_s=timeout_s,
                    attempt=1,
                    input_chars=input_chars,
                    input_token_estimate=_estimate_token_count(input_chars),
                    priority=priority,
                )
                raise
            else:
                self._clear_lmstudio_state()
                self.error_notifier.resolve_many("lmstudio_connection", "lmstudio_service_unavailable", "lmstudio_timeout")
            finally:
                with self._lock:
                    if daily_started:
                        if self._daily_recap_running > 0:
                            self._daily_recap_running -= 1
                    elif self._daily_recap_waiting > 0:
                        self._daily_recap_waiting -= 1

            source_reports = [
                score_event_evidence_quality(
                    summary_id=summary.id,
                    day=day,
                    start_ts=summary.start_ts,
                    end_ts=summary.end_ts,
                    summary_json=summary.summary_json if isinstance(summary.summary_json, dict) else {},
                    activity_entities=summary.summary_json.get("activity_entities") if isinstance(summary.summary_json, dict) else None,
                    parser_coverage=summary.summary_json.get("parser_coverage") if isinstance(summary.summary_json, dict) else None,
                    source_batch=summary.summary_json.get("source_batch") if isinstance(summary.summary_json, dict) else None,
                    source_context=summary.summary_json.get("source_context") if isinstance(summary.summary_json, dict) else None,
                )
                for summary in summaries
            ]
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
                    structured_payload_json=recap_json if isinstance(recap_json, dict) else None,
                    generated_from_task_clusters=bool(isinstance(recap_json, dict) and recap_json.get("generated_from_task_clusters", False)),
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
            daily_quality = score_daily_evidence_quality(
                summary_id=daily_summary.id,
                day=day,
                start_ts=day_start.timestamp(),
                end_ts=day_end.timestamp(),
                recap_json=recap_json if isinstance(recap_json, dict) else None,
                source_event_reports=source_reports,
            )
            if isinstance(recap_json, dict):
                recap_json = dict(recap_json)
                recap_json["evidence_quality_report"] = daily_quality.to_dict()
                self.storage.update_daily_summary_record(
                    day,
                    recap_json=recap_json,
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
        manual_bypass_lock_gate = trigger in {"manual", "summary-window"}
        self.logger.info(
            "event=summary_admission_decision allowed=%s reason=%s lock_state=%s trigger=%s request_reason=%s admission_trigger=%s manual_bypass_lock_gate=%s process_backlog_only_while_locked=%s",
            allowed,
            reason,
            self._lock_state_label(),
            trigger,
            trigger,
            trigger,
            manual_bypass_lock_gate,
            self._process_backlog_only_while_locked,
        )

    def reconcile_missing_daily_summaries(
        self,
        *,
        reason: str = "startup_backfill",
        min_age_hours: float = 2.0,
        max_days: int = 60,
        enabled: bool = True,
    ) -> dict[str, int | str | bool]:
        lock_state = self._lock_state_label()
        now = datetime.now()
        if not enabled:
            self.logger.info(
                "event=daily_summary_backfill_reconcile_started enabled=false request_reason=%s lock_state=%s min_age_hours=%s max_days=%s now_local=%s",
                reason,
                lock_state,
                float(min_age_hours),
                int(max_days),
                now.isoformat(),
            )
            self.logger.info("event=daily_summary_backfill_noop reason=disabled")
            return {"enabled": False, "scanned_days": 0, "missing_days": 0, "enqueued": 0}
        today = now.date()
        cutoff_day = (now - timedelta(hours=max(0.0, min_age_hours))).date()
        candidate_days = [
            day for day in self.storage.list_summary_days(limit=max(1, int(max_days)))
            if day < today and day < cutoff_day
        ]
        self.logger.info(
            "event=daily_summary_backfill_reconcile_started enabled=true request_reason=%s lock_state=%s min_age_hours=%s max_days=%s now_local=%s scanned_days=%s cutoff_day=%s",
            reason,
            lock_state,
            float(min_age_hours),
            int(max_days),
            now.isoformat(),
            len(candidate_days),
            cutoff_day.isoformat(),
        )
        enqueued = 0
        missing = 0
        use_coalesced = bool(self.semantic_coalescer and self.semantic_coalescer.enabled)
        for day in sorted(candidate_days):
            if self._shutdown_event.is_set() or self._stop_event.is_set() or not self._accepting_new_jobs:
                self.logger.info("event=daily_summary_backfill_noop reason=shutdown")
                break
            defer_reason = self._daily_recap_defer_reason(reason=reason)
            if defer_reason is not None:
                self.logger.info(
                    "event=daily_summary_backfill_deferred reason=%s target_day=%s",
                    defer_reason,
                    day.isoformat(),
                )
                break
            if self.storage.get_daily_summary_for_day(day) is not None:
                self.logger.info(
                    "event=daily_summary_candidate_evaluated day=%s eligible=false reason=already_has_daily_summary blocked_by_lock_gate=false request_reason=%s",
                    day.isoformat(),
                    reason,
                )
                continue
            existing_job = self.storage.get_daily_summary_job_for_day(day)
            if existing_job is not None and str(existing_job["status"]) in {"queued", "running"}:
                self.logger.info(
                    "event=daily_summary_candidate_evaluated day=%s eligible=false reason=already_queued_or_running existing_status=%s blocked_by_lock_gate=false request_reason=%s",
                    day.isoformat(),
                    str(existing_job["status"]),
                    reason,
                )
                continue
            summaries = self.storage.list_effective_summaries_for_day(day, use_coalesced=use_coalesced)
            if not summaries:
                self.logger.info(
                    "event=daily_summary_candidate_evaluated day=%s eligible=false reason=no_event_summaries has_event_summaries=false event_summary_count=0 blocked_by_lock_gate=false request_reason=%s",
                    day.isoformat(),
                    reason,
                )
                continue
            self.logger.info(
                "event=daily_summary_candidate_evaluated day=%s eligible=true reason=ready has_event_summaries=true event_summary_count=%s blocked_by_lock_gate=false request_reason=%s",
                day.isoformat(),
                len(summaries),
                reason,
            )
            missing += 1
            try:
                summary_id, replaced = self.generate_daily_recap_for_day(day, reason=reason)
            except LLMJobCancelledError:
                self.logger.info("event=daily_summary_backfill_noop reason=queue_stopped target_day=%s", day.isoformat())
                break
            enqueued += 1
            job = self.storage.get_daily_summary_job_for_day(day)
            self.logger.info(
                "event=daily_summary_backfill_job_enqueued target_day=%s job_id=%s reason=%s daily_summary_id=%s replaced=%s",
                day.isoformat(),
                int(job["id"]) if job is not None else 0,
                reason,
                summary_id,
                replaced,
            )
        self.logger.info(
            "event=daily_summary_backfill_scan_completed scanned_days=%s missing_days=%s enqueued=%s",
            len(candidate_days),
            missing,
            enqueued,
        )
        if enqueued == 0:
            self.logger.info("event=daily_summary_backfill_noop scanned_days=%s", len(candidate_days))
        return {"enabled": True, "scanned_days": len(candidate_days), "missing_days": missing, "enqueued": enqueued, "reason": reason}

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
        priority: int | None = None,
    ) -> int | None:
        started_at = time.perf_counter()
        input_chars = sum(len(segment.text) for segment in batch.text_segments)
        request_timeout_s = timeout_s if timeout_s is not None else self._lmstudio_timeout_seconds()
        request_priority = priority if priority is not None else self._event_summary_priority(reason)
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
                    priority=request_priority,
                )
            self._clear_lmstudio_state()
            log_llm_stage(
                self.logger,
                "summary_postprocess",
                "start",
                job_id=job_id,
                job_type=job_type,
                timeout_s=request_timeout_s,
                attempt=1,
            )
            try:
                summary_payload = self._enrich_summary_payload(batch, summary_text, summary_json, reason=reason)
                recent_summaries = self.storage.list_summaries(limit=self.summary_deduplicator.recent_compare_count)
                dedup_decision = self.summary_deduplicator.evaluate(
                    batch=batch,
                    summary_text=summary_text,
                    summary_json=summary_payload,
                    recent_summaries=recent_summaries,
                )
            except Exception as exc:
                log_llm_stage(
                    self.logger,
                    "summary_postprocess",
                    "error",
                    level=logging.ERROR,
                    job_id=job_id,
                    job_type=job_type,
                    timeout_s=request_timeout_s,
                    attempt=1,
                    error_type=exc.__class__.__name__,
                    error=safe_error(exc),
                    exc_info=True,
                )
                raise set_failed_stage(exc, "summary_postprocess")
            log_llm_stage(
                self.logger,
                "summary_postprocess",
                "ok",
                job_id=job_id,
                job_type=job_type,
                timeout_s=request_timeout_s,
                attempt=1,
                dedup_action=dedup_decision.action,
            )

            summary_id: int | None = None
            persisted_summary_payload: dict[str, object] | None = None
            try:
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
                        persisted_summary_payload = merged_payload
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
                        persisted_summary_payload = summary_payload
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
                    persisted_summary_payload = summary_payload
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

                activity_entity_count = self._persist_activity_entities_for_batch(batch, summary_id=summary_id)
                if activity_entity_count:
                    self.logger.info(
                        "event=activity_entities_persisted day=%s summary_id=%s entity_count=%s",
                        date.fromtimestamp(batch.start_ts).isoformat(),
                        "none" if summary_id is None else int(summary_id),
                        activity_entity_count,
                    )

                if summary_id is not None and persisted_summary_payload is not None:
                    self._persist_structured_task_evidence(
                        summary_id=summary_id,
                        start_ts=batch.start_ts,
                        end_ts=batch.end_ts,
                        summary_text=summary_text,
                        summary_json=persisted_summary_payload,
                        job_id=job_id,
                    )
                    persisted_entities = [
                        asdict(item)
                        for item in self.storage.list_activity_entities_for_summary(summary_id)
                    ]
                    if not persisted_entities:
                        persisted_entities = [asdict(item) for item in batch.activity_entities]
                    quality_report = score_event_evidence_quality(
                        summary_id=summary_id,
                        day=date.fromtimestamp(batch.start_ts),
                        start_ts=batch.start_ts,
                        end_ts=batch.end_ts,
                        summary_json=persisted_summary_payload,
                        activity_entities=persisted_entities,
                        parser_coverage=batch.parser_coverage,
                        source_batch=persisted_summary_payload.get("source_batch") if isinstance(persisted_summary_payload.get("source_batch"), dict) else None,
                        source_context=persisted_summary_payload.get("source_context") if isinstance(persisted_summary_payload.get("source_context"), dict) else None,
                    )
                    quality_payload = dict(persisted_summary_payload)
                    quality_payload["evidence_quality_report"] = quality_report.to_dict()
                    self.storage.update_summary_record(summary_id, summary_json=quality_payload)
                    persisted_summary_payload = quality_payload

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
            except Exception as exc:
                log_llm_stage(
                    self.logger,
                    "summary_store",
                    "error",
                    level=logging.ERROR,
                    job_id=job_id,
                    job_type=job_type,
                    timeout_s=request_timeout_s,
                    attempt=1,
                    summary_id=summary_id,
                    error_type=exc.__class__.__name__,
                    error=safe_error(exc),
                    exc_info=True,
                )
                raise set_failed_stage(exc, "summary_store")
            if self.semantic_coalescer is not None and self.semantic_coalescer.enabled:
                try:
                    day = date.fromtimestamp(batch.start_ts)
                    self.semantic_coalescer.refresh_day(day)
                except Exception:
                    self.logger.exception("event=semantic_coalescing_failed day=%s", date.fromtimestamp(batch.start_ts).isoformat())
            self.error_notifier.resolve_many(
                "summary_generation_failure",
                "summary_postprocess_error",
                "summary_store_error",
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
            self._set_lmstudio_state("degraded", "llm_timeout", "LM Studio timed out while generating an event summary.")
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
                error_category="llm_timeout",
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
            self._set_lmstudio_state("offline", "llm_transport_error", "LM Studio is unreachable.")
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
                error_category="llm_transport_error",
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
            self._set_lmstudio_state(
                "degraded",
                (
                    "llm_response_parse_error"
                    if get_failed_stage(exc, default="response_parse") == "response_parse"
                    else "llm_transport_error"
                ),
                "LM Studio returned an unavailable response.",
            )
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
                error_category=(
                    "llm_response_parse_error"
                    if get_failed_stage(exc, default="response_parse") == "response_parse"
                    else "llm_transport_error"
                ),
                error_type=exc.__class__.__name__,
                error=safe_error(exc),
                exc_info=True,
            )
            return None
        except Exception as exc:
            failed_stage = get_failed_stage(exc, default="summary_store")
            error_category = "summary_store_error"
            failure_message = "Summary store failed."
            notification_category = "summary_store_error"
            notification_message = "Summary store failed after a valid LM Studio response."
            if failed_stage == "summary_postprocess":
                error_category = "summary_postprocess_error"
                failure_message = "Summary post-processing failed."
                notification_category = "summary_postprocess_error"
                notification_message = "Summary post-processing failed after a valid LM Studio response."
            elif failed_stage not in {"summary_postprocess", "summary_store"}:
                error_category = "summary_store_error"
                failure_message = "Summary generation failed."
                notification_category = "summary_generation_failure"
                notification_message = (
                    "Summary generation failed after the LM Studio response. Check local post-processing and storage logs."
                )
            self.storage.update_summary_job(
                job_id,
                status="failed",
                error=failure_message,
                job_type=job_type,
                finished_at=time.time(),
                timeout_s=request_timeout_s,
                attempt=1,
                input_chars=input_chars,
                input_token_estimate=_estimate_token_count(input_chars),
            )
            with self._lock:
                self._unrecoverable_error = failure_message
            self._set_lmstudio_state("degraded", error_category, failure_message)
            self.error_notifier.notify(
                notification_category,
                notification_message,
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
                failed_stage=failed_stage,
                error_category=error_category,
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
        payload["activity_entities"] = [
            {
                "entity_type": item.entity_type,
                "entity_value": item.entity_value,
                "entity_normalized": item.entity_normalized,
                "source_kind": item.source_kind,
                "source_ref": item.source_ref,
                "evidence_kind": item.evidence_kind,
                "confidence": item.confidence,
                "attributes": item.attributes,
            }
            for item in batch.activity_entities
        ]
        payload["parser_coverage"] = [dict(item) for item in batch.parser_coverage]
        payload["source_batch"]["active_interval_count"] = len(batch.active_intervals)
        payload["source_batch"]["blocked_interval_count"] = len(batch.blocked_intervals)
        payload["source_batch"]["text_segment_count"] = len(batch.text_segments)
        payload["source_batch"]["screenshot_count"] = len(batch.screenshots)
        payload["source_batch"]["activity_entity_count"] = len(batch.activity_entities)
        payload["source_batch"]["parser_coverage_count"] = len(batch.parser_coverage)
        _augment_privacy_limited_event_evidence(payload, batch)
        _filter_internal_artifacts_from_summary_payload(payload, app_data_dir=self.app_data_dir)
        return payload

    def _persist_activity_entities_for_batch(
        self,
        batch: SummaryBatch,
        *,
        summary_id: int | None,
    ) -> int:
        if not batch.activity_entities:
            return 0

        day = date.fromtimestamp(batch.start_ts)
        inserted_ids = self.storage.add_activity_entities(
            day=day,
            start_ts=batch.start_ts,
            end_ts=batch.end_ts,
            summary_id=summary_id,
            entities=batch.activity_entities,
        )
        return len(inserted_ids)

    def _persist_structured_task_evidence(
        self,
        *,
        summary_id: int,
        start_ts: float,
        end_ts: float,
        summary_text: str,
        summary_json: dict[str, object],
        job_id: int,
    ) -> None:
        record = SummaryRecord(
            id=summary_id,
            job_id=job_id,
            start_ts=start_ts,
            end_ts=end_ts,
            summary_text=summary_text,
            summary_json=summary_json,
            created_ts=time.time(),
        )
        evidence = extract_task_evidence(record)
        self.storage.update_event_summary_structured_fields(
            summary_id,
            structured_payload_json=evidence.payload,
            primary_task_label=evidence.primary_task_label,
            primary_activity_type=evidence.primary_activity_type,
            is_blocked=evidence.is_blocked,
            is_low_value=evidence.is_low_value,
            noise_reason=evidence.noise_reason,
            confidence=evidence.confidence,
        )
        self.storage.replace_activity_entities_for_summary(
            day=date.fromtimestamp(start_ts),
            start_ts=start_ts,
            end_ts=end_ts,
            summary_id=summary_id,
            entities=evidence.entities,
        )

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

    def _event_summary_priority(self, reason: str) -> int:
        return int(self._EVENT_SUMMARY_PRIORITIES.get(reason, 50))

    def _daily_recap_priority(self, reason: str) -> int:
        return int(self._DAILY_RECAP_PRIORITIES.get(reason, 180))

    def _set_lmstudio_state(self, state: str, category: str, message: str) -> None:
        with self._lock:
            self._lmstudio_state = state
            self._lmstudio_last_error_category = category
            self._lmstudio_last_error_message = message

    def _clear_lmstudio_state(self) -> None:
        with self._lock:
            self._lmstudio_state = "ok"
            self._lmstudio_last_error_category = None
            self._lmstudio_last_error_message = None

    def _daily_recap_defer_reason(self, *, reason: str) -> str | None:
        if reason == "manual":
            return None
        runtime = self.get_runtime_status()
        if bool(runtime["has_unrecoverable_error"]):
            return "summary_error"
        lmstudio_state = str(runtime["lmstudio_state"])
        if lmstudio_state != "ok":
            return f"lmstudio_{lmstudio_state}"
        if bool(runtime["process_backlog_only_while_locked"]) and runtime["session_locked"] is False:
            return "pc_unlocked"
        if int(runtime["queued_jobs"]) > 0 or int(runtime["running_jobs"]) > 0:
            return "event_summary_inflight"
        try:
            pending = self.storage.get_pending_counts()
        except Exception:
            pending = {
                "intervals": 0,
                "key_events": 0,
                "processed_key_events": 0,
                "text_segments": 0,
                "screenshots": 0,
            }
        if (
            int(pending["intervals"]) > 0
            or int(pending["key_events"]) > 0
            or int(pending["processed_key_events"]) > 0
            or int(pending["text_segments"]) > 0
            or int(pending["screenshots"]) > 0
        ):
            return "event_backlog"
        return None

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
            "queued_by_type": {},
            "running_by_type": {},
        }


def _estimate_token_count(chars: int) -> int | None:
    if chars <= 0:
        return None
    return max(1, (chars + 3) // 4)


_BROWSER_SUFFIXES = (
    " - Google Chrome",
    " - Chromium",
    " - Microsoft Edge",
    " - Edge",
)
_MEETING_TOKENS = ("teams", "webex", "zoom", "meet", "slack")
_FILE_LIKE_RE = re.compile(r"\b([A-Za-z0-9][A-Za-z0-9._-]{0,127}\.[A-Za-z0-9]{1,10})\b")
_PDF_RE = re.compile(r"\.pdf$", flags=re.IGNORECASE)
_DOC_EXT_RE = re.compile(r"\.(doc|docx|ppt|pptx|xls|xlsx|txt|md|rtf)$", flags=re.IGNORECASE)
_PRIVACY_CAVEAT = "Interpretation uses blocked-app metadata (process/window title) only; text and screenshot content were not captured."


def _augment_privacy_limited_event_evidence(payload: dict[str, object], batch: SummaryBatch) -> None:
    existing_blocked_refs = _coerce_payload_list(payload.get("blocked_observed_references"))
    inferred_blocked_refs = _infer_blocked_observed_references(batch)
    blocked_observed_references = _merge_reference_lists(existing_blocked_refs, inferred_blocked_refs)
    payload["blocked_observed_references"] = blocked_observed_references

    files_and_documents = _coerce_payload_list(payload.get("files_and_documents") or payload.get("files"))
    files_and_documents = _augment_files_and_documents_with_blocked_refs(files_and_documents, blocked_observed_references)
    payload["files_and_documents"] = files_and_documents

    conversations_or_references = _coerce_payload_list(
        payload.get("conversations_or_references") or payload.get("conversations")
    )
    conversations_or_references = _augment_conversations_with_blocked_refs(
        conversations_or_references,
        blocked_observed_references,
    )
    payload["conversations_or_references"] = conversations_or_references

    payload["jira_update_candidates"] = _coerce_payload_list(payload.get("jira_update_candidates"))
    unknowns_and_privacy_limits = _coerce_payload_list(
        payload.get("unknowns_and_privacy_limits") or payload.get("unknowns")
    )
    if blocked_observed_references and not any(
        isinstance(item, str) and "blocked" in item.lower() for item in unknowns_and_privacy_limits
    ):
        unknowns_and_privacy_limits.append(_PRIVACY_CAVEAT)
    payload["unknowns_and_privacy_limits"] = unknowns_and_privacy_limits


def _filter_internal_artifacts_from_summary_payload(payload: dict[str, object], *, app_data_dir: str | None) -> int:
    filtered = 0
    for key in ("files_and_documents", "files", "task_candidates", "outcomes", "jira_update_candidates", "conversations_or_references", "conversations"):
        values = _coerce_payload_list(payload.get(key))
        kept: list[str] = []
        for item in values:
            if is_internal_artifact_path(str(item), app_data_dir=app_data_dir):
                filtered += 1
                continue
            kept.append(item)
        payload[key] = kept
    if filtered:
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        metadata["internal_artifacts_filtered"] = int(metadata.get("internal_artifacts_filtered", 0)) + filtered
        payload["metadata"] = metadata
    return filtered


def _infer_blocked_observed_references(batch: SummaryBatch) -> list[dict[str, object]]:
    inferred: list[dict[str, object]] = []
    for interval in batch.blocked_intervals:
        process = str(interval.process_name or "").strip()
        title = str(interval.window_title or "").strip()
        if not process and not title:
            continue

        inferred_type = "unknown"
        extracted_entities: list[str] = []
        safe_interpretation = "Observed blocked application metadata only; content is unknown."
        confidence = 0.45

        normalized_process = process.lower()
        normalized_title = title.lower()
        browser_title = _strip_browser_suffix(title)
        file_hint = _extract_file_hint(browser_title)

        if file_hint and _PDF_RE.search(file_hint):
            inferred_type = "pdf"
            extracted_entities = [file_hint]
            safe_interpretation = f"Viewed or referenced `{file_hint}` in a blocked browser window; content was not captured."
            confidence = 0.9
        elif file_hint and _DOC_EXT_RE.search(file_hint):
            inferred_type = "document"
            extracted_entities = [file_hint]
            safe_interpretation = f"Viewed or referenced document `{file_hint}` in a blocked window; content was not captured."
            confidence = 0.84
        elif "outlook" in normalized_process or "outlook" in normalized_title:
            inferred_type = "mail_subject"
            extracted_entities = [title]
            safe_interpretation = f"Observed mail subject/title `{title}` in blocked Outlook; message content was not captured."
            confidence = 0.8
        elif any(token in normalized_process or token in normalized_title for token in _MEETING_TOKENS):
            inferred_type = "meeting_title"
            extracted_entities = [title]
            safe_interpretation = f"Observed meeting or chat title `{title}` in a blocked app; conversation content was not captured."
            confidence = 0.78
        elif _looks_like_browser_process_or_title(normalized_process, normalized_title) and browser_title:
            inferred_type = "web_page"
            extracted_entities = [browser_title]
            safe_interpretation = f"Observed browser page title `{browser_title}` in blocked browsing activity; page content was not captured."
            confidence = 0.72

        inferred.append(
            {
                "process": process,
                "window_title": title,
                "inferred_reference_type": inferred_type,
                "extracted_entities": extracted_entities,
                "content_captured": False,
                "metadata_used": True,
                "safe_interpretation": safe_interpretation,
                "confidence": confidence,
                "caveat": _PRIVACY_CAVEAT,
            }
        )
    return inferred


def _augment_files_and_documents_with_blocked_refs(
    current: list[object],
    blocked_observed_references: list[object],
) -> list[object]:
    items = list(current)
    seen = {
        (_normalize_for_key(entry.get("path/name")), _normalize_for_key(entry.get("status")))
        for entry in items
        if isinstance(entry, dict)
    }
    for ref in blocked_observed_references:
        if not isinstance(ref, dict):
            continue
        ref_type = str(ref.get("inferred_reference_type") or "").strip().lower()
        if ref_type not in {"pdf", "document"}:
            continue
        extracted = ref.get("extracted_entities")
        entity_name = ""
        if isinstance(extracted, list):
            for item in extracted:
                text = str(item).strip()
                if text:
                    entity_name = text
                    break
        if not entity_name:
            entity_name = str(ref.get("window_title") or "").strip()
        key = (_normalize_for_key(entity_name), "read_or_viewed")
        if not entity_name or key in seen:
            continue
        seen.add(key)
        items.append(
            {
                "path/name": entity_name,
                "status": "read_or_viewed",
                "source": "blocked_window_title_metadata",
                "privacy_limited": True,
                "confidence": ref.get("confidence", 0.7),
            }
        )
    return items


def _augment_conversations_with_blocked_refs(
    current: list[object],
    blocked_observed_references: list[object],
) -> list[object]:
    items = list(current)
    seen = {_normalize_for_key(entry.get("reference")) for entry in items if isinstance(entry, dict)}
    for ref in blocked_observed_references:
        if not isinstance(ref, dict):
            continue
        ref_type = str(ref.get("inferred_reference_type") or "").strip().lower()
        if ref_type not in {"mail_subject", "meeting_title", "web_page"}:
            continue
        window_title = str(ref.get("window_title") or "").strip()
        if not window_title:
            continue
        normalized = _normalize_for_key(window_title)
        if normalized in seen:
            continue
        seen.add(normalized)
        items.append(
            {
                "reference": window_title,
                "type": ref_type,
                "privacy_limited": True,
                "content_captured": False,
                "confidence": ref.get("confidence", 0.65),
                "caveat": _PRIVACY_CAVEAT,
            }
        )
    return items


def _merge_reference_lists(primary: list[object], secondary: list[object]) -> list[object]:
    merged: list[object] = list(primary)
    seen: set[tuple[str, str]] = set()
    for item in merged:
        if not isinstance(item, dict):
            continue
        seen.add(
            (
                _normalize_for_key(item.get("process")),
                _normalize_for_key(item.get("window_title")),
            )
        )
    for item in secondary:
        if not isinstance(item, dict):
            continue
        key = (
            _normalize_for_key(item.get("process")),
            _normalize_for_key(item.get("window_title")),
        )
        if key in seen:
            continue
        seen.add(key)
        merged.append(item)
    return merged


def _coerce_payload_list(value: object) -> list[object]:
    if isinstance(value, list):
        return list(value)
    if value is None:
        return []
    return [value]


def _normalize_for_key(value: object) -> str:
    return str(value or "").strip().lower()


def _looks_like_browser_process_or_title(process_name: str, window_title: str) -> bool:
    return any(token in process_name or token in window_title for token in ("chrome", "edge", "chromium", "browser"))


def _strip_browser_suffix(title: str) -> str:
    stripped = title.strip()
    for suffix in _BROWSER_SUFFIXES:
        if stripped.endswith(suffix):
            return stripped[: -len(suffix)].strip()
    return stripped


def _extract_file_hint(text: str) -> str:
    for match in _FILE_LIKE_RE.finditer(text):
        value = str(match.group(1)).strip()
        if value:
            return value
    return ""
