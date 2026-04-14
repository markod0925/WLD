from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import date

from .batching import BatchBuilder, SummaryBatch
from .error_notifications import ErrorNotificationManager
from .errors import LMStudioConnectionError, LMStudioServiceUnavailableError
from .lmstudio_client import LMStudioClient
from .storage import SQLiteStorage


@dataclass(slots=True)
class _QueuedSummaryJob:
    job_id: int
    batch: SummaryBatch
    reason: str


class Summarizer:
    def __init__(
        self,
        storage: SQLiteStorage,
        batch_builder: BatchBuilder,
        lm_client: LMStudioClient,
        max_parallel_jobs: int = 2,
        error_notifier: ErrorNotificationManager | None = None,
    ) -> None:
        self.storage = storage
        self.batch_builder = batch_builder
        self.lm_client = lm_client
        self.logger = logging.getLogger(__name__)
        self.error_notifier = error_notifier or ErrorNotificationManager()

        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._queue: deque[_QueuedSummaryJob] = deque()
        self._reserved_ranges: dict[int, tuple[float, float]] = {}
        self._running_jobs: set[int] = set()
        self._workers: list[threading.Thread] = []
        self._stop_event = threading.Event()
        self._max_parallel_jobs = max(1, int(max_parallel_jobs))
        self._unrecoverable_error: str | None = None

        self._ensure_worker_count_locked()

    def stop(self) -> None:
        self._stop_event.set()
        with self._condition:
            self._condition.notify_all()
        for worker in self._workers:
            worker.join(timeout=2)

    def update_max_parallel_jobs(self, max_parallel_jobs: int) -> None:
        with self._condition:
            self._max_parallel_jobs = max(1, int(max_parallel_jobs))
            self._ensure_worker_count_locked()
            self._condition.notify_all()

    def clear_unrecoverable_error(self) -> None:
        with self._lock:
            self._unrecoverable_error = None

    def get_unrecoverable_error(self) -> str | None:
        with self._lock:
            return self._unrecoverable_error

    def has_unrecoverable_error(self) -> bool:
        return self.get_unrecoverable_error() is not None

    def dispatch_pending_jobs(self, reason: str = "manual", max_new_jobs: int | None = None) -> int:
        with self._condition:
            available_slots = self._max_parallel_jobs - (len(self._queue) + len(self._running_jobs))
            if max_new_jobs is not None:
                available_slots = min(available_slots, max_new_jobs)
            if available_slots <= 0:
                return 0

            created = 0
            for _ in range(available_slots):
                excluded_ranges = list(self._reserved_ranges.values())
                batch = self.batch_builder.build_pending_batch(excluded_ranges=excluded_ranges)
                if batch is None:
                    break

                job_id = self.storage.create_summary_job(batch.start_ts, batch.end_ts, status="queued")
                self._queue.append(_QueuedSummaryJob(job_id=job_id, batch=batch, reason=reason))
                self._reserved_ranges[job_id] = (batch.start_ts, batch.end_ts)
                created += 1

                self.logger.info(
                    (
                        "event=summary_job_queued job_id=%s reason=%s start_ts=%.3f end_ts=%.3f "
                        "intervals=%s blocked_intervals=%s text_segments=%s screenshots=%s queue_size=%s"
                    ),
                    job_id,
                    reason,
                    batch.start_ts,
                    batch.end_ts,
                    len(batch.active_intervals),
                    len(batch.blocked_intervals),
                    len(batch.text_segments),
                    len(batch.screenshots),
                    len(self._queue),
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
                self._reserved_ranges.pop(item.job_id, None)
            self._condition.notify_all()
            return len(cancelled)

    def wait_for_activity(self, timeout_seconds: float = 0.5) -> None:
        with self._condition:
            self._condition.wait(timeout=timeout_seconds)

    def get_runtime_status(self) -> dict[str, int | bool | str | None]:
        with self._lock:
            queued_jobs = len(self._queue)
            running_jobs = len(self._running_jobs)
            max_parallel = self._max_parallel_jobs
            unrecoverable_error = self._unrecoverable_error

        persisted_counts = self.storage.get_summary_job_status_counts()
        return {
            "queued_jobs": queued_jobs,
            "running_jobs": running_jobs,
            "pending_summary_jobs": queued_jobs + running_jobs,
            "completed_jobs": int(persisted_counts.get("succeeded", 0)),
            "failed_jobs": int(persisted_counts.get("failed", 0)),
            "cancelled_jobs": int(persisted_counts.get("cancelled", 0)),
            "max_parallel_summary_jobs": max_parallel,
            "has_unrecoverable_error": unrecoverable_error is not None,
            "unrecoverable_error": unrecoverable_error,
        }

    def flush_pending(self, reason: str = "manual") -> int | None:
        batch = self.batch_builder.build_pending_batch(excluded_ranges=list(self._reserved_ranges.values()))
        if batch is None:
            self.logger.info("event=summary_job_skipped reason=%s detail=no_pending_data", reason)
            return None

        job_id = self.storage.create_summary_job(batch.start_ts, batch.end_ts, status="running")
        return self._run_summary_job(job_id=job_id, batch=batch, reason=reason)

    def generate_daily_recap_for_day(self, day: date) -> tuple[int, bool]:
        summaries = self.storage.list_summaries_for_day(day)
        if not summaries:
            raise ValueError(f"No summaries available for day {day.isoformat()}")

        try:
            recap_text, recap_json = self.lm_client.summarize_daily_recap(day=day, summaries=summaries)
        except LMStudioConnectionError as exc:
            self._notify_lmstudio_error("lmstudio_connection", str(exc), key=f"{self._lmstudio_identity()}|connection")
            raise
        except LMStudioServiceUnavailableError as exc:
            self._notify_lmstudio_error(
                "lmstudio_service_unavailable",
                str(exc),
                key=f"{self._lmstudio_identity()}|unavailable",
            )
            raise
        else:
            self.error_notifier.resolve_many("lmstudio_connection", "lmstudio_service_unavailable")

        daily_summary, replaced = self.storage.create_daily_summary(
            day=day,
            recap_text=recap_text,
            recap_json=recap_json if isinstance(recap_json, dict) else None,
            source_batch_count=len(summaries),
        )
        return int(daily_summary.id or 0), replaced

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            queued_job: _QueuedSummaryJob | None = None
            with self._condition:
                while not self._stop_event.is_set() and not self._queue:
                    self._condition.wait(timeout=0.5)
                if self._stop_event.is_set():
                    return
                if self._queue:
                    queued_job = self._queue.popleft()
                    self._running_jobs.add(queued_job.job_id)
                    self.logger.info(
                        "event=summary_job_dequeued job_id=%s queue_size=%s running_jobs=%s",
                        queued_job.job_id,
                        len(self._queue),
                        len(self._running_jobs),
                    )

            if queued_job is None:
                continue

            self.storage.update_summary_job(queued_job.job_id, status="running")
            self._run_summary_job(job_id=queued_job.job_id, batch=queued_job.batch, reason=queued_job.reason)

            with self._condition:
                self._running_jobs.discard(queued_job.job_id)
                self._reserved_ranges.pop(queued_job.job_id, None)
                self._condition.notify_all()

    def _run_summary_job(self, job_id: int, batch: SummaryBatch, reason: str) -> int | None:
        self.logger.info(
            (
                "event=summary_job_started job_id=%s reason=%s start_ts=%.3f end_ts=%.3f "
                "intervals=%s blocked_intervals=%s text_segments=%s screenshots=%s queue_size=%s"
            ),
            job_id,
            reason,
            batch.start_ts,
            batch.end_ts,
            len(batch.active_intervals),
            len(batch.blocked_intervals),
            len(batch.text_segments),
            len(batch.screenshots),
            len(self._queue),
        )
        try:
            summary_text, summary_json = self.lm_client.summarize_batch(batch)
            summary_id = self.storage.insert_summary(
                job_id=job_id,
                start_ts=batch.start_ts,
                end_ts=batch.end_ts,
                summary_text=summary_text,
                summary_json=summary_json,
            )

            self.storage.mark_intervals_summarized(batch.start_ts, batch.end_ts)
            self.storage.purge_raw_data(batch.start_ts, batch.end_ts)
            self.storage.update_summary_job(job_id, status="succeeded")
            self.error_notifier.resolve_many(
                "summary_generation_failure",
                "lmstudio_connection",
                "lmstudio_service_unavailable",
            )
            self.logger.info(
                "event=summary_job_completed job_id=%s summary_id=%s start_ts=%.3f end_ts=%.3f",
                job_id,
                summary_id,
                batch.start_ts,
                batch.end_ts,
            )
            return summary_id
        except LMStudioConnectionError as exc:
            self.storage.update_summary_job(job_id, status="failed", error="LM Studio is unreachable.")
            self._unrecoverable_error = "Connection error: Unable to reach LM Studio. Check that it is running."
            self._notify_lmstudio_error(
                "lmstudio_connection",
                str(exc),
                key=f"{self._lmstudio_identity()}|connection",
            )
            self.logger.exception("event=summary_job_failed job_id=%s reason=%s error=%s", job_id, reason, exc)
            return None
        except LMStudioServiceUnavailableError as exc:
            self.storage.update_summary_job(
                job_id,
                status="failed",
                error="LM Studio returned an unavailable response.",
            )
            self._unrecoverable_error = "Service unavailable: LM Studio could not generate a response."
            self._notify_lmstudio_error(
                "lmstudio_service_unavailable",
                str(exc),
                key=f"{self._lmstudio_identity()}|unavailable",
            )
            self.logger.exception("event=summary_job_failed job_id=%s reason=%s error=%s", job_id, reason, exc)
            return None
        except Exception as exc:
            self.storage.update_summary_job(job_id, status="failed", error="Summary generation failed.")
            with self._lock:
                self._unrecoverable_error = "Summary generation failed."
            self.error_notifier.notify(
                "summary_generation_failure",
                "Summary generation failed. Check that LM Studio is running and the configured model is available.",
                key=f"{self._lmstudio_identity()}|{exc.__class__.__name__}",
            )
            self.logger.exception("event=summary_job_failed job_id=%s reason=%s error=%s", job_id, reason, exc)
            return None

    def _ensure_worker_count_locked(self) -> None:
        while len(self._workers) < self._max_parallel_jobs:
            index = len(self._workers) + 1
            worker = threading.Thread(target=self._worker_loop, name=f"SummaryWorker-{index}", daemon=True)
            self._workers.append(worker)
            worker.start()

    def wait_for_idle(self, timeout_seconds: float) -> bool:
        deadline = time.time() + max(0.0, timeout_seconds)
        with self._condition:
            while self._queue or self._running_jobs:
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
