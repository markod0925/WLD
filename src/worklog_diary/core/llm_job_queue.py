from __future__ import annotations

import heapq
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar

from .lmstudio_logging import llm_job_context, log_llm_stage, safe_error

T = TypeVar("T")


class LLMJobCancelledError(RuntimeError):
    """Raised when a queued LLM job is cancelled before it starts."""


@dataclass(slots=True)
class LLMJobMetadata:
    job_id: object
    job_type: str
    queued_at: float
    started_at: float | None = None
    finished_at: float | None = None
    timeout_s: float = 0.0
    attempt: int = 1
    input_chars: int = 0
    input_token_estimate: int | None = None
    priority: int = 100


@dataclass(slots=True)
class _QueuedLLMJob(Generic[T]):
    metadata: LLMJobMetadata
    operation: Callable[[], T]
    done: threading.Event = field(default_factory=threading.Event)
    result: T | None = None
    error: BaseException | None = None
    on_started: Callable[[LLMJobMetadata], None] | None = None
    on_cancelled: Callable[[LLMJobMetadata, str], None] | None = None


class LLMJobQueue:
    def __init__(self, *, max_concurrent_jobs: int = 1, logger: logging.Logger | None = None) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._queue: list[tuple[int, int, _QueuedLLMJob[Any]]] = []
        self._sequence = 0
        self._next_job_number = 0
        self._active_jobs = 0
        self._max_concurrent_jobs = max(1, int(max_concurrent_jobs))
        self._accepting_jobs = True
        self._closing = False
        self._closed = False
        self._stop_event = threading.Event()
        self._workers: list[threading.Thread] = []
        self._start_workers()

    def allocate_job_id(self, job_type: str) -> str:
        with self._condition:
            self._next_job_number += 1
            return f"{job_type}:{self._next_job_number}"

    def submit(
        self,
        *,
        job_type: str,
        operation: Callable[[], T],
        job_id: object | None = None,
        timeout_s: int | float = 0,
        attempt: int = 1,
        input_chars: int = 0,
        input_token_estimate: int | None = None,
        priority: int = 100,
        on_started: Callable[[LLMJobMetadata], None] | None = None,
        on_cancelled: Callable[[LLMJobMetadata, str], None] | None = None,
    ) -> T:
        if job_id is None:
            job_id = self.allocate_job_id(job_type)
        metadata = LLMJobMetadata(
            job_id=job_id,
            job_type=job_type,
            queued_at=0.0,
            timeout_s=float(timeout_s),
            attempt=max(1, int(attempt)),
            input_chars=max(0, int(input_chars)),
            input_token_estimate=input_token_estimate,
            priority=int(priority),
        )
        job: _QueuedLLMJob[T] = _QueuedLLMJob(
            metadata=metadata,
            operation=operation,
            on_started=on_started,
            on_cancelled=on_cancelled,
        )

        with self._condition:
            if not self._accepting_jobs or self._stop_event.is_set():
                queue_reason = "queue_closing" if self._closing else "queue_stopped"
                job.error = LLMJobCancelledError("LLM queue is shutting down")
                job.metadata.finished_at = time.time()
                self._condition.notify_all()
                cancelled = True
            else:
                job.metadata.queued_at = time.time()
                heapq.heappush(self._queue, (job.metadata.priority, self._sequence, job))
                self._sequence += 1
                queue_size = len(self._queue)
                log_llm_stage(
                    self.logger,
                    "job_queued",
                    "ok",
                    job_id=job.metadata.job_id,
                    job_type=job.metadata.job_type,
                    timeout_s=job.metadata.timeout_s,
                    attempt=job.metadata.attempt,
                    input_chars=job.metadata.input_chars,
                    input_token_estimate=job.metadata.input_token_estimate,
                    priority=job.metadata.priority,
                    queue_size=queue_size,
                )
                self._condition.notify()
                cancelled = False

        if cancelled:
            if job.on_cancelled is not None:
                try:
                    job.on_cancelled(job.metadata, queue_reason)
                except Exception:
                    self.logger.exception(
                        "event=llm_queue_callback_failed job_id=%s job_type=%s callback=on_cancelled",
                        job.metadata.job_id,
                        job.metadata.job_type,
                    )
            log_llm_stage(
                self.logger,
                "job_cancelled",
                "skip",
                job_id=job.metadata.job_id,
                job_type=job.metadata.job_type,
                timeout_s=job.metadata.timeout_s,
                attempt=job.metadata.attempt,
                input_chars=job.metadata.input_chars,
                input_token_estimate=job.metadata.input_token_estimate,
                reason=queue_reason,
                queue_size=len(self._queue),
            )
            raise job.error

        job.done.wait()
        if job.error is not None:
            raise job.error
        return job.result  # type: ignore[return-value]

    def snapshot(self) -> dict[str, int | bool]:
        with self._condition:
            queued_jobs = len(self._queue)
            running_jobs = self._active_jobs
            max_concurrent = self._max_concurrent_jobs
            accepting_jobs = self._accepting_jobs
            closing = self._closing
            closed = self._closed
        return {
            "queued_jobs": queued_jobs,
            "running_jobs": running_jobs,
            "pending_jobs": queued_jobs + running_jobs,
            "max_concurrent_jobs": max_concurrent,
            "accepting_jobs": accepting_jobs,
            "closing": closing,
            "closed": closed,
            "stopped": closed or closing or not accepting_jobs,
        }

    def stop(self, *, reason: str = "shutdown") -> int:
        with self._condition:
            if self._closed:
                return 0
            self._accepting_jobs = False
            self._closing = True
            self._stop_event.set()

            cancelled_jobs: list[_QueuedLLMJob[Any]] = []
            while self._queue:
                _, _, job = heapq.heappop(self._queue)
                job.metadata.finished_at = time.time()
                job.error = LLMJobCancelledError(reason)
                cancelled_jobs.append(job)
            self._condition.notify_all()

        cancelled = 0
        for job in cancelled_jobs:
            cancelled += 1
            if job.on_cancelled is not None:
                try:
                    job.on_cancelled(job.metadata, reason)
                except Exception:
                    self.logger.exception(
                        "event=llm_queue_callback_failed job_id=%s job_type=%s callback=on_cancelled",
                        job.metadata.job_id,
                        job.metadata.job_type,
                    )
            log_llm_stage(
                self.logger,
                "job_cancelled",
                "skip",
                job_id=job.metadata.job_id,
                job_type=job.metadata.job_type,
                timeout_s=job.metadata.timeout_s,
                attempt=job.metadata.attempt,
                input_chars=job.metadata.input_chars,
                input_token_estimate=job.metadata.input_token_estimate,
                reason=reason,
                queue_size=len(self._queue),
            )
            job.done.set()

        for worker in self._workers:
            worker.join(timeout=2)
        with self._condition:
            self._closing = False
            self._closed = True
            self._condition.notify_all()
        return cancelled

    def _start_workers(self) -> None:
        with self._condition:
            while len(self._workers) < self._max_concurrent_jobs:
                index = len(self._workers) + 1
                worker = threading.Thread(target=self._worker_loop, name=f"LLMJobWorker-{index}", daemon=True)
                self._workers.append(worker)
                worker.start()

    def _worker_loop(self) -> None:
        while True:
            with self._condition:
                while not self._stop_event.is_set() and (not self._queue or self._active_jobs >= self._max_concurrent_jobs):
                    self._condition.wait(timeout=0.5)
                if self._stop_event.is_set() and not self._queue:
                    return
                if not self._queue:
                    continue
                _, _, job = heapq.heappop(self._queue)
                self._active_jobs += 1
                queue_size = len(self._queue)

            started_at = time.time()
            queue_wait_s = max(0.0, started_at - job.metadata.queued_at)
            job.metadata.started_at = started_at
            if job.on_started is not None:
                try:
                    job.on_started(job.metadata)
                except Exception:
                    self.logger.exception(
                        "event=llm_queue_callback_failed job_id=%s job_type=%s callback=on_started",
                        job.metadata.job_id,
                        job.metadata.job_type,
                    )
            log_llm_stage(
                self.logger,
                "job_started",
                "ok",
                job_id=job.metadata.job_id,
                job_type=job.metadata.job_type,
                timeout_s=job.metadata.timeout_s,
                attempt=job.metadata.attempt,
                input_chars=job.metadata.input_chars,
                input_token_estimate=job.metadata.input_token_estimate,
                queue_size=queue_size,
                queue_wait_s=queue_wait_s,
            )

            try:
                with llm_job_context(
                    job.metadata.job_id,
                    job_type=job.metadata.job_type,
                    timeout_s=job.metadata.timeout_s,
                    attempt=job.metadata.attempt,
                    input_chars=job.metadata.input_chars,
                    input_token_estimate=job.metadata.input_token_estimate,
                    queue_size=queue_size,
                    queue_wait_s=queue_wait_s,
                ):
                    job.result = job.operation()
            except BaseException as exc:
                job.error = exc
                job.metadata.finished_at = time.time()
                stage = "job_cancelled" if isinstance(exc, LLMJobCancelledError) else "job_failed"
                level = logging.INFO if isinstance(exc, LLMJobCancelledError) else logging.ERROR
                log_llm_stage(
                    self.logger,
                    stage,
                    "skip" if isinstance(exc, LLMJobCancelledError) else "error",
                    level=level,
                    job_id=job.metadata.job_id,
                    job_type=job.metadata.job_type,
                    timeout_s=job.metadata.timeout_s,
                    attempt=job.metadata.attempt,
                    input_chars=job.metadata.input_chars,
                    input_token_estimate=job.metadata.input_token_estimate,
                    elapsed_s=max(0.0, job.metadata.finished_at - started_at),
                    queue_wait_s=queue_wait_s,
                    error_type=exc.__class__.__name__,
                    error=safe_error(exc),
                    exc_info=not isinstance(exc, LLMJobCancelledError),
                )
            else:
                job.metadata.finished_at = time.time()
                log_llm_stage(
                    self.logger,
                    "job_completed",
                    "ok",
                    job_id=job.metadata.job_id,
                    job_type=job.metadata.job_type,
                    timeout_s=job.metadata.timeout_s,
                    attempt=job.metadata.attempt,
                    input_chars=job.metadata.input_chars,
                    input_token_estimate=job.metadata.input_token_estimate,
                    elapsed_s=max(0.0, job.metadata.finished_at - started_at),
                    queue_wait_s=queue_wait_s,
                )
            finally:
                with self._condition:
                    self._active_jobs -= 1
                    self._condition.notify_all()
                job.done.set()
