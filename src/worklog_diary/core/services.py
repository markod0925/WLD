from __future__ import annotations

import logging
import math
import threading
import time
from dataclasses import dataclass

from .batching import BatchBuilder
from .config import AppConfig, save_config
from .keyboard_capture import KeyboardCaptureService
from .lmstudio_client import LMStudioClient
from .logging_setup import configure_logging
from .models import SharedState
from .privacy import PrivacyPolicyEngine
from .scheduler import FlushScheduler
from .screenshot_capture import ScreenshotCaptureService
from .storage import SQLiteStorage
from .summarizer import Summarizer
from .text_reconstructor import TextReconstructionService, TextReconstructor
from .window_tracker import ForegroundWindowTrackerService


@dataclass(slots=True)
class FlushDrainResult:
    stop_reason: str
    summaries_created: int
    failed_jobs: int
    cancelled_jobs: int
    pending_summary_jobs: int


class MonitoringServices:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.config.ensure_paths()

        configure_logging(self.config.app_data_dir)
        self.logger = logging.getLogger(__name__)

        self.state = SharedState()
        self.storage = SQLiteStorage(self.config.db_path)
        self.privacy = PrivacyPolicyEngine(set(self.config.blocked_processes))

        self.window_tracker = ForegroundWindowTrackerService(
            storage=self.storage,
            privacy=self.privacy,
            state=self.state,
            poll_interval_seconds=self.config.foreground_poll_interval_seconds,
        )
        self.keyboard_capture = KeyboardCaptureService(
            storage=self.storage,
            state=self.state,
            privacy=self.privacy,
        )
        self.screenshot_capture = ScreenshotCaptureService(
            storage=self.storage,
            state=self.state,
            privacy=self.privacy,
            screenshot_dir=self.config.screenshot_dir,
            interval_seconds=self.config.screenshot_interval_seconds,
            capture_mode=self.config.capture_mode,
        )

        self.text_reconstructor = TextReconstructor(
            inactivity_gap_seconds=self.config.text_inactivity_gap_seconds,
        )
        self.text_service = TextReconstructionService(
            storage=self.storage,
            reconstructor=self.text_reconstructor,
            poll_interval_seconds=self.config.reconstruction_poll_interval_seconds,
        )

        self.batch_builder = BatchBuilder(
            storage=self.storage,
            max_text_segments=self.config.max_text_segments_per_summary,
            max_screenshots=self.config.max_screenshots_per_summary,
        )
        self.lmstudio_client = LMStudioClient(
            base_url=self.config.lmstudio_base_url,
            model=self.config.lmstudio_model,
            timeout_seconds=self.config.request_timeout_seconds,
        )
        self.summarizer = Summarizer(
            storage=self.storage,
            batch_builder=self.batch_builder,
            lm_client=self.lmstudio_client,
            max_parallel_jobs=self.config.max_parallel_summary_jobs,
        )
        self.scheduler = FlushScheduler(
            interval_seconds=self.config.flush_interval_seconds,
            flush_callback=self.flush_now,
            state=self.state,
        )

        self._services_running = False
        self._flush_lock = threading.Lock()
        self._drain_cancel_event = threading.Event()
        self._status_lock = threading.Lock()
        self._drain_active = False
        self._drain_reason: str | None = None

    def start_monitoring(self) -> None:
        self.state.set_monitoring_active(True)
        if not self._services_running:
            self.window_tracker.start()
            self.keyboard_capture.start()
            self.screenshot_capture.start()
            self.text_service.start()
            self.scheduler.start()
            self._services_running = True

        self.logger.info("event=monitoring_state_change active=true")

    def pause_monitoring(self) -> None:
        self.state.set_monitoring_active(False)
        self.window_tracker.pause()
        self.logger.info("event=monitoring_state_change active=false mode=pause")

    def stop_monitoring(self) -> None:
        self.state.set_monitoring_active(False)
        self.window_tracker.pause()

        if self._services_running:
            self.scheduler.stop()
            self.screenshot_capture.stop()
            self.keyboard_capture.stop()
            self.text_service.stop()
            self.window_tracker.stop()
            self._services_running = False

        self.logger.info("event=monitoring_state_change active=false mode=stop")

    def cancel_flush_drain(self) -> bool:
        if not self.is_drain_active:
            return False
        self._drain_cancel_event.set()
        self.logger.info("event=summary_drain_cancel_requested")
        return True

    @property
    def is_drain_active(self) -> bool:
        with self._status_lock:
            return self._drain_active

    def flush_now(self, reason: str = "manual") -> FlushDrainResult | None:
        if not self._flush_lock.acquire(blocking=False):
            self.logger.info("event=summary_flush_skipped reason=already_running request_reason=%s", reason)
            return None

        self._drain_cancel_event.clear()
        self.summarizer.clear_unrecoverable_error()
        start_counts = self.storage.get_summary_job_status_counts()

        with self._status_lock:
            self._drain_active = True
            self._drain_reason = reason

        self.logger.info(
            "event=summary_drain_started reason=%s max_parallel_jobs=%s",
            reason,
            self.config.max_parallel_summary_jobs,
        )

        stop_reason = "empty"
        summaries_created = 0

        try:
            while True:
                if self._drain_cancel_event.is_set():
                    cancelled = self.summarizer.cancel_queued_jobs(reason="cancelled_by_user")
                    self.logger.info("event=summary_drain_stopped reason=cancelled cancelled_queued_jobs=%s", cancelled)
                    stop_reason = "cancelled"
                    break

                for _ in range(5):
                    self.text_service.process_once(force_flush=True)
                    if self.storage.count_unprocessed_key_events() == 0:
                        break

                dispatched = self.summarizer.dispatch_pending_jobs(reason=reason)
                summaries_created += dispatched

                pending = self.storage.get_pending_counts()
                runtime = self.summarizer.get_runtime_status()

                self.logger.info(
                    (
                        "event=summary_drain_tick reason=%s dispatched=%s queued=%s running=%s "
                        "pending_text_segments=%s pending_screenshots=%s pending_intervals=%s"
                    ),
                    reason,
                    dispatched,
                    runtime["queued_jobs"],
                    runtime["running_jobs"],
                    pending["text_segments"],
                    pending["screenshots"],
                    pending["intervals"],
                )

                if bool(runtime["has_unrecoverable_error"]):
                    self.summarizer.cancel_queued_jobs(reason="cancelled_after_failure")
                    stop_reason = "error"
                    break

                backlog_remaining = _has_pending_backlog(pending)
                if not backlog_remaining and int(runtime["pending_summary_jobs"]) == 0:
                    stop_reason = "empty"
                    break

                self.summarizer.wait_for_activity(timeout_seconds=0.4)

            self.summarizer.wait_for_idle(timeout_seconds=30.0)

            now = time.time()
            self.state.set_flush_times(last_flush_ts=now, next_flush_ts=now + self.config.flush_interval_seconds)

            end_counts = self.storage.get_summary_job_status_counts()
            result = FlushDrainResult(
                stop_reason=stop_reason,
                summaries_created=max(0, end_counts.get("succeeded", 0) - start_counts.get("succeeded", 0)),
                failed_jobs=max(0, end_counts.get("failed", 0) - start_counts.get("failed", 0)),
                cancelled_jobs=max(0, end_counts.get("cancelled", 0) - start_counts.get("cancelled", 0)),
                pending_summary_jobs=int(self.summarizer.get_runtime_status()["pending_summary_jobs"]),
            )
            self.logger.info(
                (
                    "event=summary_drain_finished reason=%s stop_reason=%s created=%s failed=%s cancelled=%s "
                    "pending_summary_jobs=%s"
                ),
                reason,
                result.stop_reason,
                result.summaries_created,
                result.failed_jobs,
                result.cancelled_jobs,
                result.pending_summary_jobs,
            )
            return result
        finally:
            with self._status_lock:
                self._drain_active = False
                self._drain_reason = None
            self._flush_lock.release()

    def apply_config(self, config: AppConfig) -> None:
        was_monitoring = self.state.snapshot().monitoring_active

        if self._services_running:
            self.stop_monitoring()

        self.config = config
        self.config.ensure_paths()
        save_config(self.config, self.config.config_path)

        self.privacy.update_blocked_processes(self.config.blocked_processes)
        self.window_tracker.poll_interval_seconds = max(0.2, self.config.foreground_poll_interval_seconds)
        self.screenshot_capture.interval_seconds = max(5, self.config.screenshot_interval_seconds)
        self.screenshot_capture.capture_mode = self.config.capture_mode
        self.text_service.poll_interval_seconds = max(0.5, self.config.reconstruction_poll_interval_seconds)
        self.text_reconstructor.inactivity_gap_seconds = self.config.text_inactivity_gap_seconds
        self.batch_builder.max_text_segments = self.config.max_text_segments_per_summary
        self.batch_builder.max_screenshots = self.config.max_screenshots_per_summary
        self.lmstudio_client.base_url = self.config.lmstudio_base_url.rstrip("/")
        self.lmstudio_client.model = self.config.lmstudio_model
        self.lmstudio_client.timeout_seconds = self.config.request_timeout_seconds
        self.summarizer.update_max_parallel_jobs(self.config.max_parallel_summary_jobs)
        self.scheduler.interval_seconds = max(30, self.config.flush_interval_seconds)

        if was_monitoring:
            self.start_monitoring()

    def get_status(self) -> dict:
        snapshot = self.state.snapshot()
        pending = self.storage.get_pending_counts()
        summary_runtime = self.summarizer.get_runtime_status()
        with self._status_lock:
            drain_active = self._drain_active
            drain_reason = self._drain_reason

        buffer_state = _synthesize_buffer_state(
            pending_counts=pending,
            running_jobs=int(summary_runtime["running_jobs"]),
        )
        approx_batches_remaining = _estimate_remaining_batches(
            pending_counts=pending,
            pending_summary_jobs=int(summary_runtime["pending_summary_jobs"]),
            max_text_segments=max(1, self.batch_builder.max_text_segments),
            max_screenshots=max(1, self.batch_builder.max_screenshots),
        )

        return {
            "monitoring_active": snapshot.monitoring_active,
            "blocked": snapshot.blocked,
            "foreground": snapshot.foreground_info,
            "active_interval_id": snapshot.active_interval_id,
            "last_flush_ts": snapshot.last_flush_ts,
            "next_flush_ts": snapshot.next_flush_ts,
            "pending": pending,
            "pending_screenshot_count": pending["screenshots"],
            "pending_text_segment_count": pending["text_segments"],
            "pending_summary_job_count": int(summary_runtime["pending_summary_jobs"]),
            "summary_running": int(summary_runtime["running_jobs"]) > 0,
            "summary_jobs": {
                "queued": int(summary_runtime["queued_jobs"]),
                "running": int(summary_runtime["running_jobs"]),
                "completed": int(summary_runtime["completed_jobs"]),
                "failed": int(summary_runtime["failed_jobs"]),
                "cancelled": int(summary_runtime["cancelled_jobs"]),
            },
            "flush_drain_active": drain_active,
            "flush_drain_reason": drain_reason,
            "flush_drain_cancel_requested": self._drain_cancel_event.is_set(),
            "buffer_state": buffer_state,
            "approx_remaining_batches": approx_batches_remaining,
            "max_parallel_summary_jobs": int(summary_runtime["max_parallel_summary_jobs"]),
            "unrecoverable_summary_error": summary_runtime["unrecoverable_error"],
        }

    def shutdown(self) -> None:
        self.cancel_flush_drain()
        self.stop_monitoring()
        self.summarizer.stop()
        self.storage.close()



def _has_pending_backlog(pending_counts: dict[str, int]) -> bool:
    return (
        pending_counts["intervals"] > 0
        or pending_counts["key_events"] > 0
        or pending_counts["processed_key_events"] > 0
        or pending_counts["text_segments"] > 0
        or pending_counts["screenshots"] > 0
    )



def _synthesize_buffer_state(pending_counts: dict[str, int], running_jobs: int) -> str:
    has_backlog = _has_pending_backlog(pending_counts)
    if running_jobs > 0:
        if has_backlog:
            return "Summarizing, backlog remaining"
        return "Summarizing"
    if has_backlog:
        return "Buffer pending"
    return "Buffer empty"



def _estimate_remaining_batches(
    pending_counts: dict[str, int],
    pending_summary_jobs: int,
    max_text_segments: int,
    max_screenshots: int,
) -> int:
    text_batches = math.ceil(pending_counts["text_segments"] / max_text_segments) if pending_counts["text_segments"] > 0 else 0
    screenshot_batches = (
        math.ceil(pending_counts["screenshots"] / max_screenshots) if pending_counts["screenshots"] > 0 else 0
    )
    interval_batches = 1 if pending_counts["intervals"] > 0 else 0
    raw_estimate = max(text_batches, screenshot_batches, interval_batches)
    return max(raw_estimate, pending_summary_jobs)
