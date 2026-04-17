from __future__ import annotations

import logging
import math
import threading
import time
from dataclasses import dataclass
from collections.abc import Callable

from .batching import BatchBuilder
from .config import AppConfig
from .error_notifications import ErrorNotificationManager
from .keyboard_capture import KeyboardCaptureService
from .lmstudio_client import LMStudioClient
from .models import SharedState
from .privacy import PrivacyPolicyEngine
from .scheduler import FlushScheduler
from .screenshot_capture import ScreenshotCaptureService
from .session_monitor import SessionMonitor
from .storage import SQLiteStorage
from .summarizer import Summarizer
from .summary_dedup import SummaryDeduplicator
from .text_reconstructor import TextReconstructionService, TextReconstructor
from .window_tracker import ForegroundWindowTrackerService


@dataclass(slots=True)
class FlushDrainResult:
    stop_reason: str
    summaries_created: int
    failed_jobs: int
    cancelled_jobs: int
    pending_summary_jobs: int


@dataclass(slots=True)
class MonitoringServiceBundle:
    shutdown_event: threading.Event
    state: SharedState
    storage: SQLiteStorage
    privacy: PrivacyPolicyEngine
    error_notifier: ErrorNotificationManager
    window_tracker: ForegroundWindowTrackerService
    keyboard_capture: KeyboardCaptureService
    screenshot_capture: ScreenshotCaptureService
    text_reconstructor: TextReconstructor
    text_service: TextReconstructionService
    batch_builder: BatchBuilder
    lmstudio_client: LMStudioClient
    summarizer: Summarizer
    scheduler: FlushScheduler | None = None
    session_monitor: SessionMonitor | None = None


class ServiceRegistry:
    def __init__(self, config: AppConfig) -> None:
        self.config = config

    def build_bundle(self) -> MonitoringServiceBundle:
        shutdown_event = threading.Event()
        state = SharedState()
        storage = SQLiteStorage(self.config.db_path)
        privacy = PrivacyPolicyEngine(set(self.config.blocked_processes))
        error_notifier = ErrorNotificationManager()

        window_tracker = ForegroundWindowTrackerService(
            storage=storage,
            privacy=privacy,
            state=state,
            poll_interval_seconds=self.config.foreground_poll_interval_seconds,
            shutdown_event=shutdown_event,
        )
        keyboard_capture = KeyboardCaptureService(
            storage=storage,
            state=state,
            privacy=privacy,
            shutdown_event=shutdown_event,
        )
        screenshot_capture = ScreenshotCaptureService(
            storage=storage,
            state=state,
            privacy=privacy,
            screenshot_dir=self.config.screenshot_dir,
            interval_seconds=self.config.screenshot_interval_seconds,
            capture_mode=self.config.capture_mode,
            shutdown_event=shutdown_event,
            dedup_exact_hash_enabled=self.config.screenshot_dedup_exact_hash_enabled,
            dedup_perceptual_hash_enabled=self.config.screenshot_dedup_perceptual_hash_enabled,
            dedup_phash_threshold=self.config.screenshot_dedup_phash_threshold,
            dedup_ssim_enabled=self.config.screenshot_dedup_ssim_enabled,
            dedup_ssim_threshold=self.config.screenshot_dedup_ssim_threshold,
            dedup_resize_width=self.config.screenshot_dedup_resize_width,
            dedup_compare_recent_count=self.config.screenshot_dedup_compare_recent_count,
            min_keep_interval_seconds=self.config.screenshot_min_keep_interval_seconds,
        )

        text_reconstructor = TextReconstructor(
            inactivity_gap_seconds=self.config.text_inactivity_gap_seconds,
        )
        text_service = TextReconstructionService(
            storage=storage,
            reconstructor=text_reconstructor,
            poll_interval_seconds=self.config.reconstruction_poll_interval_seconds,
            state=state,
            shutdown_event=shutdown_event,
        )

        batch_builder = BatchBuilder(
            storage=storage,
            max_text_segments=self.config.max_text_segments_per_summary,
            max_screenshots=self.config.max_screenshots_per_summary,
            dedup_enabled=self.config.screenshot_dedup_enabled,
            dedup_threshold=self.config.screenshot_dedup_threshold,
            min_keep_interval_seconds=self.config.screenshot_min_keep_interval_seconds,
            activity_segment_min_duration_seconds=self.config.activity_segment_min_duration_seconds,
            activity_segment_max_duration_seconds=self.config.activity_segment_max_duration_seconds,
            activity_segment_idle_gap_seconds=self.config.activity_segment_idle_gap_seconds,
            activity_segment_title_similarity_threshold=self.config.activity_segment_title_similarity_threshold,
            activity_segment_screenshot_phash_threshold=self.config.screenshot_dedup_phash_threshold,
            activity_segment_screenshot_ssim_threshold=self.config.screenshot_dedup_ssim_threshold,
        )
        lmstudio_client = LMStudioClient(
            base_url=self.config.lmstudio_base_url,
            model=self.config.lmstudio_model,
            timeout_seconds=self.config.request_timeout_seconds,
        )
        summary_deduplicator = SummaryDeduplicator(
            suppress_threshold=self.config.summary_similarity_suppress_threshold,
            merge_threshold=self.config.summary_similarity_merge_threshold,
            cooldown_seconds=self.config.summary_cooldown_seconds,
            recent_compare_count=self.config.recent_summary_compare_count,
        )
        summarizer = Summarizer(
            storage=storage,
            batch_builder=batch_builder,
            lm_client=lmstudio_client,
            max_parallel_jobs=self.config.max_parallel_summary_jobs,
            error_notifier=error_notifier,
            shutdown_event=shutdown_event,
            summary_deduplicator=summary_deduplicator,
        )

        return MonitoringServiceBundle(
            shutdown_event=shutdown_event,
            state=state,
            storage=storage,
            privacy=privacy,
            error_notifier=error_notifier,
            window_tracker=window_tracker,
            keyboard_capture=keyboard_capture,
            screenshot_capture=screenshot_capture,
            text_reconstructor=text_reconstructor,
            text_service=text_service,
            batch_builder=batch_builder,
            lmstudio_client=lmstudio_client,
            summarizer=summarizer,
        )

    def build_scheduler(
        self,
        *,
        flush_callback: Callable[[str], FlushDrainResult | None],
        state: SharedState,
        shutdown_event: threading.Event | None = None,
    ) -> FlushScheduler:
        return FlushScheduler(
            interval_seconds=self.config.flush_interval_seconds,
            flush_callback=flush_callback,
            state=state,
            shutdown_event=shutdown_event,
        )

    def build_session_monitor(
        self,
        *,
        on_locked: Callable[[], None],
        on_unlocked: Callable[[], None],
    ) -> SessionMonitor:
        return SessionMonitor(
            on_locked=on_locked,
            on_unlocked=on_unlocked,
        )


class MonitoringLifecycleManager:
    def __init__(self, services: MonitoringServiceBundle, logger: logging.Logger) -> None:
        self.services = services
        self.logger = logger
        self._status_lock = threading.Lock()
        self._services_running = False
        self._monitoring_requested = False
        self._manual_pause = False
        self._paused_by_lock = False
        self._lifecycle_phase = "stopped"

    def attach_scheduler(self, scheduler: FlushScheduler) -> None:
        self.services.scheduler = scheduler

    def attach_session_monitor(self, session_monitor: SessionMonitor) -> None:
        self.services.session_monitor = session_monitor

    def start_monitoring(self) -> None:
        with self._status_lock:
            self._monitoring_requested = True
            self._manual_pause = False

        if not self._services_running:
            self.services.window_tracker.start()
            self.services.keyboard_capture.start()
            self.services.screenshot_capture.start()
            self.services.text_service.start()
            if self.services.scheduler is not None:
                self.services.scheduler.start()
            if self.services.session_monitor is not None:
                self.services.session_monitor.start()
            self._services_running = True

        active = self._apply_effective_monitoring_state()
        self._set_lifecycle_phase("idle")
        self.logger.info(
            "event=monitoring_state_change active=%s mode=start paused_by_lock=%s",
            active,
            self._paused_by_lock,
        )

    def pause_monitoring(self) -> None:
        with self._status_lock:
            self._manual_pause = True
            self._monitoring_requested = True
            self._paused_by_lock = False
        self._apply_effective_monitoring_state()
        self._set_lifecycle_phase("idle")
        self.logger.info("event=monitoring_state_change active=false mode=pause")

    def stop_monitoring(self) -> None:
        with self._status_lock:
            self._monitoring_requested = False
            self._manual_pause = False
            self._paused_by_lock = False

        self._apply_effective_monitoring_state()

        if self._services_running:
            if self.services.scheduler is not None:
                self.services.scheduler.stop()
            self.services.screenshot_capture.stop()
            self.services.keyboard_capture.stop()
            self.services.text_service.stop()
            self.services.window_tracker.stop()
            if self.services.session_monitor is not None:
                self.services.session_monitor.stop()
            self._services_running = False

        self._set_lifecycle_phase("stopped")
        self.logger.info("event=monitoring_state_change active=false mode=stop")

    def handle_session_locked(self) -> None:
        with self._status_lock:
            was_paused_by_lock = self._paused_by_lock
            self._paused_by_lock = True
            should_log_pause = self._monitoring_requested and not self._manual_pause and not was_paused_by_lock
        self._apply_effective_monitoring_state()
        if should_log_pause:
            self.logger.info("event=monitoring_paused_by_lock")

    def handle_session_unlocked(self) -> None:
        with self._status_lock:
            was_paused_by_lock = self._paused_by_lock
            self._paused_by_lock = False
            should_log_resume = was_paused_by_lock and self._monitoring_requested and not self._manual_pause
        active = self._apply_effective_monitoring_state()
        if should_log_resume and active:
            self.logger.info("event=monitoring_resumed_after_unlock")

    def snapshot(self) -> dict[str, bool | str]:
        with self._status_lock:
            return {
                "services_running": self._services_running,
                "monitoring_requested": self._monitoring_requested,
                "manual_pause": self._manual_pause,
                "paused_by_lock": self._paused_by_lock,
                "lifecycle_phase": self._lifecycle_phase,
            }

    @property
    def is_services_running(self) -> bool:
        with self._status_lock:
            return self._services_running

    @property
    def monitoring_requested(self) -> bool:
        with self._status_lock:
            return self._monitoring_requested

    @property
    def manual_pause(self) -> bool:
        with self._status_lock:
            return self._manual_pause

    @property
    def paused_by_lock(self) -> bool:
        with self._status_lock:
            return self._paused_by_lock

    def _apply_effective_monitoring_state(self) -> bool:
        with self._status_lock:
            target_active = self._monitoring_requested and not self._manual_pause and not self._paused_by_lock
        self.services.state.set_monitoring_active(target_active)
        if not target_active:
            self.services.window_tracker.pause()
        return target_active

    def set_draining(self) -> None:
        self._set_lifecycle_phase("draining")

    def set_idle(self) -> None:
        self._set_lifecycle_phase("idle")

    def lifecycle_phase(self) -> str:
        with self._status_lock:
            return self._lifecycle_phase

    def _set_lifecycle_phase(self, phase: str) -> None:
        with self._status_lock:
            self._lifecycle_phase = phase


class FlushCoordinator:
    def __init__(
        self,
        services: MonitoringServiceBundle,
        lifecycle_manager: MonitoringLifecycleManager,
        flush_interval_seconds: int,
        logger: logging.Logger,
    ) -> None:
        self.services = services
        self.lifecycle_manager = lifecycle_manager
        self.flush_interval_seconds = max(30, int(flush_interval_seconds))
        self.logger = logger
        self._flush_lock = threading.Lock()
        self._drain_cancel_event = threading.Event()
        self._status_lock = threading.Lock()
        self._drain_active = False
        self._drain_reason: str | None = None

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

    def snapshot(self) -> dict[str, bool | str | None]:
        with self._status_lock:
            return {
                "drain_active": self._drain_active,
                "drain_reason": self._drain_reason,
                "cancel_requested": self._drain_cancel_event.is_set(),
            }

    def flush_now(self, reason: str = "manual") -> FlushDrainResult | None:
        if self.services.shutdown_event.is_set():
            self.logger.info("event=summary_flush_skipped reason=shutdown request_reason=%s", reason)
            return None
        if not self._flush_lock.acquire(blocking=False):
            self.logger.info("event=summary_flush_skipped reason=already_running request_reason=%s", reason)
            return None
        if self.lifecycle_manager.paused_by_lock:
            self.logger.info("event=summary_flush_skipped reason=paused_by_lock request_reason=%s", reason)
            self._flush_lock.release()
            return None

        flush_started_at = time.perf_counter()
        self._drain_cancel_event.clear()
        self.services.summarizer.clear_unrecoverable_error()
        start_counts = self.services.storage.get_summary_job_status_counts()

        with self._status_lock:
            self._drain_active = True
            self._drain_reason = reason

        self.logger.info(
            "event=summary_drain_started reason=%s max_parallel_jobs=%s",
            reason,
            self.services.summarizer.get_runtime_status()["max_parallel_summary_jobs"],
        )

        stop_reason = "empty"

        try:
            self.lifecycle_manager.set_draining()
            self.services.keyboard_capture.flush_pending_events(reason=f"summary_flush:{reason}")
            while True:
                if self._drain_cancel_event.is_set():
                    cancelled = self.services.summarizer.cancel_queued_jobs(reason="cancelled_by_user")
                    self.logger.info("event=summary_drain_stopped reason=cancelled cancelled_queued_jobs=%s", cancelled)
                    stop_reason = "cancelled"
                    break

                for _ in range(5):
                    self.services.text_service.process_once(force_flush=True)
                    if self.services.storage.count_unprocessed_key_events() == 0:
                        break

                dispatched = self.services.summarizer.dispatch_pending_jobs(reason=reason)

                pending = self.services.storage.get_pending_counts()
                runtime = self.services.summarizer.get_runtime_status()

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
                    self.services.summarizer.cancel_queued_jobs(reason="cancelled_after_failure")
                    stop_reason = "error"
                    break

                backlog_remaining = _has_pending_backlog(pending)
                if not backlog_remaining and int(runtime["pending_summary_jobs"]) == 0:
                    stop_reason = "empty"
                    break

                self.services.summarizer.wait_for_activity(timeout_seconds=0.4)

            self.services.summarizer.wait_for_idle(timeout_seconds=30.0)

            now = time.time()
            self.services.state.set_flush_times(
                last_flush_ts=now,
                next_flush_ts=now + self.flush_interval_seconds,
            )

            end_counts = self.services.storage.get_summary_job_status_counts()
            result = FlushDrainResult(
                stop_reason=stop_reason,
                summaries_created=max(0, end_counts.get("succeeded", 0) - start_counts.get("succeeded", 0)),
                failed_jobs=max(0, end_counts.get("failed", 0) - start_counts.get("failed", 0)),
                cancelled_jobs=max(0, end_counts.get("cancelled", 0) - start_counts.get("cancelled", 0)),
                pending_summary_jobs=int(self.services.summarizer.get_runtime_status()["pending_summary_jobs"]),
            )
            if result.stop_reason != "error":
                self.services.error_notifier.resolve("flush_failure")
            duration_ms = (time.perf_counter() - flush_started_at) * 1000.0
            self.logger.info(
                (
                    "event=summary_drain_finished reason=%s stop_reason=%s duration_ms=%.3f created=%s failed=%s "
                    "cancelled=%s pending_summary_jobs=%s"
                ),
                reason,
                result.stop_reason,
                duration_ms,
                result.summaries_created,
                result.failed_jobs,
                result.cancelled_jobs,
                result.pending_summary_jobs,
            )
            return result
        except Exception as exc:
            self.services.error_notifier.notify(
                "flush_failure",
                "Flush failed: Unable to complete the flush. Check LM Studio and try again.",
                key=f"{reason}|{exc.__class__.__name__}",
            )
            duration_ms = (time.perf_counter() - flush_started_at) * 1000.0
            self.logger.exception("event=summary_drain_failed reason=%s error=%s", reason, exc)
            self.logger.info(
                "event=summary_drain_duration reason=%s stop_reason=error duration_ms=%.3f",
                reason,
                duration_ms,
            )
            return FlushDrainResult(
                stop_reason="error",
                summaries_created=0,
                failed_jobs=0,
                cancelled_jobs=0,
                pending_summary_jobs=int(self.services.summarizer.get_runtime_status()["pending_summary_jobs"]),
            )
        finally:
            with self._status_lock:
                self._drain_active = False
                self._drain_reason = None
            self.lifecycle_manager.set_idle()
            self._flush_lock.release()


class DiagnosticsService:
    """Expose runtime state and diagnostics snapshots to the UI layer."""

    def __init__(
        self,
        services: MonitoringServiceBundle,
        lifecycle_manager: MonitoringLifecycleManager,
        flush_coordinator: FlushCoordinator,
        logger: logging.Logger,
    ) -> None:
        self.services = services
        self.lifecycle_manager = lifecycle_manager
        self.flush_coordinator = flush_coordinator
        self.logger = logger

    def get_health_snapshot(self) -> dict[str, bool | str | None]:
        snapshot = self.services.state.snapshot()
        lifecycle = self.lifecycle_manager.snapshot()
        drain = self.flush_coordinator.snapshot()
        runtime = self.services.summarizer.get_runtime_status()
        return {
            "monitoring_active": snapshot.monitoring_active,
            "monitoring_requested": lifecycle["monitoring_requested"],
            "paused_by_lock": lifecycle["paused_by_lock"],
            "manual_pause": lifecycle["manual_pause"],
            "lifecycle_phase": lifecycle["lifecycle_phase"],
            "summary_running": int(runtime["running_jobs"]) > 0,
            "flush_drain_active": bool(drain["drain_active"]),
            "unrecoverable_summary_error": runtime["unrecoverable_error"],
        }

    def get_diagnostics_snapshot(self) -> dict:
        return self.services.storage.get_diagnostics_snapshot()

    def get_status(self) -> dict:
        snapshot = self.services.state.snapshot()
        pending = self.services.storage.get_pending_counts()
        summary_runtime = self.services.summarizer.get_runtime_status()
        lifecycle = self.lifecycle_manager.snapshot()
        drain = self.flush_coordinator.snapshot()

        monitoring_state = "Monitoring"
        if not snapshot.monitoring_active:
            monitoring_state = "Paused (PC locked)" if lifecycle["paused_by_lock"] else "Paused"

        buffer_state = _synthesize_buffer_state(
            pending_counts=pending,
            running_jobs=int(summary_runtime["running_jobs"]),
        )
        approx_batches_remaining = _estimate_remaining_batches(
            pending_counts=pending,
            pending_summary_jobs=int(summary_runtime["pending_summary_jobs"]),
            max_text_segments=max(1, self.services.batch_builder.max_text_segments),
            max_screenshots=max(1, self.services.batch_builder.max_screenshots),
        )

        return {
            "monitoring_active": snapshot.monitoring_active,
            "monitoring_requested": lifecycle["monitoring_requested"],
            "monitoring_state": monitoring_state,
            "paused_by_lock": lifecycle["paused_by_lock"],
            "manual_pause": lifecycle["manual_pause"],
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
            "flush_drain_active": bool(drain["drain_active"]),
            "flush_drain_reason": drain["drain_reason"],
            "flush_drain_cancel_requested": bool(drain["cancel_requested"]),
            "buffer_state": buffer_state,
            "approx_remaining_batches": approx_batches_remaining,
            "max_parallel_summary_jobs": int(summary_runtime["max_parallel_summary_jobs"]),
            "unrecoverable_summary_error": summary_runtime["unrecoverable_error"],
        }


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
    key_batches = 1 if pending_counts["key_events"] > 0 or pending_counts["processed_key_events"] > 0 else 0
    return max(pending_summary_jobs, text_batches + screenshot_batches + interval_batches + key_batches)
