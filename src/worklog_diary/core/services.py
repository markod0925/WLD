from __future__ import annotations

import logging
import math
import threading
import time
from dataclasses import dataclass
from datetime import date
from collections.abc import Callable

from .batching import BatchBuilder
from .config import AppConfig, app_data_dir_source, is_frozen_executable, save_config
from .error_notifications import ErrorNotificationManager
from .errors import LMStudioConnectionError, LMStudioServiceUnavailableError
from .keyboard_capture import KeyboardCaptureService
from .lmstudio_client import LMStudioClient
from .logging_setup import configure_logging
from .models import SharedState
from .privacy import PrivacyPolicyEngine
from .scheduler import FlushScheduler
from .session_monitor import SessionMonitor
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

        configure_logging(self.config.log_dir)
        self.logger = logging.getLogger(__name__)
        self.logger.info(
            (
                "event=runtime_paths mode=%s app_data_dir=%s log_dir=%s screenshot_dir=%s "
                "db_path=%s config_path=%s"
            ),
            "portable" if is_frozen_executable() else "dev",
            self.config.app_data_dir,
            self.config.log_dir,
            self.config.screenshot_dir,
            self.config.db_path,
            self.config.config_path,
        )
        self.logger.info("event=runtime_paths_source source=%s", app_data_dir_source())

        self.state = SharedState()
        self.storage = SQLiteStorage(self.config.db_path)
        self.privacy = PrivacyPolicyEngine(set(self.config.blocked_processes))
        self.error_notifier = ErrorNotificationManager()

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
            state=self.state,
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
            error_notifier=self.error_notifier,
        )
        self.scheduler = FlushScheduler(
            interval_seconds=self.config.flush_interval_seconds,
            flush_callback=self.flush_now,
            state=self.state,
        )
        self.session_monitor = SessionMonitor(
            on_locked=self.handle_session_locked,
            on_unlocked=self.handle_session_unlocked,
        )

        self._services_running = False
        self._flush_lock = threading.Lock()
        self._drain_cancel_event = threading.Event()
        self._status_lock = threading.Lock()
        self._drain_active = False
        self._drain_reason: str | None = None
        self._monitoring_requested = False
        self._manual_pause = False
        self._paused_by_lock = False

    def set_error_notification_sink(self, sink: Callable[[str, str], None] | None) -> None:
        self.error_notifier.set_sink(sink)

    def notify_user_error(self, category: str, message: str, *, key: str | None = None) -> bool:
        return self.error_notifier.notify(category, message, key=key)

    def start_monitoring(self) -> None:
        with self._status_lock:
            self._monitoring_requested = True
            self._manual_pause = False

        if not self._services_running:
            self.window_tracker.start()
            self.keyboard_capture.start()
            self.screenshot_capture.start()
            self.text_service.start()
            self.scheduler.start()
            self.session_monitor.start()
            self._services_running = True

        active = self._apply_effective_monitoring_state()
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
        self.logger.info("event=monitoring_state_change active=false mode=pause")

    def stop_monitoring(self) -> None:
        with self._status_lock:
            self._monitoring_requested = False
            self._manual_pause = False
            self._paused_by_lock = False

        self._apply_effective_monitoring_state()

        if self._services_running:
            self.scheduler.stop()
            self.screenshot_capture.stop()
            self.keyboard_capture.stop()
            self.text_service.stop()
            self.window_tracker.stop()
            self.session_monitor.stop()
            self._services_running = False

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

    def _apply_effective_monitoring_state(self) -> bool:
        with self._status_lock:
            target_active = self._monitoring_requested and not self._manual_pause and not self._paused_by_lock
        self.state.set_monitoring_active(target_active)
        if not target_active:
            self.window_tracker.pause()
        return target_active

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
        status_lock = getattr(self, "_status_lock", None)
        if status_lock is None:
            paused_by_lock = False
        else:
            with status_lock:
                paused_by_lock = bool(getattr(self, "_paused_by_lock", False))
        if paused_by_lock:
            self.logger.info("event=summary_flush_skipped reason=paused_by_lock request_reason=%s", reason)
            self._flush_lock.release()
            return None

        flush_started_at = time.perf_counter()
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
                if result.stop_reason != "error":
                    self.error_notifier.resolve("flush_failure")
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
                self.error_notifier.notify(
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
                    pending_summary_jobs=int(self.summarizer.get_runtime_status()["pending_summary_jobs"]),
                )
        finally:
            with self._status_lock:
                self._drain_active = False
                self._drain_reason = None
            self._flush_lock.release()

    def apply_config(self, config: AppConfig) -> None:
        with self._status_lock:
            was_monitoring_requested = self._monitoring_requested

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

        if was_monitoring_requested:
            self.start_monitoring()

    def get_status(self) -> dict:
        snapshot = self.state.snapshot()
        pending = self.storage.get_pending_counts()
        summary_runtime = self.summarizer.get_runtime_status()
        with self._status_lock:
            drain_active = self._drain_active
            drain_reason = self._drain_reason
            paused_by_lock = self._paused_by_lock
            manual_pause = self._manual_pause
            monitoring_requested = self._monitoring_requested

        monitoring_state = "Monitoring"
        if not snapshot.monitoring_active:
            monitoring_state = "Paused (PC locked)" if paused_by_lock else "Paused"

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
            "monitoring_requested": monitoring_requested,
            "monitoring_state": monitoring_state,
            "paused_by_lock": paused_by_lock,
            "manual_pause": manual_pause,
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

    def generate_daily_recap(self, day: date) -> dict[str, int | str | bool]:
        day_key = day.isoformat()
        self.logger.info("event=daily_recap_generation_started day=%s", day_key)
        try:
            summary_id, replaced = self.summarizer.generate_daily_recap_for_day(day)
            stored = self.storage.get_daily_summary_for_day(day)
            source_batch_count = (
                stored.source_batch_count if stored is not None else self.storage.count_batch_summaries_for_day(day)
            )
            self.logger.info(
                "event=daily_recap_generation_succeeded day=%s daily_summary_id=%s source_batch_count=%s replaced=%s",
                day_key,
                summary_id,
                source_batch_count,
                replaced,
            )
            if replaced:
                self.logger.info("event=daily_recap_replaced day=%s", day_key)
            self.error_notifier.resolve_many(
                "summary_generation_failure",
                "lmstudio_connection",
                "lmstudio_service_unavailable",
            )
            return {
                "day": day_key,
                "daily_summary_id": summary_id,
                "source_batch_count": int(source_batch_count),
                "replaced": bool(replaced),
            }
        except ValueError:
            raise
        except LMStudioConnectionError:
            raise
        except LMStudioServiceUnavailableError:
            raise
        except Exception as exc:
            self.error_notifier.notify(
                "summary_generation_failure",
                "Summary generation failed. The daily recap could not be created.",
                key=f"{day_key}|{exc.__class__.__name__}",
            )
            self.logger.exception("event=daily_recap_generation_failed day=%s error=%s", day_key, exc)
            raise

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
