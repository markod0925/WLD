from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date

from .config import AppConfig, app_data_dir_source, is_frozen_executable, save_config
from .errors import LMStudioConnectionError, LMStudioServiceUnavailableError
from .logging_setup import configure_logging
from .monitoring_components import (
    DiagnosticsService,
    FlushCoordinator,
    FlushDrainResult,
    MonitoringLifecycleManager,
    MonitoringServiceBundle,
    ServiceRegistry,
)


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

        self.registry = ServiceRegistry(self.config)
        self._services: MonitoringServiceBundle = self.registry.build_bundle()

        self.state = self._services.state
        self.storage = self._services.storage
        self.privacy = self._services.privacy
        self.error_notifier = self._services.error_notifier
        self.window_tracker = self._services.window_tracker
        self.keyboard_capture = self._services.keyboard_capture
        self.screenshot_capture = self._services.screenshot_capture
        self.text_reconstructor = self._services.text_reconstructor
        self.text_service = self._services.text_service
        self.batch_builder = self._services.batch_builder
        self.lmstudio_client = self._services.lmstudio_client
        self.summarizer = self._services.summarizer

        self.lifecycle_manager = MonitoringLifecycleManager(self._services, self.logger)
        self.flush_coordinator = FlushCoordinator(
            self._services,
            self.lifecycle_manager,
            self.config.flush_interval_seconds,
            self.logger,
        )
        self.scheduler = self.registry.build_scheduler(
            flush_callback=self.flush_coordinator.flush_now,
            state=self.state,
            shutdown_event=self._services.shutdown_event,
        )
        self._services.scheduler = self.scheduler
        self.lifecycle_manager.attach_scheduler(self.scheduler)

        self.session_monitor = self.registry.build_session_monitor(
            on_locked=self.lifecycle_manager.handle_session_locked,
            on_unlocked=self.lifecycle_manager.handle_session_unlocked,
        )
        self._services.session_monitor = self.session_monitor
        self.lifecycle_manager.attach_session_monitor(self.session_monitor)

        self.diagnostics_service = DiagnosticsService(
            self._services,
            self.lifecycle_manager,
            self.flush_coordinator,
            self.logger,
        )

    def set_error_notification_sink(self, sink: Callable[[str, str], None] | None) -> None:
        self.error_notifier.set_sink(sink)

    def notify_user_error(self, category: str, message: str, *, key: str | None = None) -> bool:
        return self.error_notifier.notify(category, message, key=key)

    def start_monitoring(self) -> None:
        self.lifecycle_manager.start_monitoring()

    def pause_monitoring(self) -> None:
        self.lifecycle_manager.pause_monitoring()

    def stop_monitoring(self) -> None:
        self.lifecycle_manager.stop_monitoring()

    def handle_session_locked(self) -> None:
        self.lifecycle_manager.handle_session_locked()

    def handle_session_unlocked(self) -> None:
        self.lifecycle_manager.handle_session_unlocked()

    def cancel_flush_drain(self) -> bool:
        return self.flush_coordinator.cancel_flush_drain()

    @property
    def is_drain_active(self) -> bool:
        return self.flush_coordinator.is_drain_active

    def flush_now(self, reason: str = "manual") -> FlushDrainResult | None:
        return self.flush_coordinator.flush_now(reason)

    def apply_config(self, config: AppConfig) -> None:
        lifecycle_snapshot = self.lifecycle_manager.snapshot()
        was_monitoring_requested = lifecycle_snapshot["monitoring_requested"]

        if lifecycle_snapshot["services_running"]:
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
        self.batch_builder.dedup_enabled = self.config.screenshot_dedup_enabled
        self.batch_builder.dedup_threshold = self.config.screenshot_dedup_threshold
        self.batch_builder.min_keep_interval_seconds = self.config.screenshot_min_keep_interval_seconds
        self.lmstudio_client.base_url = self.config.lmstudio_base_url.rstrip("/")
        self.lmstudio_client.model = self.config.lmstudio_model
        self.lmstudio_client.timeout_seconds = self.config.request_timeout_seconds
        self.summarizer.update_max_parallel_jobs(self.config.max_parallel_summary_jobs)
        self.scheduler.interval_seconds = max(30, self.config.flush_interval_seconds)
        self.flush_coordinator.flush_interval_seconds = max(30, self.config.flush_interval_seconds)

        if was_monitoring_requested:
            self.start_monitoring()

    def get_status(self) -> dict:
        return self.diagnostics_service.get_status()

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
        self._services.shutdown_event.set()
        self.cancel_flush_drain()
        self.stop_monitoring()
        self.summarizer.stop()
        self.storage.close()
