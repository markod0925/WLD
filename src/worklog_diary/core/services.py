from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date

from .config import (
    AppConfig,
    app_data_dir_source,
    is_frozen_executable,
    safe_config_diff,
    safe_config_snapshot,
    save_config,
)
from .crash_monitor import CrashMonitor
from .errors import LMStudioConnectionError, LMStudioServiceUnavailableError, LMStudioTimeoutError
from .logging_setup import configure_logging
from .lmstudio_logging import get_failed_stage
from .monitoring_components import (
    DiagnosticsService,
    FlushCoordinator,
    FlushDrainResult,
    MonitoringLifecycleManager,
    MonitoringServiceBundle,
    ServiceRegistry,
)
from .summary_dedup import SummaryDeduplicator
from .semantic_coalescing import SemanticCoalescingConfig
from .. import __version__


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
        self.logger.info("event=config_loaded path=%s", self.config.config_path)
        self.logger.info("event=config_snapshot %s", _format_kv(safe_config_snapshot(self.config)))
        self.logger.info("event=runtime_paths_source source=%s", app_data_dir_source())

        self.crash_reporter = CrashMonitor(self.config.app_data_dir, self.config.log_dir, self.logger)
        self.crash_reporter.install(app_version=__version__)

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
        self._shutdown_completed = False

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
        previous_config = self.config
        self.logger.info("event=config_apply_start")
        lifecycle_snapshot = self.lifecycle_manager.snapshot()
        was_monitoring_requested = lifecycle_snapshot["monitoring_requested"]
        try:
            if lifecycle_snapshot["services_running"]:
                self.stop_monitoring()

            self.config = config
            self.config.ensure_paths()
            save_config(self.config, self.config.config_path)

            self.privacy.update_blocked_processes(self.config.blocked_processes)
            self.window_tracker.poll_interval_seconds = max(0.2, self.config.foreground_poll_interval_seconds)
            self.screenshot_capture.interval_seconds = max(5, self.config.screenshot_interval_seconds)
            self.screenshot_capture.capture_mode = self.config.capture_mode
            self.screenshot_capture._dedup_state.exact_hash_enabled = self.config.screenshot_dedup_exact_hash_enabled
            self.screenshot_capture._dedup_state.perceptual_hash_enabled = self.config.screenshot_dedup_perceptual_hash_enabled
            self.screenshot_capture._dedup_state.phash_threshold = self.config.screenshot_dedup_phash_threshold
            self.screenshot_capture._dedup_state.ssim_enabled = self.config.screenshot_dedup_ssim_enabled
            self.screenshot_capture._dedup_state.ssim_threshold = self.config.screenshot_dedup_ssim_threshold
            self.screenshot_capture._dedup_state.compare_recent_count = self.config.screenshot_dedup_compare_recent_count
            self.screenshot_capture._dedup_state.min_interval_same_visual_context_seconds = (
                self.config.screenshot_min_keep_interval_seconds
            )
            self.screenshot_capture._dedup_state._trim_history()
            self.screenshot_capture._dedup_resize_width = max(8, self.config.screenshot_dedup_resize_width)
            self.text_service.poll_interval_seconds = max(0.5, self.config.reconstruction_poll_interval_seconds)
            self.text_reconstructor.inactivity_gap_seconds = self.config.text_inactivity_gap_seconds
            self.batch_builder.max_text_segments = self.config.max_text_segments_per_summary
            self.batch_builder.max_screenshots = self.config.max_screenshots_per_summary
            self.batch_builder.dedup_enabled = self.config.screenshot_dedup_enabled
            self.batch_builder.dedup_threshold = self.config.screenshot_dedup_threshold
            self.batch_builder.min_keep_interval_seconds = self.config.screenshot_min_keep_interval_seconds
            self.batch_builder.activity_segment_min_duration_seconds = self.config.activity_segment_min_duration_seconds
            self.batch_builder.activity_segment_max_duration_seconds = self.config.activity_segment_max_duration_seconds
            self.batch_builder.activity_segment_idle_gap_seconds = self.config.activity_segment_idle_gap_seconds
            self.batch_builder.activity_segment_title_similarity_threshold = (
                self.config.activity_segment_title_similarity_threshold
            )
            self.batch_builder.activity_segment_screenshot_phash_threshold = self.config.screenshot_dedup_phash_threshold
            self.batch_builder.activity_segment_screenshot_ssim_threshold = self.config.screenshot_dedup_ssim_threshold
            self.lmstudio_client.base_url = self.config.lmstudio_base_url.rstrip("/")
            self.lmstudio_client.model = self.config.lmstudio_model
            self.lmstudio_client.timeout_seconds = self.config.request_timeout_seconds
            self.lmstudio_client.daily_timeout_seconds = max(
                self.config.request_timeout_seconds * 2,
                self.config.request_timeout_seconds,
            )
            self.lmstudio_client.prompt_builder.max_prompt_chars = self.config.lmstudio_max_prompt_chars
            self.summarizer.summary_deduplicator = SummaryDeduplicator(
                suppress_threshold=self.config.summary_similarity_suppress_threshold,
                merge_threshold=self.config.summary_similarity_merge_threshold,
                cooldown_seconds=self.config.summary_cooldown_seconds,
                recent_compare_count=self.config.recent_summary_compare_count,
            )
            if self.summarizer.semantic_coalescer is not None:
                self.summarizer.semantic_coalescer.engine.config = SemanticCoalescingConfig(
                    enabled=self.config.semantic_coalescing_enabled,
                    embedding_base_url=self.config.semantic_embedding_base_url,
                    embedding_model=self.config.semantic_embedding_model,
                    max_candidate_gap_seconds=self.config.semantic_max_candidate_gap_seconds,
                    max_neighbor_count=self.config.semantic_max_neighbor_count,
                    min_cosine_similarity=self.config.semantic_min_cosine_similarity,
                    min_merge_score=self.config.semantic_min_merge_score,
                    same_app_boost=self.config.semantic_same_app_boost,
                    window_title_boost=self.config.semantic_window_title_boost,
                    keyword_overlap_boost=self.config.semantic_keyword_overlap_boost,
                    temporal_gap_penalty_weight=self.config.semantic_temporal_gap_penalty_weight,
                    app_switch_penalty=self.config.semantic_app_switch_penalty,
                    lock_boundary_blocks_merge=self.config.semantic_lock_boundary_blocks_merge,
                    pause_boundary_blocks_merge=self.config.semantic_pause_boundary_blocks_merge,
                    transition_keywords=list(self.config.semantic_transition_keywords),
                    store_merge_diagnostics=self.config.semantic_store_merge_diagnostics,
                    recompute_missing_embeddings_on_startup=self.config.semantic_recompute_missing_embeddings_on_startup,
                )
                self.summarizer.semantic_coalescer.diagnostics_enabled = self.config.semantic_store_merge_diagnostics
                embedding_client = self.summarizer.semantic_coalescer.engine.embedding_provider.client
                embedding_client.base_url = self.config.semantic_embedding_base_url.rstrip("/")
                embedding_client.model = self.config.semantic_embedding_model
            self.summarizer.update_max_parallel_jobs(self.config.max_parallel_summary_jobs)
            self.summarizer.set_process_backlog_only_while_locked(self.config.process_backlog_only_while_locked)
            self.scheduler.interval_seconds = max(30, self.config.flush_interval_seconds)
            self.flush_coordinator.flush_interval_seconds = max(30, self.config.flush_interval_seconds)
            changes = safe_config_diff(previous_config, self.config)
            for key, values in changes.items():
                self.logger.info("event=config_apply_diff key=%s old=%s new=%s", key, values[0], values[1])

            if was_monitoring_requested:
                self.start_monitoring()
            self.logger.info("event=config_apply_complete changed_count=%s", len(changes))
            self.logger.info("event=config_snapshot %s", _format_kv(safe_config_snapshot(self.config)))
        except Exception as exc:
            self.logger.exception(
                "event=config_apply_failed error_type=%s error=%s",
                exc.__class__.__name__,
                exc,
            )
            raise

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
        except LMStudioTimeoutError:
            raise
        except Exception as exc:
            self.error_notifier.notify(
                "summary_generation_failure",
                "Summary generation failed. The daily recap could not be created.",
                key=f"{day_key}|{exc.__class__.__name__}",
            )
            self.logger.exception(
                "event=daily_recap_generation_failed day=%s failed_stage=%s error_type=%s error=%s",
                day_key,
                get_failed_stage(exc, default="unknown"),
                exc.__class__.__name__,
                exc,
            )
            raise

    def shutdown(self) -> None:
        if self._shutdown_completed:
            return
        self._shutdown_completed = True
        first_error: Exception | None = None
        session_id = getattr(self.crash_reporter, "session_id", None)
        if session_id is not None:
            self.logger.info("event=shutdown_start session_id=%s", session_id)
        else:
            self.logger.info("event=shutdown_start")
        mark_shutdown_start = getattr(self.crash_reporter, "mark_shutdown_start", None)
        if callable(mark_shutdown_start):
            mark_shutdown_start()
        self._services.shutdown_event.set()
        shutdown_steps: list[tuple[str, Callable[[], None]]] = [
            ("summary_admission_stop", lambda: self.summarizer.stop_accepting_new_jobs()),
            ("scheduler_stop", lambda: self.scheduler.stop()),
            ("scheduler_stop_log", lambda: self.logger.info("event=summary_scheduler_stopped")),
            ("summary_drain_cancel", lambda: self.cancel_flush_drain()),
            ("summary_workers_stop", lambda: self.logger.info("event=summary_workers_draining_or_cancelled")),
            ("summary_workers_join", lambda: self.summarizer.stop()),
            ("summary_queue_state", lambda: self.logger.info("event=summary_queue_final_state %s", _format_kv(self.summarizer.get_runtime_status()))),
            ("capture_stop", lambda: self.window_tracker.stop()),
            ("keyboard_stop", lambda: self.keyboard_capture.stop()),
            ("text_stop", lambda: self.text_service.stop()),
            ("screenshot_stop", lambda: self.screenshot_capture.stop()),
            ("session_monitor_stop", lambda: self.session_monitor.stop() if self.session_monitor is not None else None),
            ("monitors_stopped_log", lambda: self.logger.info("event=monitors_stopped")),
            ("crash_finalize", lambda: self.crash_reporter.mark_clean_exit()),
            ("storage_close", lambda: self._close_storage_with_log()),
        ]
        for step_name, step in shutdown_steps:
            try:
                step()
            except Exception as exc:
                if first_error is None:
                    first_error = exc
                self.logger.exception(
                    "event=services_shutdown_step_failed step=%s error_type=%s error=%s",
                    step_name,
                    exc.__class__.__name__,
                    exc,
                )
        self.logger.info("event=shutdown_complete")
        if first_error is not None:
            raise first_error

    def _close_storage_with_log(self) -> None:
        self.storage.close()
        self.logger.info("event=storage_closed")


def _format_kv(data: dict[str, object]) -> str:
    return " ".join(f"{key}={value}" for key, value in sorted(data.items()))
