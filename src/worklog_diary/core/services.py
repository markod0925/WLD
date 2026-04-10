from __future__ import annotations

import logging
import threading
import time

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
        )
        self.scheduler = FlushScheduler(
            interval_seconds=self.config.flush_interval_seconds,
            flush_callback=self.flush_now,
            state=self.state,
        )

        self._services_running = False
        self._flush_lock = threading.Lock()

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

    def flush_now(self, reason: str = "manual") -> int | None:
        if not self._flush_lock.acquire(blocking=False):
            self.logger.info("event=summary_flush_skipped reason=already_running request_reason=%s", reason)
            return None

        try:
            for _ in range(5):
                self.text_service.process_once(force_flush=True)
                if self.storage.count_unprocessed_key_events() == 0:
                    break
            summary_id = self.summarizer.flush_pending(reason=reason)
            now = time.time()
            self.state.set_flush_times(last_flush_ts=now, next_flush_ts=now + self.config.flush_interval_seconds)
            return summary_id
        finally:
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
        self.text_service.poll_interval_seconds = max(0.5, self.config.reconstruction_poll_interval_seconds)
        self.text_reconstructor.inactivity_gap_seconds = self.config.text_inactivity_gap_seconds
        self.batch_builder.max_text_segments = self.config.max_text_segments_per_summary
        self.batch_builder.max_screenshots = self.config.max_screenshots_per_summary
        self.lmstudio_client.base_url = self.config.lmstudio_base_url.rstrip("/")
        self.lmstudio_client.model = self.config.lmstudio_model
        self.lmstudio_client.timeout_seconds = self.config.request_timeout_seconds
        self.scheduler.interval_seconds = max(30, self.config.flush_interval_seconds)

        if was_monitoring:
            self.start_monitoring()

    def get_status(self) -> dict:
        snapshot = self.state.snapshot()
        pending = self.storage.get_pending_counts()
        return {
            "monitoring_active": snapshot.monitoring_active,
            "blocked": snapshot.blocked,
            "foreground": snapshot.foreground_info,
            "active_interval_id": snapshot.active_interval_id,
            "last_flush_ts": snapshot.last_flush_ts,
            "next_flush_ts": snapshot.next_flush_ts,
            "pending": pending,
        }

    def shutdown(self) -> None:
        self.stop_monitoring()
        self.storage.close()
