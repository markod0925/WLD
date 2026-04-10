from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import ScreenshotRecord, SharedState
from .privacy import PrivacyPolicyEngine
from .storage import SQLiteStorage
from .window_tracker import get_foreground_window_info


class ScreenshotCaptureService:
    def __init__(
        self,
        storage: SQLiteStorage,
        state: SharedState,
        privacy: PrivacyPolicyEngine,
        screenshot_dir: str,
        interval_seconds: int,
        foreground_provider: Callable[[], Any] = get_foreground_window_info,
    ) -> None:
        self.storage = storage
        self.state = state
        self.privacy = privacy
        self.screenshot_dir = Path(screenshot_dir)
        self.interval_seconds = max(5, interval_seconds)
        self.foreground_provider = foreground_provider
        self.logger = logging.getLogger(__name__)
        self._capture_backend_missing_logged = False

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self.screenshot_dir.mkdir(parents=True, exist_ok=True)
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="ScreenshotCapture", daemon=True)
        self._thread.start()
        self.logger.info("Screenshot capture service started")

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        self.logger.info("Screenshot capture service stopped")

    def capture_once(self) -> bool:
        snapshot = self.state.snapshot()
        if not snapshot.monitoring_active:
            self.logger.debug("event=screenshot_skipped reason=monitoring_inactive")
            return False
        if snapshot.blocked:
            self.logger.debug("event=screenshot_skipped reason=state_blocked")
            return False
        if snapshot.foreground_info is None:
            self.logger.debug("event=screenshot_skipped reason=missing_foreground")
            return False
        if snapshot.active_interval_id is None:
            self.logger.debug("event=screenshot_skipped reason=missing_interval")
            return False

        current_info = self.foreground_provider()
        blocked_now = self.privacy.is_blocked(current_info.process_name)
        matches_state = (
            snapshot.foreground_info.hwnd == current_info.hwnd
            and snapshot.foreground_info.pid == current_info.pid
        )
        if blocked_now:
            self.logger.info(
                "event=screenshot_skipped reason=blocked process=%s title=%s",
                current_info.process_name,
                current_info.window_title,
            )
            return False
        if not matches_state:
            self.logger.info(
                (
                    "event=screenshot_skipped reason=foreground_mismatch "
                    "state_process=%s state_hwnd=%s current_process=%s current_hwnd=%s"
                ),
                snapshot.foreground_info.process_name,
                snapshot.foreground_info.hwnd,
                current_info.process_name,
                current_info.hwnd,
            )
            return False

        ts = time.time()
        filename = datetime.fromtimestamp(ts).strftime("%Y%m%d_%H%M%S_%f") + ".png"
        file_path = self.screenshot_dir / filename

        try:
            import mss
            import mss.tools
        except Exception as exc:
            if not self._capture_backend_missing_logged:
                self.logger.warning("Screenshot capture backend unavailable: %s", exc)
                self._capture_backend_missing_logged = True
            return False

        with mss.mss() as sct:
            image = sct.grab(sct.monitors[0])
            mss.tools.to_png(image.rgb, image.size, output=str(file_path))

        record = ScreenshotRecord(
            id=None,
            ts=ts,
            file_path=str(file_path),
            process_name=current_info.process_name,
            window_title=current_info.window_title,
            active_interval_id=snapshot.active_interval_id,
        )
        self.storage.insert_screenshot(record)
        self.logger.info(
            "event=screenshot_captured file=%s process=%s title=%s interval_id=%s",
            file_path,
            current_info.process_name,
            current_info.window_title,
            snapshot.active_interval_id,
        )
        return True

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.capture_once()
            except Exception as exc:
                self.logger.exception("Screenshot capture failed: %s", exc)
            finally:
                self._stop_event.wait(self.interval_seconds)
