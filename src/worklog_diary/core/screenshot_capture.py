from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from pathlib import Path

import mss
import mss.tools

from .models import ScreenshotRecord, SharedState
from .storage import SQLiteStorage


class ScreenshotCaptureService:
    def __init__(
        self,
        storage: SQLiteStorage,
        state: SharedState,
        screenshot_dir: str,
        interval_seconds: int,
    ) -> None:
        self.storage = storage
        self.state = state
        self.screenshot_dir = Path(screenshot_dir)
        self.interval_seconds = max(5, interval_seconds)
        self.logger = logging.getLogger(__name__)

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
            return False
        if snapshot.blocked:
            return False
        if snapshot.foreground_info is None:
            return False

        ts = time.time()
        filename = datetime.fromtimestamp(ts).strftime("%Y%m%d_%H%M%S_%f") + ".png"
        file_path = self.screenshot_dir / filename

        with mss.mss() as sct:
            image = sct.grab(sct.monitors[0])
            mss.tools.to_png(image.rgb, image.size, output=str(file_path))

        record = ScreenshotRecord(
            id=None,
            ts=ts,
            file_path=str(file_path),
            process_name=snapshot.foreground_info.process_name,
            window_title=snapshot.foreground_info.window_title,
            active_interval_id=snapshot.active_interval_id,
        )
        self.storage.insert_screenshot(record)
        return True

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.capture_once()
            except Exception as exc:
                self.logger.exception("Screenshot capture failed: %s", exc)
            finally:
                self._stop_event.wait(self.interval_seconds)
