from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import mss
    import mss.tools
except Exception as exc:  # pragma: no cover - optional dependency
    mss = None
    MSS_IMPORT_ERROR = exc
else:  # pragma: no cover - import success depends on environment
    MSS_IMPORT_ERROR = None

from .models import ScreenshotRecord, SharedState
from .privacy import PrivacyPolicyEngine
from .screenshot_dedup import compute_screenshot_fingerprint
from .storage import SQLiteStorage
from .window_tracker import get_foreground_window_info, get_window_capture_rect


class ScreenshotCaptureService:
    """Capture screenshots for the active foreground window on a background loop."""

    def __init__(
        self,
        storage: SQLiteStorage,
        state: SharedState,
        privacy: PrivacyPolicyEngine,
        screenshot_dir: str,
        interval_seconds: int,
        capture_mode: str = "active_window",
        foreground_provider: Callable[[], Any] = get_foreground_window_info,
        window_rect_provider: Callable[[int], tuple[int, int, int, int] | None] = get_window_capture_rect,
        shutdown_event: threading.Event | None = None,
    ) -> None:
        self.storage = storage
        self.state = state
        self.privacy = privacy
        self.screenshot_dir = Path(screenshot_dir)
        self.interval_seconds = max(5, interval_seconds)
        self.capture_mode = _normalize_capture_mode(capture_mode)
        self.foreground_provider = foreground_provider
        self.window_rect_provider = window_rect_provider
        self.logger = logging.getLogger(__name__)
        self._capture_backend_missing_logged = False

        self._shutdown_event = shutdown_event or threading.Event()
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

        if mss is None:
            if not self._capture_backend_missing_logged:
                self.logger.warning("Screenshot capture backend unavailable: %s", MSS_IMPORT_ERROR)
                self._capture_backend_missing_logged = True
            return False

        with mss.mss() as sct:
            full_monitor = _monitor_region(sct.monitors[0])
            window_rect = self.window_rect_provider(current_info.hwnd) if self.capture_mode == "active_window" else None
            capture_region = resolve_capture_region(self.capture_mode, full_monitor, window_rect)
            if capture_region is None:
                self.logger.info(
                    "event=screenshot_skipped reason=invalid_capture_region mode=%s process=%s hwnd=%s",
                    self.capture_mode,
                    current_info.process_name,
                    current_info.hwnd,
                )
                return False

            image = sct.grab(capture_region)
            fingerprint = compute_screenshot_fingerprint(image.rgb, image.size)
            mss.tools.to_png(image.rgb, image.size, output=str(file_path))

        record = ScreenshotRecord(
            id=None,
            ts=ts,
            file_path=str(file_path),
            process_name=current_info.process_name,
            window_title=current_info.window_title,
            active_interval_id=snapshot.active_interval_id,
            window_hwnd=current_info.hwnd,
            fingerprint=fingerprint,
        )
        self.storage.insert_screenshot(record)
        self.logger.info(
            "event=screenshot_captured file=%s process=%s title=%s interval_id=%s mode=%s",
            file_path,
            current_info.process_name,
            current_info.window_title,
            snapshot.active_interval_id,
            self.capture_mode,
        )
        return True

    def _run(self) -> None:
        while not self._stop_event.is_set() and not self._shutdown_event.is_set():
            should_stop = False
            try:
                self.capture_once()
            except Exception as exc:
                self.logger.exception("Screenshot capture failed: %s", exc)
            finally:
                should_stop = self._stop_event.wait(self.interval_seconds) or self._shutdown_event.is_set()
            if should_stop:
                break



def resolve_capture_region(
    capture_mode: str,
    full_monitor: dict[str, int],
    window_rect: tuple[int, int, int, int] | None,
) -> dict[str, int] | None:
    normalized_mode = _normalize_capture_mode(capture_mode)
    if normalized_mode == "full_screen":
        return full_monitor

    if window_rect is None:
        return None

    left, top, right, bottom = window_rect
    if right <= left or bottom <= top:
        return None

    monitor_left = int(full_monitor["left"])
    monitor_top = int(full_monitor["top"])
    monitor_right = monitor_left + int(full_monitor["width"])
    monitor_bottom = monitor_top + int(full_monitor["height"])

    clipped_left = max(left, monitor_left)
    clipped_top = max(top, monitor_top)
    clipped_right = min(right, monitor_right)
    clipped_bottom = min(bottom, monitor_bottom)

    if clipped_right <= clipped_left or clipped_bottom <= clipped_top:
        return None

    return {
        "left": clipped_left,
        "top": clipped_top,
        "width": clipped_right - clipped_left,
        "height": clipped_bottom - clipped_top,
    }



def _monitor_region(monitor: dict[str, int]) -> dict[str, int]:
    return {
        "left": int(monitor["left"]),
        "top": int(monitor["top"]),
        "width": int(monitor["width"]),
        "height": int(monitor["height"]),
    }



def _normalize_capture_mode(capture_mode: str) -> str:
    value = capture_mode.strip().lower()
    if value == "active_window":
        return "active_window"
    if value == "full_screen":
        return "full_screen"
    return "active_window"
