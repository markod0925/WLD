from __future__ import annotations

import ctypes
import logging
import os
import threading
import time
from ctypes import wintypes
from pathlib import Path

from .models import ForegroundInfo, SharedState
from .privacy import PrivacyPolicyEngine
from .storage import SQLiteStorage

PROCESS_QUERY_LIMITED_INFORMATION = 0x1000


class ForegroundWindowTrackerService:
    def __init__(
        self,
        storage: SQLiteStorage,
        privacy: PrivacyPolicyEngine,
        state: SharedState,
        poll_interval_seconds: float,
    ) -> None:
        self.storage = storage
        self.privacy = privacy
        self.state = state
        self.poll_interval_seconds = max(0.2, poll_interval_seconds)
        self.logger = logging.getLogger(__name__)

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._current_signature: tuple[int, int, str, str] | None = None
        self._current_interval_id: int | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="ForegroundTracker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        self._close_current_interval_if_needed()

    def pause(self) -> None:
        self._close_current_interval_if_needed()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                snapshot = self.state.snapshot()
                if not snapshot.monitoring_active:
                    self._close_current_interval_if_needed()
                    time.sleep(self.poll_interval_seconds)
                    continue

                info = get_foreground_window_info()
                blocked = self.privacy.is_blocked(info.process_name)
                signature = (info.hwnd, info.pid, info.process_name, info.window_title)

                if signature != self._current_signature:
                    self._close_current_interval_if_needed(end_ts=info.timestamp)
                    self._current_interval_id = self.storage.start_interval(info, blocked)
                    self._current_signature = signature

                self.state.update_foreground(info, blocked, self._current_interval_id)
            except Exception as exc:
                self.logger.exception("Foreground tracker loop error: %s", exc)
            finally:
                time.sleep(self.poll_interval_seconds)

    def _close_current_interval_if_needed(self, end_ts: float | None = None) -> None:
        if self._current_interval_id is None:
            return
        close_ts = end_ts or time.time()
        try:
            self.storage.close_interval(self._current_interval_id, close_ts)
        finally:
            self._current_signature = None
            self._current_interval_id = None
            self.state.update_foreground(None, False, None)



def get_foreground_window_info() -> ForegroundInfo:
    now = time.time()
    if os.name != "nt":
        return ForegroundInfo(
            timestamp=now,
            hwnd=0,
            pid=0,
            process_name="unsupported.exe",
            window_title="Unsupported platform",
        )

    user32 = ctypes.windll.user32
    hwnd = int(user32.GetForegroundWindow())
    if hwnd == 0:
        return ForegroundInfo(
            timestamp=now,
            hwnd=0,
            pid=0,
            process_name="unknown.exe",
            window_title="",
        )

    title_length = int(user32.GetWindowTextLengthW(hwnd))
    title_buffer = ctypes.create_unicode_buffer(title_length + 1)
    user32.GetWindowTextW(hwnd, title_buffer, title_length + 1)
    window_title = title_buffer.value

    pid = wintypes.DWORD(0)
    user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
    process_name = _get_process_name(int(pid.value))

    return ForegroundInfo(
        timestamp=now,
        hwnd=hwnd,
        pid=int(pid.value),
        process_name=process_name,
        window_title=window_title,
    )



def _get_process_name(pid: int) -> str:
    if os.name != "nt":
        return "unsupported.exe"

    kernel32 = ctypes.windll.kernel32
    process_handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not process_handle:
        return f"pid_{pid}.exe"

    try:
        buffer_size = wintypes.DWORD(260)
        exe_buffer = ctypes.create_unicode_buffer(buffer_size.value)
        success = kernel32.QueryFullProcessImageNameW(
            process_handle,
            0,
            exe_buffer,
            ctypes.byref(buffer_size),
        )
        if not success:
            return f"pid_{pid}.exe"
        return Path(exe_buffer.value).name.lower()
    finally:
        kernel32.CloseHandle(process_handle)
