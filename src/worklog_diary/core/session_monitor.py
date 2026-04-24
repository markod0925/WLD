from __future__ import annotations

import ctypes
import logging
import os
import threading
import uuid
from collections.abc import Callable

from ctypes import wintypes

from .config import native_hooks_disabled

WTS_SESSION_LOCK = 0x7
WTS_SESSION_UNLOCK = 0x8


class SessionMonitor:
    """Listen for Windows session lock and unlock events on a background thread."""

    def __init__(
        self,
        *,
        on_locked: Callable[[], None],
        on_unlocked: Callable[[], None],
    ) -> None:
        self.on_locked = on_locked
        self.on_unlocked = on_unlocked
        self.logger = logging.getLogger(__name__)

        self._thread: threading.Thread | None = None
        self._hwnd: int | None = None
        self._window_class_name = f"WorkLogDiarySessionMonitor_{uuid.uuid4().hex}"
        self._startup_event = threading.Event()
        self._stop_lock = threading.Lock()
        self._startup_error: str | None = None
        self._wnd_proc_ref: object | None = None
        self._thread_exit_reason = "unknown"

    def start(self) -> None:
        if os.name != "nt":
            return
        if self._thread and self._thread.is_alive():
            return
        if native_hooks_disabled():
            self.logger.info("Session monitor disabled in test/native-hook-off mode")
            return
        self.logger.info("event=session_monitor_start")

        self._startup_event.clear()
        self._startup_error = None
        self._thread_exit_reason = "unknown"
        self._thread = threading.Thread(target=self._run_windows_loop, name="SessionMonitor", daemon=True)
        self._thread.start()
        self._startup_event.wait(timeout=5.0)

        if self._startup_error:
            self.logger.warning("event=session_monitor_start_failed error_type=RuntimeError detail=%s", self._startup_error)

    def stop(self) -> None:
        if os.name != "nt":
            return

        with self._stop_lock:
            thread = self._thread
            hwnd = self._hwnd
            self._thread = None
            self._hwnd = None

        if thread is None:
            return

        if hwnd:
            try:
                ctypes.windll.user32.PostMessageW(int(hwnd), 0x0010, 0, 0)  # WM_CLOSE
            except Exception as exc:
                self.logger.warning(
                    "[CRASH] stage=session_monitor_stop_post_message status=error pid=%s thread=%s error_type=%s error=%s",
                    os.getpid(),
                    threading.current_thread().name,
                    exc.__class__.__name__,
                    exc,
                )

        thread.join(timeout=5.0)

    def _run_windows_loop(self) -> None:
        try:
            user32 = ctypes.windll.user32
            kernel32 = ctypes.windll.kernel32
            wtsapi32 = ctypes.windll.wtsapi32

            WM_WTSSESSION_CHANGE = 0x02B1
            NOTIFY_FOR_THIS_SESSION = 0
            WM_CLOSE = 0x0010
            WM_DESTROY = 0x0002

            LRESULT = getattr(wintypes, "LRESULT", ctypes.c_ssize_t)
            WNDPROC = ctypes.WINFUNCTYPE(LRESULT, wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM)
            hcursor_type = getattr(wintypes, "HCURSOR", getattr(wintypes, "HANDLE", ctypes.c_void_p))

            class WNDCLASSW(ctypes.Structure):
                _fields_ = [
                    ("style", wintypes.UINT),
                    ("lpfnWndProc", WNDPROC),
                    ("cbClsExtra", ctypes.c_int),
                    ("cbWndExtra", ctypes.c_int),
                    ("hInstance", wintypes.HINSTANCE),
                    ("hIcon", wintypes.HICON),
                    ("hCursor", hcursor_type),
                    ("hbrBackground", wintypes.HBRUSH),
                    ("lpszMenuName", wintypes.LPCWSTR),
                    ("lpszClassName", wintypes.LPCWSTR),
                ]

            wtsapi32.WTSRegisterSessionNotification.argtypes = [wintypes.HWND, wintypes.DWORD]
            wtsapi32.WTSRegisterSessionNotification.restype = wintypes.BOOL
            wtsapi32.WTSUnRegisterSessionNotification.argtypes = [wintypes.HWND]
            wtsapi32.WTSUnRegisterSessionNotification.restype = wintypes.BOOL

            user32.DefWindowProcW.argtypes = [wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM]
            user32.DefWindowProcW.restype = LRESULT
            user32.CreateWindowExW.restype = wintypes.HWND
            user32.CreateWindowExW.argtypes = [
                wintypes.DWORD,
                wintypes.LPCWSTR,
                wintypes.LPCWSTR,
                wintypes.DWORD,
                ctypes.c_int,
                ctypes.c_int,
                ctypes.c_int,
                ctypes.c_int,
                wintypes.HWND,
                wintypes.HMENU,
                wintypes.HINSTANCE,
                wintypes.LPVOID,
            ]

            def wnd_proc(hwnd: int, msg: int, wparam: int, lparam: int) -> int:
                if msg == WM_WTSSESSION_CHANGE:
                    self._handle_session_change_code(int(wparam))
                    return 0
                if msg == WM_CLOSE:
                    user32.DestroyWindow(hwnd)
                    return 0
                if msg == WM_DESTROY:
                    user32.PostQuitMessage(0)
                    return 0
                return int(user32.DefWindowProcW(hwnd, msg, wparam, lparam))

            wnd_proc_cb = WNDPROC(wnd_proc)
            self._wnd_proc_ref = wnd_proc_cb

            h_instance = kernel32.GetModuleHandleW(None)
            wnd_class = WNDCLASSW()
            wnd_class.lpfnWndProc = wnd_proc_cb
            wnd_class.lpszClassName = self._window_class_name
            wnd_class.hInstance = h_instance

            class_atom = user32.RegisterClassW(ctypes.byref(wnd_class))
            if not class_atom:
                self._startup_error = "RegisterClassW failed"
                self._startup_event.set()
                self._wnd_proc_ref = None
                self._thread_exit_reason = "register_class_failed"
                return

            hwnd = user32.CreateWindowExW(
                0,
                self._window_class_name,
                self._window_class_name,
                0,
                0,
                0,
                0,
                0,
                None,
                None,
                h_instance,
                None,
            )
            if not hwnd:
                self._startup_error = "CreateWindowExW failed"
                self._startup_event.set()
                user32.UnregisterClassW(self._window_class_name, h_instance)
                self._wnd_proc_ref = None
                self._thread_exit_reason = "create_window_failed"
                return

            registered = bool(wtsapi32.WTSRegisterSessionNotification(hwnd, NOTIFY_FOR_THIS_SESSION))
            if not registered:
                self._startup_error = "WTSRegisterSessionNotification failed"
                self.logger.warning("event=session_monitor_registration_failed")
                self._startup_event.set()
                user32.DestroyWindow(hwnd)
                user32.UnregisterClassW(self._window_class_name, h_instance)
                self._wnd_proc_ref = None
                self._thread_exit_reason = "session_notification_failed"
                return

            self._hwnd = int(hwnd)
            self._startup_event.set()
            self.logger.info("event=session_monitor_started ok=true")
            try:
                msg = wintypes.MSG()
                while True:
                    result = int(user32.GetMessageW(ctypes.byref(msg), None, 0, 0))
                    if result <= 0:
                        self._thread_exit_reason = "message_loop_stopped"
                        break
                    user32.TranslateMessage(ctypes.byref(msg))
                    user32.DispatchMessageW(ctypes.byref(msg))
            finally:
                if registered:
                    try:
                        wtsapi32.WTSUnRegisterSessionNotification(hwnd)
                    except Exception as exc:
                        self.logger.warning(
                            "[CRASH] stage=session_monitor_unregister_notification status=error pid=%s thread=%s error_type=%s error=%s",
                            os.getpid(),
                            threading.current_thread().name,
                            exc.__class__.__name__,
                            exc,
                        )
                try:
                    user32.UnregisterClassW(self._window_class_name, h_instance)
                except Exception as exc:
                    self.logger.warning(
                        "[CRASH] stage=session_monitor_unregister_class status=error pid=%s thread=%s error_type=%s error=%s",
                        os.getpid(),
                        threading.current_thread().name,
                        exc.__class__.__name__,
                        exc,
                    )
                self._hwnd = None
                self._wnd_proc_ref = None
        except Exception as exc:
            self._startup_error = str(exc)
            self._startup_event.set()
            self._thread_exit_reason = "startup_failed"
            self.logger.exception(
                "event=session_monitor_start_failed error_type=%s detail=%s",
                exc.__class__.__name__,
                exc,
            )
        finally:
            self.logger.info("event=session_monitor_thread_exit reason=%s", self._thread_exit_reason)

    def _safe_invoke(self, callback: Callable[[], None]) -> None:
        try:
            callback()
        except Exception as exc:
            self.logger.exception("event=session_monitor_callback_failed error=%s", exc)

    def _handle_session_change_code(self, code: int) -> None:
        if code == WTS_SESSION_LOCK:
            self.logger.info("event=session_locked")
            self._safe_invoke(self.on_locked)
        elif code == WTS_SESSION_UNLOCK:
            self.logger.info("event=session_unlocked")
            self._safe_invoke(self.on_unlocked)
