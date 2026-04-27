from __future__ import annotations

import ctypes
import logging
import os
import threading
import uuid
from collections.abc import Callable
from typing import Any

from ctypes import wintypes

from .config import native_hooks_disabled

WTS_SESSION_LOCK = 0x7
WTS_SESSION_UNLOCK = 0x8

WM_CLOSE = 0x0010
WM_DESTROY = 0x0002
WM_QUIT = 0x0012
WM_WTSSESSION_CHANGE = 0x02B1
NOTIFY_FOR_THIS_SESSION = 0


def _win_type(name: str, fallback: Any) -> Any:
    return getattr(wintypes, name, fallback)


HWND = _win_type("HWND", ctypes.c_void_p)
HINSTANCE = _win_type("HINSTANCE", ctypes.c_void_p)
HCURSOR = _win_type("HCURSOR", ctypes.c_void_p)
HICON = _win_type("HICON", ctypes.c_void_p)
HMENU = _win_type("HMENU", ctypes.c_void_p)
LPARAM = _win_type("LPARAM", ctypes.c_ssize_t)
WPARAM = _win_type("WPARAM", ctypes.c_size_t)
LRESULT = _win_type("LRESULT", ctypes.c_ssize_t)
UINT = _win_type("UINT", ctypes.c_uint)
DWORD = _win_type("DWORD", ctypes.c_uint32)
BOOL = _win_type("BOOL", ctypes.c_int)
LPCWSTR = _win_type("LPCWSTR", ctypes.c_wchar_p)
ATOM = _win_type("ATOM", ctypes.c_ushort)

WNDPROC_FACTORY = getattr(ctypes, "WINFUNCTYPE", ctypes.CFUNCTYPE)


class POINT(ctypes.Structure):
    _fields_ = [
        ("x", ctypes.c_long),
        ("y", ctypes.c_long),
    ]


class MSG(ctypes.Structure):
    _fields_ = [
        ("hwnd", HWND),
        ("message", UINT),
        ("wParam", WPARAM),
        ("lParam", LPARAM),
        ("time", DWORD),
        ("pt", POINT),
        ("lPrivate", DWORD),
    ]


WNDPROC = WNDPROC_FACTORY(LRESULT, HWND, UINT, WPARAM, LPARAM)


class WNDCLASSEXW(ctypes.Structure):
    _fields_ = [
        ("cbSize", UINT),
        ("style", UINT),
        ("lpfnWndProc", WNDPROC),
        ("cbClsExtra", ctypes.c_int),
        ("cbWndExtra", ctypes.c_int),
        ("hInstance", HINSTANCE),
        ("hIcon", HICON),
        ("hCursor", HCURSOR),
        ("hbrBackground", ctypes.c_void_p),
        ("lpszMenuName", LPCWSTR),
        ("lpszClassName", LPCWSTR),
        ("hIconSm", HICON),
    ]


def _format_handle(value: int | ctypes.c_void_p | None) -> str:
    try:
        if value is None:
            return "0x0"
        return f"0x{int(value):x}"
    except Exception:
        return "0x0"


def _get_last_error(kernel32: Any | None) -> int:
    try:
        if kernel32 is not None and hasattr(kernel32, "GetLastError"):
            return int(kernel32.GetLastError())
    except Exception:
        pass
    try:
        return int(ctypes.get_last_error())
    except Exception:
        return 0


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
        self._thread_id: int | None = None
        self._hwnd: int | None = None
        self._window_class_name = f"WorkLogDiarySessionMonitor_{uuid.uuid4().hex}"
        self._startup_event = threading.Event()
        self._stop_requested = threading.Event()
        self._stop_lock = threading.Lock()
        self._startup_error: str | None = None
        self._startup_error_type: str | None = None
        self._startup_last_error: int = 0
        self._wnd_proc_ref: Any | None = None
        self._thread_exit_reason = "unknown"
        self._window_destroyed_logged = False

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
        self._stop_requested.clear()
        self._startup_error = None
        self._startup_error_type = None
        self._startup_last_error = 0
        self._thread_exit_reason = "unknown"
        self._window_destroyed_logged = False
        self._thread = threading.Thread(target=self._run_windows_loop, name="SessionMonitor", daemon=True)
        self._thread.start()
        started = self._startup_event.wait(timeout=5.0)

        if not started and self._startup_error is None:
            self._startup_error_type = "TimeoutError"
            self._startup_error = "startup_timeout"
            self._startup_last_error = 0
            self._thread_exit_reason = "startup_timeout"
            self.logger.warning(
                "event=session_monitor_start_failed error_type=%s detail=%s last_error=%s",
                self._startup_error_type,
                self._startup_error,
                self._startup_last_error,
            )

    def stop(self) -> None:
        if os.name != "nt":
            return

        with self._stop_lock:
            thread = self._thread
            hwnd = self._hwnd
            thread_id = self._thread_id
            destroyed_logged = self._window_destroyed_logged

        if thread is None:
            return

        self._stop_requested.set()

        try:
            windll = getattr(ctypes, "windll", None)
            if windll is None:
                raise RuntimeError("ctypes.windll unavailable")
            user32 = windll.user32
            kernel32 = windll.kernel32

            posted = False
            if hwnd:
                try:
                    user32.PostMessageW(HWND(hwnd), WM_CLOSE, WPARAM(0), LPARAM(0))
                    posted = True
                except Exception:
                    posted = False
            if not posted and thread_id is not None:
                try:
                    user32.PostThreadMessageW(DWORD(thread_id), WM_QUIT, WPARAM(0), LPARAM(0))
                    posted = True
                except Exception:
                    posted = False
            if not posted:
                self.logger.warning(
                    "event=session_monitor_stop_post_failed last_error=%s",
                    _get_last_error(kernel32),
                )
        except Exception as exc:
            self.logger.warning(
                "[CRASH] stage=session_monitor_stop_post_message status=error pid=%s thread=%s error_type=%s error=%s",
                os.getpid(),
                threading.current_thread().name,
                exc.__class__.__name__,
                exc,
            )

        thread.join(timeout=5.0)

        if hwnd and not destroyed_logged and not self._window_destroyed_logged and not thread.is_alive():
            self._log_window_destroyed(hwnd)

    def _run_windows_loop(self) -> None:
        startup_complete = False
        message_loop_started = False
        message_loop_exit_reason = "not_started"
        message_loop_last_error = 0
        hwnd: int | None = None
        class_registered = False
        session_notification_registered = False
        user32 = None
        kernel32 = None
        wtsapi32 = None
        h_instance = None

        try:
            if os.name != "nt":
                raise RuntimeError("Windows session monitor unavailable on non-nt platform")
            windll = getattr(ctypes, "windll", None)
            if windll is None:
                raise RuntimeError("ctypes.windll unavailable")

            user32 = windll.user32
            kernel32 = windll.kernel32
            wtsapi32 = windll.wtsapi32

            self._thread_id = int(kernel32.GetCurrentThreadId())
            self.logger.info("event=session_monitor_thread_start thread_id=%s", self._thread_id)

            user32.DefWindowProcW.argtypes = [HWND, UINT, WPARAM, LPARAM]
            user32.DefWindowProcW.restype = LRESULT
            user32.CreateWindowExW.argtypes = [
                DWORD,
                LPCWSTR,
                LPCWSTR,
                DWORD,
                ctypes.c_int,
                ctypes.c_int,
                ctypes.c_int,
                ctypes.c_int,
                HWND,
                HMENU,
                HINSTANCE,
                ctypes.c_void_p,
            ]
            user32.CreateWindowExW.restype = HWND
            user32.DestroyWindow.argtypes = [HWND]
            user32.DestroyWindow.restype = BOOL
            user32.GetMessageW.argtypes = [ctypes.POINTER(MSG), HWND, UINT, UINT]
            user32.GetMessageW.restype = ctypes.c_int
            user32.TranslateMessage.argtypes = [ctypes.POINTER(MSG)]
            user32.TranslateMessage.restype = BOOL
            user32.DispatchMessageW.argtypes = [ctypes.POINTER(MSG)]
            user32.DispatchMessageW.restype = LRESULT
            user32.PostQuitMessage.argtypes = [ctypes.c_int]
            user32.PostQuitMessage.restype = None
            user32.RegisterClassExW.argtypes = [ctypes.POINTER(WNDCLASSEXW)]
            user32.RegisterClassExW.restype = ATOM
            user32.UnregisterClassW.argtypes = [LPCWSTR, HINSTANCE]
            user32.UnregisterClassW.restype = BOOL
            user32.GetModuleHandleW.argtypes = [LPCWSTR]
            user32.GetModuleHandleW.restype = HINSTANCE
            user32.PostMessageW.argtypes = [HWND, UINT, WPARAM, LPARAM]
            user32.PostMessageW.restype = BOOL
            user32.PostThreadMessageW.argtypes = [DWORD, UINT, WPARAM, LPARAM]
            user32.PostThreadMessageW.restype = BOOL
            user32.IsWindow.argtypes = [HWND]
            user32.IsWindow.restype = BOOL

            wtsapi32.WTSRegisterSessionNotification.argtypes = [HWND, DWORD]
            wtsapi32.WTSRegisterSessionNotification.restype = BOOL
            wtsapi32.WTSUnRegisterSessionNotification.argtypes = [HWND]
            wtsapi32.WTSUnRegisterSessionNotification.restype = BOOL

            h_instance = kernel32.GetModuleHandleW(None)
            if not h_instance:
                last_error = _get_last_error(kernel32)
                self._record_start_failure("OSError", "GetModuleHandleW failed", last_error)
                return

            def wnd_proc(hwnd_value: int, msg: int, wparam: int, lparam: int) -> int:
                if msg == WM_WTSSESSION_CHANGE:
                    if not self._stop_requested.is_set():
                        self._handle_session_change_code(int(wparam))
                    return 0
                if msg == WM_CLOSE:
                    if not user32.DestroyWindow(HWND(hwnd_value)):
                        self.logger.warning(
                            "event=session_monitor_destroy_window_failed hwnd=%s last_error=%s",
                            _format_handle(hwnd_value),
                            _get_last_error(kernel32),
                        )
                    return 0
                if msg == WM_DESTROY:
                    self._log_window_destroyed(hwnd_value)
                    user32.PostQuitMessage(0)
                    return 0
                return int(user32.DefWindowProcW(HWND(hwnd_value), UINT(msg), WPARAM(wparam), LPARAM(lparam)))

            wnd_proc_cb = WNDPROC(wnd_proc)
            self._wnd_proc_ref = wnd_proc_cb

            wnd_class = WNDCLASSEXW()
            wnd_class.cbSize = ctypes.sizeof(WNDCLASSEXW)
            wnd_class.style = 0
            wnd_class.lpfnWndProc = wnd_proc_cb
            wnd_class.cbClsExtra = 0
            wnd_class.cbWndExtra = 0
            wnd_class.hInstance = HINSTANCE(h_instance)
            wnd_class.hIcon = HICON(0)
            wnd_class.hCursor = HCURSOR(0)
            wnd_class.hbrBackground = ctypes.c_void_p(0)
            wnd_class.lpszMenuName = None
            wnd_class.lpszClassName = self._window_class_name
            wnd_class.hIconSm = HICON(0)

            class_atom = user32.RegisterClassExW(ctypes.pointer(wnd_class))
            if not class_atom:
                last_error = _get_last_error(kernel32)
                self._record_start_failure("OSError", "RegisterClassExW failed", last_error)
                return
            class_registered = True
            self.logger.info(
                "event=session_monitor_window_class_registered atom=%s class_name=%s",
                int(class_atom),
                self._window_class_name,
            )

            hwnd_value = user32.CreateWindowExW(
                DWORD(0),
                self._window_class_name,
                self._window_class_name,
                DWORD(0),
                0,
                0,
                0,
                0,
                HWND(0),
                HMENU(0),
                HINSTANCE(h_instance),
                None,
            )
            if not hwnd_value:
                last_error = _get_last_error(kernel32)
                self._record_start_failure("OSError", "CreateWindowExW failed", last_error)
                return
            hwnd = int(hwnd_value)
            self._hwnd = hwnd
            self.logger.info("event=session_monitor_window_created hwnd=%s", _format_handle(hwnd))

            session_notification_registered = bool(
                wtsapi32.WTSRegisterSessionNotification(HWND(hwnd), DWORD(NOTIFY_FOR_THIS_SESSION))
            )
            if not session_notification_registered:
                last_error = _get_last_error(kernel32)
                self._record_start_failure(
                    "OSError",
                    "WTSRegisterSessionNotification failed",
                    last_error,
                )
                return

            startup_complete = True
            self._startup_event.set()
            self.logger.info("event=session_monitor_started ok=true")
            if self._stop_requested.is_set():
                if not user32.DestroyWindow(HWND(hwnd)):
                    self.logger.warning(
                        "event=session_monitor_destroy_window_failed hwnd=%s last_error=%s",
                        _format_handle(hwnd),
                        _get_last_error(kernel32),
                    )

            self.logger.info("event=session_monitor_message_loop_start hwnd=%s", _format_handle(hwnd))
            message_loop_started = True
            msg = MSG()
            while True:
                result = int(user32.GetMessageW(ctypes.byref(msg), HWND(0), UINT(0), UINT(0)))
                if result == -1:
                    message_loop_exit_reason = "get_message_failed"
                    message_loop_last_error = _get_last_error(kernel32)
                    break
                if result == 0:
                    message_loop_exit_reason = "wm_quit"
                    break
                user32.TranslateMessage(ctypes.byref(msg))
                user32.DispatchMessageW(ctypes.byref(msg))
        except Exception as exc:
            last_error = 0
            if kernel32 is not None:
                try:
                    last_error = _get_last_error(kernel32)
                except Exception:
                    last_error = 0
            if not startup_complete:
                self._record_start_failure(exc.__class__.__name__, str(exc), last_error)
                return
            self._thread_exit_reason = "thread_exception"
            self._startup_event.set()
            self.logger.exception(
                "event=session_monitor_thread_error error_type=%s detail=%s last_error=%s",
                exc.__class__.__name__,
                exc,
                last_error,
            )
        finally:
            if message_loop_started:
                self.logger.info(
                    "event=session_monitor_message_loop_exit reason=%s last_error=%s",
                    message_loop_exit_reason,
                    message_loop_last_error,
                )

            try:
                if session_notification_registered:
                    if wtsapi32 is not None and not wtsapi32.WTSUnRegisterSessionNotification(HWND(hwnd or 0)):
                        self.logger.warning(
                            "event=session_monitor_unregister_notification_failed hwnd=%s last_error=%s",
                            _format_handle(hwnd),
                            _get_last_error(kernel32),
                        )
            except Exception as exc:
                self.logger.warning(
                    "[CRASH] stage=session_monitor_unregister_notification status=error pid=%s thread=%s error_type=%s error=%s",
                    os.getpid(),
                    threading.current_thread().name,
                    exc.__class__.__name__,
                    exc,
                )

            try:
                if user32 is not None and hwnd and user32.IsWindow(HWND(hwnd)):
                    if not user32.DestroyWindow(HWND(hwnd)):
                        self.logger.warning(
                            "event=session_monitor_destroy_window_failed hwnd=%s last_error=%s",
                            _format_handle(hwnd),
                            _get_last_error(kernel32),
                        )
                    elif not self._window_destroyed_logged:
                        self._log_window_destroyed(hwnd)
            except Exception as exc:
                self.logger.warning(
                    "[CRASH] stage=session_monitor_destroy_window status=error pid=%s thread=%s error_type=%s error=%s",
                    os.getpid(),
                    threading.current_thread().name,
                    exc.__class__.__name__,
                    exc,
                )

            try:
                if class_registered and user32 is not None:
                    if not user32.UnregisterClassW(self._window_class_name, HINSTANCE(h_instance)):
                        self.logger.warning(
                            "event=session_monitor_unregister_class_failed class_name=%s last_error=%s",
                            self._window_class_name,
                            _get_last_error(kernel32),
                        )
                    else:
                        self.logger.info("event=session_monitor_window_class_unregistered class_name=%s", self._window_class_name)
            except Exception as exc:
                self.logger.warning(
                    "[CRASH] stage=session_monitor_unregister_class status=error pid=%s thread=%s error_type=%s error=%s",
                    os.getpid(),
                    threading.current_thread().name,
                    exc.__class__.__name__,
                    exc,
                )

            with self._stop_lock:
                self._hwnd = None
                self._thread_id = None
                self._wnd_proc_ref = None

            if not startup_complete:
                self._startup_event.set()

            if self._thread_exit_reason == "unknown":
                self._thread_exit_reason = message_loop_exit_reason if message_loop_started else "startup_failed"
            self.logger.info("event=session_monitor_thread_exit reason=%s", self._thread_exit_reason)

    def _record_start_failure(self, error_type: str, detail: str, last_error: int) -> None:
        self._startup_error_type = error_type
        self._startup_error = detail
        self._startup_last_error = last_error
        self._thread_exit_reason = "startup_failed"
        self._startup_event.set()
        self.logger.warning(
            "event=session_monitor_start_failed error_type=%s detail=%s last_error=%s",
            error_type,
            detail,
            last_error,
        )

    def _log_window_destroyed(self, hwnd: int) -> None:
        if self._window_destroyed_logged:
            return
        self._window_destroyed_logged = True
        self.logger.info("event=session_monitor_window_destroyed hwnd=%s", _format_handle(hwnd))

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
