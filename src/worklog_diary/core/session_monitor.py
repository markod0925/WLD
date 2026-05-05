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


def _bind_win32_function(dll: Any, name: str, argtypes: list[Any], restype: Any) -> Any:
    func = getattr(dll, name)
    func.argtypes = argtypes
    func.restype = restype
    return func


def _configure_win32_bindings(kernel32: Any, user32: Any, wtsapi32: Any) -> None:
    _bind_win32_function(kernel32, "GetCurrentThreadId", [], DWORD)
    _bind_win32_function(kernel32, "GetModuleHandleW", [LPCWSTR], HINSTANCE)

    _bind_win32_function(user32, "DefWindowProcW", [HWND, UINT, WPARAM, LPARAM], LRESULT)
    _bind_win32_function(
        user32,
        "CreateWindowExW",
        [
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
        ],
        HWND,
    )
    _bind_win32_function(user32, "DestroyWindow", [HWND], BOOL)
    _bind_win32_function(user32, "GetMessageW", [ctypes.POINTER(MSG), HWND, UINT, UINT], ctypes.c_int)
    _bind_win32_function(user32, "TranslateMessage", [ctypes.POINTER(MSG)], BOOL)
    _bind_win32_function(user32, "DispatchMessageW", [ctypes.POINTER(MSG)], LRESULT)
    _bind_win32_function(user32, "PostQuitMessage", [ctypes.c_int], None)
    _bind_win32_function(user32, "RegisterClassExW", [ctypes.POINTER(WNDCLASSEXW)], ATOM)
    _bind_win32_function(user32, "UnregisterClassW", [LPCWSTR, HINSTANCE], BOOL)
    _bind_win32_function(user32, "PostMessageW", [HWND, UINT, WPARAM, LPARAM], BOOL)
    _bind_win32_function(user32, "PostThreadMessageW", [DWORD, UINT, WPARAM, LPARAM], BOOL)
    _bind_win32_function(user32, "IsWindow", [HWND], BOOL)

    _bind_win32_function(wtsapi32, "WTSRegisterSessionNotification", [HWND, DWORD], BOOL)
    _bind_win32_function(wtsapi32, "WTSUnRegisterSessionNotification", [HWND], BOOL)


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
        process_backlog_only_while_locked: bool = True,
    ) -> None:
        self.on_locked = on_locked
        self.on_unlocked = on_unlocked
        self.logger = logging.getLogger(__name__)
        self._process_backlog_only_while_locked = bool(process_backlog_only_while_locked)

        self._thread: threading.Thread | None = None
        self._thread_id: int | None = None
        self._hwnd: int | None = None
        self._window_class_name = f"WorkLogDiarySessionMonitor_{uuid.uuid4().hex}"
        self._startup_event = threading.Event()
        self._stop_requested = threading.Event()
        self._stop_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._startup_error: str | None = None
        self._startup_error_type: str | None = None
        self._startup_error_category: str | None = None
        self._startup_last_error: int = 0
        self._wnd_proc_ref: Any | None = None
        self._thread_exit_reason = "unknown"
        self._window_destroyed_logged = False
        self._notifications_registered = False
        self._monitor_state = "unknown"
        self._lock_state = "unknown"
        self._stop_failure_logged = False

    def start(self) -> None:
        if os.name != "nt":
            return
        if self._thread and self._thread.is_alive():
            return
        if self._monitor_state == "degraded":
            return
        if native_hooks_disabled():
            self.logger.info("Session monitor disabled in test/native-hook-off mode")
            return
        self.logger.info("event=session_monitor_starting")

        self._startup_event.clear()
        self._stop_requested.clear()
        with self._state_lock:
            self._startup_error = None
            self._startup_error_type = None
            self._startup_error_category = None
            self._startup_last_error = 0
            self._thread_exit_reason = "unknown"
            self._window_destroyed_logged = False
            self._notifications_registered = False
            self._monitor_state = "starting"
            self._lock_state = "unknown"
            self._stop_failure_logged = False
            self._hwnd = None
            self._thread_id = None
        self._thread = threading.Thread(target=self._run_windows_loop, name="SessionMonitor", daemon=True)
        self._thread.start()
        started = self._startup_event.wait(timeout=5.0)

        if not started and self._startup_error is None:
            self._record_start_failure(
                error_category="startup_timeout",
                error_type="TimeoutError",
                exception_message="startup_timeout",
                last_error=0,
            )

    def stop(self) -> None:
        if os.name != "nt":
            return

        self.logger.info("event=session_monitor_stopping")
        self._log_backlog_gate_state(
            lock_state=self._lock_state_label(),
            backlog_processing_allowed=False,
            reason="blocked_shutdown",
        )
        self._stop_requested.set()

        with self._state_lock:
            thread = self._thread
            hwnd = self._hwnd
            thread_id = self._thread_id
            destroyed_logged = self._window_destroyed_logged
            notifications_registered = self._notifications_registered

        if thread is None and hwnd is None and not notifications_registered:
            with self._state_lock:
                if self._monitor_state != "active":
                    self._monitor_state = "unavailable"
            self.logger.info("event=session_monitor_stopped monitor_state=%s", self._monitor_state)
            return

        stop_failed_step: str | None = None
        stop_failure_message: str | None = None
        stop_failure_type: str = "RuntimeError"
        try:
            windll = getattr(ctypes, "windll", None)
            if windll is None:
                raise RuntimeError("ctypes.windll unavailable")
            user32 = windll.user32
            kernel32 = windll.kernel32
            wtsapi32 = windll.wtsapi32

            posted = True
            if hwnd:
                try:
                    user32.PostMessageW(HWND(hwnd), WM_CLOSE, WPARAM(0), LPARAM(0))
                except Exception:
                    posted = False
                    stop_failed_step = "post_message"
                    stop_failure_message = "PostMessageW failed"
                    stop_failure_type = "OSError"
            if not posted and thread_id is not None:
                try:
                    user32.PostThreadMessageW(DWORD(thread_id), WM_QUIT, WPARAM(0), LPARAM(0))
                    posted = True
                except Exception:
                    stop_failed_step = "post_message"
                    posted = False
                    stop_failure_message = "PostThreadMessageW failed"
                    stop_failure_type = "OSError"
            if not posted and hwnd is None and thread_id is None:
                posted = True

            if notifications_registered and hwnd:
                try:
                    if not wtsapi32.WTSUnRegisterSessionNotification(HWND(hwnd)):
                        stop_failed_step = stop_failed_step or "unregister_notification"
                        stop_failure_message = "WTSUnRegisterSessionNotification failed"
                        stop_failure_type = "OSError"
                except Exception:
                    stop_failed_step = stop_failed_step or "unregister_notification"
                    stop_failure_message = "WTSUnRegisterSessionNotification failed"
                    stop_failure_type = "Exception"
        except Exception as exc:
            stop_failed_step = stop_failed_step or "post_message"
            stop_failure_message = str(exc)
            stop_failure_type = exc.__class__.__name__

        if thread is not None:
            try:
                thread.join(timeout=5.0)
            except Exception as exc:
                stop_failed_step = stop_failed_step or "join_thread"
                stop_failure_message = str(exc)
                stop_failure_type = exc.__class__.__name__

        if hwnd and not destroyed_logged and not self._window_destroyed_logged and thread is not None and not thread.is_alive():
            self._log_window_destroyed(hwnd)

        if stop_failed_step is None and thread is not None and thread.is_alive():
            stop_failed_step = "join_thread"
            stop_failure_message = "thread_did_not_exit"
            stop_failure_type = "TimeoutError"

        with self._state_lock:
            self._monitor_state = "unavailable"

        if stop_failed_step is not None and not self._stop_failure_logged:
            self._stop_failure_logged = True
            self.logger.warning(
                "event=session_monitor_stop_failed step=%s error_category=win32_shutdown error_type=%s error_message=%s",
                stop_failed_step,
                stop_failure_type,
                stop_failure_message or stop_failed_step,
            )

        self.logger.info("event=session_monitor_stopped monitor_state=unavailable")

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

            _configure_win32_bindings(kernel32, user32, wtsapi32)

            self._thread_id = int(kernel32.GetCurrentThreadId())
            self.logger.info("event=session_monitor_thread_start thread_id=%s", self._thread_id)
            h_instance = kernel32.GetModuleHandleW(None)
            if not h_instance:
                last_error = _get_last_error(kernel32)
                self._record_start_failure(
                    error_category="kernel32_get_module_handle",
                    error_type="OSError",
                    exception_message="GetModuleHandleW failed",
                    last_error=last_error,
                )
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
                self._record_start_failure(
                    error_category="register_window_class",
                    error_type="OSError",
                    exception_message="RegisterClassExW failed",
                    last_error=last_error,
                )
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
                self._record_start_failure(
                    error_category="create_window",
                    error_type="OSError",
                    exception_message="CreateWindowExW failed",
                    last_error=last_error,
                )
                return
            hwnd = int(hwnd_value)
            with self._state_lock:
                self._hwnd = hwnd
            self.logger.info("event=session_monitor_window_created hwnd=%s", _format_handle(hwnd))

            session_notification_registered = bool(
                wtsapi32.WTSRegisterSessionNotification(HWND(hwnd), DWORD(NOTIFY_FOR_THIS_SESSION))
            )
            if not session_notification_registered:
                last_error = _get_last_error(kernel32)
                self._record_start_failure(
                    error_category="register_session_notification",
                    error_type="OSError",
                    exception_message="WTSRegisterSessionNotification failed",
                    last_error=last_error,
                )
                return

            startup_complete = True
            with self._state_lock:
                self._monitor_state = "active"
                self._notifications_registered = True
            self.logger.info(
                "event=session_monitor_started notifications_registered=true degraded_lock_monitoring=false hwnd_created=true lock_state=%s",
                self._lock_state_label(),
            )
            if self._process_backlog_only_while_locked:
                startup_reason = "blocked_lock_state_unknown"
                startup_allowed = False
            else:
                startup_reason = "allowed_config_disabled"
                startup_allowed = True
            self._log_backlog_gate_state(
                lock_state=self._lock_state_label(),
                backlog_processing_allowed=startup_allowed,
                reason=startup_reason,
            )
            self._startup_event.set()
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
                self._record_start_failure(
                    error_category="win32_startup_exception",
                    error_type=exc.__class__.__name__,
                    exception_message=str(exc),
                    last_error=last_error,
                )
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
                self._notifications_registered = False

            if not startup_complete:
                self._startup_event.set()

            if self._thread_exit_reason == "unknown":
                self._thread_exit_reason = message_loop_exit_reason if message_loop_started else "startup_failed"
            with self._state_lock:
                if self._monitor_state == "starting":
                    self._monitor_state = "unavailable" if startup_complete else "degraded"
            self.logger.info("event=session_monitor_thread_exit reason=%s", self._thread_exit_reason)

    def _record_start_failure(
        self,
        *,
        error_category: str,
        error_type: str,
        exception_message: str,
        last_error: int,
    ) -> None:
        with self._state_lock:
            self._startup_error_category = error_category
            self._startup_error_type = error_type
            self._startup_error = exception_message
            self._startup_last_error = last_error
            self._thread_exit_reason = "startup_failed"
            self._monitor_state = "degraded"
        self.logger.warning(
            "event=session_monitor_start_failed error_type=%s detail=%s last_error=%s error_category=%s exception_type=%s exception_message=%s degraded_lock_monitoring=true",
            error_type,
            exception_message,
            last_error,
            error_category,
            error_type,
            exception_message,
        )
        degraded_reason = "allowed_config_disabled" if not self._process_backlog_only_while_locked else "blocked_lock_monitor_degraded"
        self._log_backlog_gate_state(
            lock_state="degraded",
            backlog_processing_allowed=not self._process_backlog_only_while_locked,
            reason=degraded_reason,
        )
        self._startup_event.set()

    def _log_window_destroyed(self, hwnd: int) -> None:
        if self._window_destroyed_logged:
            return
        self._window_destroyed_logged = True
        self.logger.info("event=session_monitor_window_destroyed hwnd=%s", _format_handle(hwnd))

    def _log_backlog_gate_state(self, *, lock_state: str, backlog_processing_allowed: bool, reason: str) -> None:
        self.logger.info(
            "event=summary_backlog_gate_state process_backlog_only_while_locked=%s lock_state=%s backlog_processing_allowed=%s reason=%s",
            self._process_backlog_only_while_locked,
            lock_state,
            backlog_processing_allowed,
            reason,
        )

    def _safe_invoke(self, callback: Callable[[], None]) -> None:
        try:
            callback()
        except Exception as exc:
            self.logger.exception("event=session_monitor_callback_failed error=%s", exc)

    def _handle_session_change_code(self, code: int) -> None:
        if code == WTS_SESSION_LOCK:
            previous_state = self._lock_state_label()
            with self._state_lock:
                self._lock_state = "locked"
            self.logger.info(
                "event=session_locked source=wts_session_change previous_lock_state=%s new_lock_state=locked",
                previous_state,
            )
            self._log_backlog_gate_state(
                lock_state="locked",
                backlog_processing_allowed=True,
                reason="allowed_config_disabled" if not self._process_backlog_only_while_locked else "allowed_pc_locked",
            )
            self._safe_invoke(self.on_locked)
        elif code == WTS_SESSION_UNLOCK:
            previous_state = self._lock_state_label()
            with self._state_lock:
                self._lock_state = "unlocked"
            self.logger.info(
                "event=session_unlocked source=wts_session_change previous_lock_state=%s new_lock_state=unlocked",
                previous_state,
            )
            self._log_backlog_gate_state(
                lock_state="unlocked",
                backlog_processing_allowed=not self._process_backlog_only_while_locked,
                reason="allowed_config_disabled" if not self._process_backlog_only_while_locked else "blocked_pc_unlocked",
            )
            self._safe_invoke(self.on_unlocked)

    def _lock_state_label(self) -> str:
        with self._state_lock:
            return self._lock_state

    def get_status(self) -> dict[str, Any]:
        with self._state_lock:
            monitor_state = self._monitor_state
            lock_state = self._lock_state
            hwnd_created = self._hwnd is not None
            notifications_registered = self._notifications_registered
            startup_error = self._startup_error
            startup_error_type = self._startup_error_type
            startup_error_category = self._startup_error_category
            startup_last_error = self._startup_last_error
        if monitor_state == "starting":
            monitor_state = "unknown"
        return {
            "monitor_status": monitor_state,
            "lock_state": lock_state,
            "hwnd_created": hwnd_created,
            "notifications_registered": notifications_registered,
            "degraded_lock_monitoring": monitor_state == "degraded",
            "startup_error_category": startup_error_category,
            "startup_error_type": startup_error_type,
            "startup_error_message": startup_error,
            "startup_last_error": startup_last_error,
        }
