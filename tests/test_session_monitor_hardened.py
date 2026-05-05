from __future__ import annotations

import threading
from types import SimpleNamespace

from worklog_diary.core.session_monitor import SessionMonitor, WTS_SESSION_LOCK, WTS_SESSION_UNLOCK


def _install_fake_windows(
    monkeypatch,
    monitor: SessionMonitor,
    *,
    loop_event: threading.Event | None = None,
    last_error: int = 0,
    register_class_result: int = 1,
    create_window_result: int = 1,
    register_session_result: bool = True,
) -> tuple[dict[str, object], threading.Event]:
    loop_event = loop_event or threading.Event()
    state: dict[str, object] = {
        "post_message_calls": [],
        "post_thread_message_calls": [],
        "destroy_threads": [],
        "register_class_calls": 0,
        "create_window_calls": 0,
        "register_session_calls": 0,
        "unregister_session_calls": 0,
        "unregister_class_calls": 0,
        "window_alive": False,
    }

    def _handle_value(value: object) -> int:
        raw = getattr(value, "value", value)
        if isinstance(raw, bytes):
            return int.from_bytes(raw, byteorder="little", signed=False)
        try:
            return int(raw)
        except Exception:
            return 0

    def _get_current_thread_id() -> int:
        return 4242

    def _get_module_handle(_name) -> int:  # type: ignore[no-untyped-def]
        return 1

    def _register_class(wnd_class) -> int:  # type: ignore[no-untyped-def]
        state["register_class_calls"] = int(state["register_class_calls"]) + 1
        actual = getattr(wnd_class, "contents", None)
        if actual is None:
            actual = getattr(wnd_class, "_obj", wnd_class)
        state["wnd_proc_ref"] = actual.lpfnWndProc
        return register_class_result

    def _create_window(*_args) -> int:  # type: ignore[no-untyped-def]
        state["create_window_calls"] = int(state["create_window_calls"]) + 1
        state["window_alive"] = bool(create_window_result)
        return create_window_result

    def _register_session(hwnd, _flags) -> bool:  # type: ignore[no-untyped-def]
        state["register_session_calls"] = int(state["register_session_calls"]) + 1
        state["registered_hwnd"] = _handle_value(hwnd)
        return register_session_result

    def _unregister_session(hwnd) -> bool:  # type: ignore[no-untyped-def]
        state["unregister_session_calls"] = int(state["unregister_session_calls"]) + 1
        state["unregistered_hwnd"] = _handle_value(hwnd)
        return True

    def _destroy_window(hwnd) -> bool:  # type: ignore[no-untyped-def]
        state["destroy_threads"].append(threading.current_thread().name)
        state["destroyed_hwnd"] = _handle_value(hwnd)
        state["window_alive"] = False
        return True

    def _is_window(_hwnd) -> bool:  # type: ignore[no-untyped-def]
        return bool(state["window_alive"])

    def _post_message(hwnd, msg, _wparam, _lparam) -> bool:  # type: ignore[no-untyped-def]
        state["post_message_calls"].append((threading.current_thread().name, _handle_value(hwnd), _handle_value(msg)))
        loop_event.set()
        return True

    def _post_thread_message(thread_id, msg, _wparam, _lparam) -> bool:  # type: ignore[no-untyped-def]
        state["post_thread_message_calls"].append((threading.current_thread().name, _handle_value(thread_id), _handle_value(msg)))
        loop_event.set()
        return True

    def _get_message(_msg_ptr, _hwnd, _min, _max) -> int:  # type: ignore[no-untyped-def]
        loop_event.wait(timeout=1.0)
        return 0 if loop_event.is_set() else -1

    def _translate_message(_msg_ptr) -> bool:  # type: ignore[no-untyped-def]
        return True

    def _dispatch_message(_msg_ptr) -> int:  # type: ignore[no-untyped-def]
        return 0

    fake_user32 = SimpleNamespace(
        RegisterClassExW=_register_class,
        CreateWindowExW=_create_window,
        DestroyWindow=_destroy_window,
        GetMessageW=_get_message,
        TranslateMessage=_translate_message,
        DispatchMessageW=_dispatch_message,
        PostQuitMessage=lambda _code: None,
        UnregisterClassW=lambda _class_name, _hinstance: True,
        DefWindowProcW=lambda *_args: 0,
        PostMessageW=_post_message,
        PostThreadMessageW=_post_thread_message,
        IsWindow=_is_window,
    )
    fake_kernel32 = SimpleNamespace(
        GetCurrentThreadId=_get_current_thread_id,
        GetModuleHandleW=_get_module_handle,
        GetLastError=lambda: last_error,
    )
    fake_wtsapi32 = SimpleNamespace(
        WTSRegisterSessionNotification=_register_session,
        WTSUnRegisterSessionNotification=_unregister_session,
    )
    monkeypatch.setattr(
        "worklog_diary.core.session_monitor.ctypes.windll",
        SimpleNamespace(user32=fake_user32, kernel32=fake_kernel32, wtsapi32=fake_wtsapi32),
        raising=False,
    )
    monkeypatch.setattr("worklog_diary.core.session_monitor.os", SimpleNamespace(name="nt", getpid=lambda: 12345))
    monkeypatch.setattr("worklog_diary.core.session_monitor.native_hooks_disabled", lambda: False)
    return state, loop_event


def test_kernel32_binding_is_used_for_get_module_handle(monkeypatch, caplog) -> None:
    monitor = SessionMonitor(on_locked=lambda: None, on_unlocked=lambda: None)
    caplog.set_level("INFO")
    loop_event = threading.Event()
    state, _ = _install_fake_windows(monkeypatch, monitor, loop_event=loop_event)

    monitor.start()
    assert monitor._thread is not None
    assert hasattr(state["wnd_proc_ref"], "__call__")
    assert any("event=session_monitor_started" in rec.message for rec in caplog.records)
    assert any("notifications_registered=true" in rec.message for rec in caplog.records)

    loop_event.set()
    monitor._thread.join(timeout=1.0)


def test_start_failure_emits_legacy_compatible_structured_marker(monkeypatch, caplog) -> None:
    monitor = SessionMonitor(on_locked=lambda: None, on_unlocked=lambda: None)
    caplog.set_level("INFO")
    monkeypatch.setattr("worklog_diary.core.session_monitor.os", SimpleNamespace(name="nt", getpid=lambda: 1))
    monkeypatch.setattr("worklog_diary.core.session_monitor.ctypes.windll", None, raising=False)

    monitor._run_windows_loop()

    start_failed = [rec.message for rec in caplog.records if "event=session_monitor_start_failed" in rec.message]
    assert start_failed
    assert any("exception_type=AttributeError" in line or "exception_type=RuntimeError" in line for line in start_failed)
    assert any("event=summary_backlog_gate_state" in rec.message for rec in caplog.records)
    assert monitor.get_status()["monitor_status"] == "degraded"


def test_stop_after_failed_start_does_not_post_without_hwnd(monkeypatch, caplog) -> None:
    monitor = SessionMonitor(on_locked=lambda: None, on_unlocked=lambda: None)
    caplog.set_level("INFO")
    state, _ = _install_fake_windows(monkeypatch, monitor, create_window_result=0, last_error=222)

    monitor._run_windows_loop()
    monitor.stop()
    monitor.stop()

    assert state["post_message_calls"] == []
    assert state["post_thread_message_calls"] == []
    assert len([rec for rec in caplog.records if "event=session_monitor_stop_failed" in rec.message]) == 0


def test_lock_and_unlock_events_update_lock_state(monkeypatch, caplog) -> None:
    monitor = SessionMonitor(on_locked=lambda: None, on_unlocked=lambda: None)
    caplog.set_level("INFO")
    _install_fake_windows(monkeypatch, monitor)

    monitor._handle_session_change_code(WTS_SESSION_LOCK)
    locked_status = monitor.get_status()
    monitor._handle_session_change_code(WTS_SESSION_UNLOCK)
    unlocked_status = monitor.get_status()

    assert locked_status["lock_state"] == "locked"
    assert unlocked_status["lock_state"] == "unlocked"
    assert any("event=session_locked" in rec.message for rec in caplog.records)
    assert any("event=session_unlocked" in rec.message for rec in caplog.records)
