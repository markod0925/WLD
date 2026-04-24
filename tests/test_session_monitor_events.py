from __future__ import annotations

from types import SimpleNamespace

from worklog_diary.core.session_monitor import SessionMonitor, WTS_SESSION_LOCK, WTS_SESSION_UNLOCK


def test_session_monitor_maps_lock_and_unlock_codes_to_callbacks() -> None:
    events: list[str] = []
    monitor = SessionMonitor(
        on_locked=lambda: events.append("locked"),
        on_unlocked=lambda: events.append("unlocked"),
    )

    monitor._handle_session_change_code(WTS_SESSION_LOCK)
    monitor._handle_session_change_code(WTS_SESSION_UNLOCK)
    monitor._handle_session_change_code(999)

    assert events == ["locked", "unlocked"]


def test_session_monitor_start_failure_is_logged(monkeypatch, caplog) -> None:
    monitor = SessionMonitor(on_locked=lambda: None, on_unlocked=lambda: None)
    caplog.set_level("INFO")
    monkeypatch.setattr("worklog_diary.core.session_monitor.ctypes.windll", None, raising=False)
    monitor._run_windows_loop()
    assert any("event=session_monitor_start_failed" in rec.message for rec in caplog.records)
    assert any("event=session_monitor_thread_exit" in rec.message for rec in caplog.records)


def test_session_monitor_start_success_marker(monkeypatch, caplog) -> None:
    monitor = SessionMonitor(on_locked=lambda: None, on_unlocked=lambda: None)
    caplog.set_level("INFO")
    monkeypatch.setattr("worklog_diary.core.session_monitor.os", SimpleNamespace(name="nt", getpid=lambda: 1))
    monkeypatch.setattr("worklog_diary.core.session_monitor.native_hooks_disabled", lambda: False)

    def _fake_run() -> None:
        monitor.logger.info("event=session_monitor_started ok=true")
        monitor._startup_event.set()
        monitor.logger.info("event=session_monitor_thread_exit reason=test")

    monitor._run_windows_loop = _fake_run  # type: ignore[method-assign]
    monitor.start()
    if monitor._thread is not None:
        monitor._thread.join(timeout=1.0)
    assert any("event=session_monitor_started ok=true" in rec.message for rec in caplog.records)
