from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

from worklog_diary.core.crash_monitor import CrashMonitor, append_emergency_marker, run_protected
from worklog_diary.core.services import MonitoringServices


def _make_monitor(tmp_path: Path) -> CrashMonitor:
    logger = logging.getLogger("test.crash_monitor")
    return CrashMonitor(str(tmp_path / "app"), str(tmp_path / "logs"), logger)


def test_previous_run_unclean_shutdown_is_logged(tmp_path: Path, caplog) -> None:
    monitor = _make_monitor(tmp_path)
    monitor.app_data_dir.mkdir(parents=True, exist_ok=True)
    monitor.session_state_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "session_id": "old-session",
                "pid": 123,
                "started_at_utc": "2026-01-01T00:00:00Z",
                "last_heartbeat_utc": "2026-01-01T00:00:15Z",
                "clean_shutdown": False,
                "app_version": "0.0.1",
            }
        ),
        encoding="utf-8",
    )

    caplog.set_level(logging.WARNING)
    monitor._log_previous_unclean_shutdown_if_present()

    assert any("event=previous_run_unexpected_exit detected=true" in r.message for r in caplog.records)


def test_clean_shutdown_marks_session_state(tmp_path: Path) -> None:
    monitor = _make_monitor(tmp_path)
    monitor.app_data_dir.mkdir(parents=True, exist_ok=True)
    monitor._initialize_session_state(app_version="0.1.0")

    monitor.finalize_clean_shutdown()

    state = json.loads(monitor.session_state_path.read_text(encoding="utf-8"))
    assert state["clean_shutdown"] is True
    assert state["exit_reason"] == "clean_shutdown"
    assert int(state["pid"]) == os.getpid()


def test_corrupt_session_state_is_tolerated(tmp_path: Path, caplog) -> None:
    monitor = _make_monitor(tmp_path)
    monitor.app_data_dir.mkdir(parents=True, exist_ok=True)
    monitor.log_dir.mkdir(parents=True, exist_ok=True)
    monitor.session_state_path.write_text("{not-json", encoding="utf-8")

    caplog.set_level(logging.WARNING)

    assert monitor._load_session_state() is None
    assert any("event=session_state_load_failed" in r.message for r in caplog.records)


def test_session_writes_are_safe_under_concurrency(tmp_path: Path) -> None:
    monitor = _make_monitor(tmp_path)
    monitor.app_data_dir.mkdir(parents=True, exist_ok=True)

    failures: list[Exception] = []

    def _writer(idx: int) -> None:
        try:
            for tick in range(50):
                monitor._write_session_state({"session_id": idx, "tick": tick, "clean_shutdown": False})
        except Exception as exc:  # pragma: no cover
            failures.append(exc)

    threads = [threading.Thread(target=_writer, args=(i,)) for i in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert failures == []
    assert monitor.session_state_path.exists()


def test_emergency_marker_never_raises_when_open_fails(tmp_path: Path, monkeypatch) -> None:
    marker_path = tmp_path / "crash_last_gasp.log"

    def _boom(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        raise OSError("open failed")

    monkeypatch.setattr(Path, "open", _boom)
    append_emergency_marker(marker_path, "test-marker")


def test_heartbeat_updates_session_state(tmp_path: Path) -> None:
    monitor = _make_monitor(tmp_path)
    monitor.app_data_dir.mkdir(parents=True, exist_ok=True)
    monitor._heartbeat_interval_seconds = 0.01
    monitor._initialize_session_state(app_version="0.1.0")

    monitor._start_heartbeat()
    try:
        threading.Event().wait(0.03)
    finally:
        monitor._stop_heartbeat()

    state = json.loads(monitor.session_state_path.read_text(encoding="utf-8"))
    assert state["clean_shutdown"] is False
    assert "last_heartbeat_utc" in state


def test_finalize_prevents_late_heartbeat_overwrite(tmp_path: Path, monkeypatch) -> None:
    monitor = _make_monitor(tmp_path)
    monitor.app_data_dir.mkdir(parents=True, exist_ok=True)
    monitor._heartbeat_interval_seconds = 0.01
    monitor._initialize_session_state(app_version="0.1.0")

    original_write = monitor._write_session_state
    heartbeat_started = threading.Event()
    release_heartbeat_write = threading.Event()
    writes: list[dict] = []

    def _wrapped_write(state: dict) -> None:
        writes.append(dict(state))
        if state.get("exit_reason") == "running" and not release_heartbeat_write.is_set():
            heartbeat_started.set()
            release_heartbeat_write.wait(timeout=1.0)
        original_write(state)

    monkeypatch.setattr(monitor, "_write_session_state", _wrapped_write)

    monitor._start_heartbeat()
    assert heartbeat_started.wait(timeout=1.0)

    finalize_thread = threading.Thread(target=monitor.finalize_clean_shutdown)
    finalize_thread.start()
    time.sleep(0.02)
    release_heartbeat_write.set()
    finalize_thread.join(timeout=1.0)

    state = json.loads(monitor.session_state_path.read_text(encoding="utf-8"))
    assert state["clean_shutdown"] is True

    first_clean_idx = next(i for i, item in enumerate(writes) if item.get("clean_shutdown") is True)
    assert not any(item.get("clean_shutdown") is False for item in writes[first_clean_idx + 1 :])


def test_finalize_retries_after_failed_persist(tmp_path: Path, monkeypatch) -> None:
    monitor = _make_monitor(tmp_path)
    monitor.app_data_dir.mkdir(parents=True, exist_ok=True)
    monitor._initialize_session_state(app_version="0.1.0")

    original_write = monitor._write_session_state
    attempts = {"count": 0}

    def _flaky_write(state: dict) -> None:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise OSError("transient write failure")
        original_write(state)

    monkeypatch.setattr(monitor, "_write_session_state", _flaky_write)

    monitor.finalize_clean_shutdown()
    monitor.finalize_clean_shutdown()

    state = json.loads(monitor.session_state_path.read_text(encoding="utf-8"))
    assert attempts["count"] >= 2
    assert state["clean_shutdown"] is True


def test_install_is_non_fatal_when_stages_fail(tmp_path: Path, monkeypatch) -> None:
    monitor = _make_monitor(tmp_path)

    monkeypatch.setattr(monitor, "_initialize_session_state", lambda app_version=None: (_ for _ in ()).throw(OSError("write fail")))
    monkeypatch.setattr(monitor, "_enable_faulthandler", lambda: (_ for _ in ()).throw(RuntimeError("fault fail")))
    monkeypatch.setattr(monitor, "_install_exception_hooks", lambda: (_ for _ in ()).throw(RuntimeError("hook fail")))

    monitor.install(app_version="0.1.0")

    assert monitor._installed is True


def test_services_shutdown_attempts_clean_mark_even_if_teardown_raises() -> None:
    services = MonitoringServices.__new__(MonitoringServices)
    services._shutdown_completed = False
    services.logger = logging.getLogger("test.services.shutdown")
    services._services = SimpleNamespace(shutdown_event=threading.Event())
    services.cancel_flush_drain = lambda: False
    services.scheduler = SimpleNamespace(stop=lambda: (_ for _ in ()).throw(RuntimeError("stop failed")))
    services.summarizer = SimpleNamespace(stop=lambda: None, stop_accepting_new_jobs=lambda: None, get_runtime_status=lambda: {})
    services.window_tracker = SimpleNamespace(stop=lambda: None)
    services.keyboard_capture = SimpleNamespace(stop=lambda: None)
    services.text_service = SimpleNamespace(stop=lambda: None)
    services.screenshot_capture = SimpleNamespace(stop=lambda: None)
    services.session_monitor = None
    services.storage = SimpleNamespace(close=lambda: None)
    calls: list[str] = []
    services.crash_reporter = SimpleNamespace(mark_clean_exit=lambda: calls.append("marked"))

    try:
        services.shutdown()
    except RuntimeError:
        pass
    else:
        raise AssertionError("shutdown should surface teardown failure")

    assert calls == ["marked"]


def test_services_shutdown_closes_storage_after_summary_stop() -> None:
    services = MonitoringServices.__new__(MonitoringServices)
    services._shutdown_completed = False
    services.logger = logging.getLogger("test.services.order")
    services._services = SimpleNamespace(shutdown_event=threading.Event())
    order: list[str] = []
    services.cancel_flush_drain = lambda: order.append("cancel_drain")
    services.scheduler = SimpleNamespace(stop=lambda: order.append("scheduler_stop"))
    services.summarizer = SimpleNamespace(
        stop=lambda: order.append("summarizer_stop"),
        stop_accepting_new_jobs=lambda: order.append("stop_accepting"),
        get_runtime_status=lambda: {},
    )
    services.window_tracker = SimpleNamespace(stop=lambda: order.append("window_stop"))
    services.keyboard_capture = SimpleNamespace(stop=lambda: order.append("keyboard_stop"))
    services.text_service = SimpleNamespace(stop=lambda: order.append("text_stop"))
    services.screenshot_capture = SimpleNamespace(stop=lambda: order.append("screenshot_stop"))
    services.session_monitor = None
    services.storage = SimpleNamespace(close=lambda: order.append("storage_close"))
    services.crash_reporter = SimpleNamespace(mark_clean_exit=lambda: order.append("mark_clean_exit"))

    services.shutdown()

    assert order.index("summarizer_stop") < order.index("storage_close")
    assert order.index("mark_clean_exit") < order.index("storage_close")


def test_shutdown_continues_after_teardown_error_and_keeps_worker_barrier() -> None:
    services = MonitoringServices.__new__(MonitoringServices)
    services._shutdown_completed = False
    services.logger = logging.getLogger("test.services.shutdown-barrier")
    services._services = SimpleNamespace(shutdown_event=threading.Event())
    order: list[str] = []
    services.cancel_flush_drain = lambda: order.append("cancel_drain")
    services.scheduler = SimpleNamespace(stop=lambda: order.append("scheduler_stop"))
    services.summarizer = SimpleNamespace(
        stop=lambda: order.append("summarizer_stop"),
        stop_accepting_new_jobs=lambda: order.append("stop_accepting"),
        get_runtime_status=lambda: {"running_jobs": 0, "queued_jobs": 0, "completed_jobs": 0, "failed_jobs": 0, "cancelled_jobs": 0},
    )
    services.window_tracker = SimpleNamespace(stop=lambda: (_ for _ in ()).throw(RuntimeError("window stop failed")))
    services.keyboard_capture = SimpleNamespace(stop=lambda: order.append("keyboard_stop"))
    services.text_service = SimpleNamespace(stop=lambda: order.append("text_stop"))
    services.screenshot_capture = SimpleNamespace(stop=lambda: order.append("screenshot_stop"))
    services.session_monitor = None
    services.storage = SimpleNamespace(close=lambda: order.append("storage_close"))
    services.crash_reporter = SimpleNamespace(mark_clean_exit=lambda: order.append("mark_clean_exit"))

    try:
        services.shutdown()
    except RuntimeError:
        pass

    assert "summarizer_stop" in order
    assert "mark_clean_exit" in order
    assert "storage_close" in order


def test_hooks_capture_main_thread_thread_and_unraisable(tmp_path: Path, caplog, monkeypatch) -> None:
    monitor = _make_monitor(tmp_path)
    monitor.log_dir.mkdir(parents=True, exist_ok=True)
    caplog.set_level(logging.CRITICAL)

    original_sys = sys.excepthook
    original_thread = threading.excepthook
    original_unraisable = sys.unraisablehook

    monitor._install_exception_hooks()

    try:
        try:
            raise ValueError("main boom")
        except ValueError as exc:
            sys.excepthook(exc.__class__, exc, exc.__traceback__)

        def _boom() -> None:
            raise RuntimeError("thread boom")

        thread = threading.Thread(target=_boom, name="Worker-1")
        thread.start()
        thread.join()

        class _Unraisable:
            def __del__(self) -> None:
                raise RuntimeError("del boom")

        obj = _Unraisable()
        del obj

        assert any("stage=unhandled_exception" in r.message for r in caplog.records)
        assert any("stage=thread_unhandled_exception" in r.message for r in caplog.records)
        assert any("stage=unraisable_exception" in r.message for r in caplog.records)
    finally:
        monkeypatch.setattr(sys, "excepthook", original_sys)
        monkeypatch.setattr(threading, "excepthook", original_thread)
        monkeypatch.setattr(sys, "unraisablehook", original_unraisable)


def test_run_protected_logs_and_reraises(caplog) -> None:
    caplog.set_level(logging.CRITICAL)
    logger = logging.getLogger("test.crash.run")

    def _boom() -> int:
        raise RuntimeError("protected boom")

    try:
        run_protected("app_main_loop", logger, _boom)
    except RuntimeError:
        pass
    else:
        raise AssertionError("run_protected should re-raise runtime errors")

    assert any("event=run_protected_exception stage=app_main_loop" in r.message for r in caplog.records)
