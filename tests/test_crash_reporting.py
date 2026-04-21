from __future__ import annotations

import json
import logging
import os
import sys
import threading
from pathlib import Path

from worklog_diary.core.crash_reporting import CrashReporter, run_protected


def _make_reporter(tmp_path: Path) -> CrashReporter:
    logger = logging.getLogger("test.crash")
    return CrashReporter(str(tmp_path / "app"), str(tmp_path / "logs"), logger)


def test_previous_run_unclean_shutdown_is_logged(tmp_path: Path, caplog) -> None:
    reporter = _make_reporter(tmp_path)
    reporter.app_data_dir.mkdir(parents=True, exist_ok=True)
    reporter.marker_path.write_text(
        json.dumps(
            {
                "status": "RUNNING",
                "clean_exit": False,
                "pid": 123,
                "start_ts": 1.0,
                "heartbeat_ts": 2.0,
            }
        ),
        encoding="utf-8",
    )

    caplog.set_level(logging.WARNING)
    reporter._check_previous_run()

    assert any("[CRASH] stage=previous_run_check status=unclean_shutdown detected=true" in r.message for r in caplog.records)


def test_main_and_thread_hooks_log_crash_records(tmp_path: Path, caplog, monkeypatch) -> None:
    reporter = _make_reporter(tmp_path)
    caplog.set_level(logging.CRITICAL)

    original_sys = sys.excepthook
    original_thread = threading.excepthook

    reporter._install_exception_hooks()

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

        assert any("[CRASH] stage=unhandled_exception status=error" in r.message for r in caplog.records)
        assert any("error_type=ValueError" in r.message for r in caplog.records)
        assert any("[CRASH] stage=thread_unhandled_exception status=error" in r.message for r in caplog.records)
        assert any("thread=Worker-1" in r.message for r in caplog.records)
    finally:
        monkeypatch.setattr(sys, "excepthook", original_sys)
        monkeypatch.setattr(threading, "excepthook", original_thread)


def test_mark_clean_exit_updates_state(tmp_path: Path) -> None:
    reporter = _make_reporter(tmp_path)
    reporter.app_data_dir.mkdir(parents=True, exist_ok=True)
    reporter._mark_running()

    reporter.mark_clean_exit()

    state = json.loads(reporter.marker_path.read_text(encoding="utf-8"))
    assert state["status"] == "CLEAN_EXIT"
    assert state["clean_exit"] is True
    assert int(state["pid"]) == os.getpid()


def test_mark_clean_exit_stops_heartbeat_before_marker_write(tmp_path: Path, monkeypatch) -> None:
    reporter = _make_reporter(tmp_path)
    reporter.app_data_dir.mkdir(parents=True, exist_ok=True)

    steps: list[str] = []

    def _stop() -> None:
        steps.append("stop")

    def _write(_state: dict) -> None:
        steps.append("write")

    monkeypatch.setattr(reporter, "_stop_heartbeat", _stop)
    monkeypatch.setattr(reporter, "_write_marker_state", _write)

    reporter.mark_clean_exit()

    assert steps == ["stop", "write"]


def test_marker_writes_are_safe_under_concurrency(tmp_path: Path) -> None:
    reporter = _make_reporter(tmp_path)
    reporter.app_data_dir.mkdir(parents=True, exist_ok=True)

    failures: list[Exception] = []

    def _writer(idx: int) -> None:
        try:
            for tick in range(50):
                reporter._write_marker_state({"status": "RUNNING", "idx": idx, "tick": tick})
        except Exception as exc:  # pragma: no cover - this path should remain unreachable
            failures.append(exc)

    threads = [threading.Thread(target=_writer, args=(i,)) for i in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert failures == []
    assert reporter.marker_path.exists()


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

    assert any("[CRASH] stage=app_main_loop status=error" in r.message for r in caplog.records)
