from __future__ import annotations

import atexit
import faulthandler
import json
import logging
import os
import sys
import threading
import time
import traceback
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4
from types import TracebackType


def _flush_logging_handlers() -> None:
    root = logging.getLogger()
    for handler in root.handlers:
        try:
            handler.flush()
        except Exception:
            continue


class CrashReporter:
    """Install crash diagnostics hooks and track unclean shutdowns."""

    def __init__(self, app_data_dir: str, log_dir: str, logger: logging.Logger | None = None) -> None:
        self.app_data_dir = Path(app_data_dir)
        self.log_dir = Path(log_dir)
        self.logger = logger or logging.getLogger(__name__)
        self.marker_path = self.app_data_dir / "runtime_state.json"
        self.fault_log_path = self.log_dir / "fatal_fault.log"

        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._heartbeat_interval_seconds = 15.0
        self._marker_lock = threading.Lock()

        self._fault_log_stream: object | None = None
        self._installed = False
        self._clean_exit_marked = False

    def install(self) -> None:
        if self._installed:
            return

        self.app_data_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self._check_previous_run()
        self._mark_running()
        self._start_heartbeat()
        self._enable_faulthandler()
        self._install_exception_hooks()

        atexit.register(self._atexit_handler)
        self._installed = True

    def mark_clean_exit(self) -> None:
        self._stop_heartbeat()
        state = self._load_marker_state() or {}
        state.update(
            {
                "status": "CLEAN_EXIT",
                "clean_exit": True,
                "clean_exit_ts": time.time(),
                "pid": os.getpid(),
            }
        )
        self._write_marker_state(state)
        self._clean_exit_marked = True

    def _enable_faulthandler(self) -> None:
        try:
            self._fault_log_stream = self.fault_log_path.open("a", encoding="utf-8")
            faulthandler.enable(file=self._fault_log_stream, all_threads=True)
            self.logger.info(
                "[CRASH] stage=faulthandler_enable status=ok pid=%s output=%s",
                os.getpid(),
                self.fault_log_path,
            )
        except Exception as exc:
            faulthandler.enable(all_threads=True)
            self.logger.warning(
                "[CRASH] stage=faulthandler_enable status=degraded pid=%s error_type=%s error=%s",
                os.getpid(),
                exc.__class__.__name__,
                exc,
            )
        finally:
            _flush_logging_handlers()

    def _install_exception_hooks(self) -> None:
        def _main_thread_hook(
            exc_type: type[BaseException],
            exc_value: BaseException,
            exc_tb: TracebackType | None,
        ) -> None:
            tb_text = "".join(traceback.format_exception(exc_type, exc_value, exc_tb)).strip()
            self.logger.critical(
                "[CRASH] stage=unhandled_exception status=error pid=%s thread=%s error_type=%s error=%s\n%s",
                os.getpid(),
                threading.current_thread().name,
                exc_type.__name__,
                exc_value,
                tb_text,
            )
            _flush_logging_handlers()

        def _thread_hook(args: threading.ExceptHookArgs) -> None:
            exc_type = args.exc_type or Exception
            exc_value = args.exc_value or Exception("Unknown thread exception")
            tb_text = "".join(traceback.format_exception(exc_type, exc_value, args.exc_traceback)).strip()
            thread_name = args.thread.name if args.thread else "unknown"
            self.logger.critical(
                "[CRASH] stage=thread_unhandled_exception status=error pid=%s thread=%s error_type=%s error=%s\n%s",
                os.getpid(),
                thread_name,
                exc_type.__name__,
                exc_value,
                tb_text,
            )
            _flush_logging_handlers()

        sys.excepthook = _main_thread_hook
        threading.excepthook = _thread_hook

    def _check_previous_run(self) -> None:
        state = self._load_marker_state()
        if not state:
            return

        previous_status = str(state.get("status") or "")
        clean_exit = bool(state.get("clean_exit", False))
        if previous_status == "RUNNING" or not clean_exit:
            self.logger.warning(
                (
                    "[CRASH] stage=previous_run_check status=unclean_shutdown detected=true "
                    "pid=%s last_pid=%s last_start=%s last_heartbeat=%s"
                ),
                os.getpid(),
                state.get("pid"),
                state.get("start_ts"),
                state.get("heartbeat_ts"),
            )
            _flush_logging_handlers()

    def _mark_running(self) -> None:
        now = time.time()
        self._write_marker_state(
            {
                "status": "RUNNING",
                "clean_exit": False,
                "pid": os.getpid(),
                "start_ts": now,
                "heartbeat_ts": now,
            }
        )

    def _start_heartbeat(self) -> None:
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return

        self._heartbeat_stop.clear()

        def _heartbeat_loop() -> None:
            while not self._heartbeat_stop.wait(self._heartbeat_interval_seconds):
                state = self._load_marker_state() or {}
                state["heartbeat_ts"] = time.time()
                state["status"] = "RUNNING"
                state["clean_exit"] = False
                state.setdefault("pid", os.getpid())
                state.setdefault("start_ts", time.time())
                self._write_marker_state(state)

        self._heartbeat_thread = threading.Thread(target=_heartbeat_loop, name="CrashHeartbeat", daemon=True)
        self._heartbeat_thread.start()

    def _stop_heartbeat(self) -> None:
        self._heartbeat_stop.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=1.0)

    def _load_marker_state(self) -> dict | None:
        with self._marker_lock:
            try:
                if not self.marker_path.exists():
                    return None
                return json.loads(self.marker_path.read_text(encoding="utf-8"))
            except Exception:
                return None

    def _write_marker_state(self, state: dict) -> None:
        with self._marker_lock:
            temp_path = self.marker_path.with_name(f"{self.marker_path.stem}.{os.getpid()}.{uuid4().hex}.tmp")
            temp_path.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")
            temp_path.replace(self.marker_path)

    def _atexit_handler(self) -> None:
        if self._clean_exit_marked:
            return
        self.mark_clean_exit()



def run_protected(stage: str, logger: logging.Logger, func: Callable[[], int]) -> int:
    try:
        return int(func())
    except Exception as exc:
        logger.critical(
            "[CRASH] stage=%s status=error pid=%s thread=%s error_type=%s error=%s",
            stage,
            os.getpid(),
            threading.current_thread().name,
            exc.__class__.__name__,
            exc,
            exc_info=True,
        )
        _flush_logging_handlers()
        raise
