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
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Any
from uuid import uuid4

SESSION_STATE_SCHEMA_VERSION = 1


def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _local_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _flush_logging_handlers() -> None:
    root = logging.getLogger()
    for handler in root.handlers:
        try:
            handler.flush()
        except Exception:
            continue


def _safe_thread_name(thread: threading.Thread | None) -> str:
    if thread is None:
        return "unknown"
    try:
        return thread.name or "unknown"
    except Exception:
        return "unknown"


def append_emergency_marker(path: Path, marker: str) -> None:
    """Best-effort direct-write marker path that never raises."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(f"{_utc_now()} {marker}\n")
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        return


class CrashMonitor:
    """Crash observability primitives for runtime exceptions and session recovery."""

    def __init__(self, app_data_dir: str, log_dir: str, logger: logging.Logger | None = None) -> None:
        self.app_data_dir = Path(app_data_dir)
        self.log_dir = Path(log_dir)
        self.logger = logger or logging.getLogger(__name__)

        self.session_state_path = self.app_data_dir / "session_state.json"
        self.faulthandler_path = self.log_dir / "crash_faulthandler.log"
        self.last_gasp_path = self.log_dir / "crash_last_gasp.log"

        self._installed = False
        self._session_finalizing = False
        self._session_finalized = False
        self._session_lock = threading.Lock()
        self._heartbeat_interval_seconds = 15.0
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._fault_stream: Any | None = None

        self._session_id = str(uuid4())
        self._started_at_utc = _utc_now()
        self._started_at_local = _local_now()
        self._app_version = "unknown"

        self._original_sys_excepthook = sys.excepthook
        self._original_threading_excepthook = threading.excepthook
        self._original_unraisablehook = sys.unraisablehook

    def install(self, *, app_version: str | None = None) -> None:
        if self._installed:
            return
        self._app_version = app_version or "unknown"

        try:
            self.app_data_dir.mkdir(parents=True, exist_ok=True)
            self.log_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            self.logger.warning(
                "event=crash_monitor_stage_failed stage=ensure_directories error_type=%s error=%s",
                exc.__class__.__name__,
                exc,
            )
            append_emergency_marker(
                self.last_gasp_path,
                f"stage=ensure_directories_failed error_type={exc.__class__.__name__}",
            )

        self._run_install_stage(
            "previous_unclean_check",
            self._log_previous_unclean_shutdown_if_present,
        )
        self._run_install_stage(
            "session_state_initialize",
            lambda: self._initialize_session_state(app_version=app_version),
        )
        self.logger.info(
            "event=crash_monitor_session_start session_id=%s faulthandler_path=%s",
            self._session_id,
            self.faulthandler_path,
        )
        self._run_install_stage("faulthandler_enable", self._enable_faulthandler)
        self._run_install_stage("exception_hooks_install", self._install_exception_hooks)
        self._run_install_stage("heartbeat_start", self._start_heartbeat)
        self._run_install_stage("atexit_register", lambda: atexit.register(self.finalize_clean_shutdown))

        self._installed = True
        self.logger.info("event=crash_monitor_initialized session_id=%s", self._session_id)

    def finalize_clean_shutdown(self) -> None:
        with self._session_lock:
            if self._session_finalized:
                return
            self._session_finalizing = True
        self.logger.info("event=crash_monitor_finalize_start session_id=%s", self._session_id)
        self._append_faulthandler_marker("crash_monitor_finalize_start")

        self._stop_heartbeat()
        try:
            state = self._load_session_state() or {}
            state.update(
                {
                    "schema_version": SESSION_STATE_SCHEMA_VERSION,
                    "clean_shutdown": True,
                    "exit_reason": "clean_shutdown",
                    "exited_at_utc": _utc_now(),
                    "pid": os.getpid(),
                    "session_id": state.get("session_id", self._session_id),
                }
            )
            self._write_session_state(state)
            self._append_faulthandler_finalization_marker()
            self.logger.info("event=crash_monitor_session_finalized session_id=%s", state.get("session_id"))
            with self._session_lock:
                self._session_finalized = True
        except Exception as exc:
            self.logger.warning(
                "event=crash_monitor_finalize_failed error_type=%s error=%s",
                exc.__class__.__name__,
                exc,
            )

    def mark_clean_exit(self) -> None:
        self.finalize_clean_shutdown()

    def mark_shutdown_start(self) -> None:
        self._append_faulthandler_marker("shutdown_start")

    @property
    def session_id(self) -> str:
        return self._session_id

    def _install_exception_hooks(self) -> None:
        def _main_thread_hook(
            exc_type: type[BaseException],
            exc_value: BaseException,
            exc_tb: TracebackType | None,
        ) -> None:
            if issubclass(exc_type, KeyboardInterrupt):
                self._call_original_sys_hook(exc_type, exc_value, exc_tb)
                return
            self._log_uncaught_exception(
                stage="unhandled_exception",
                exc_type=exc_type,
                exc_value=exc_value,
                exc_traceback=exc_tb,
                thread_name=threading.current_thread().name,
            )
            self._call_original_sys_hook(exc_type, exc_value, exc_tb)

        def _thread_hook(args: threading.ExceptHookArgs) -> None:
            exc_type = args.exc_type or RuntimeError
            exc_value = args.exc_value or RuntimeError("unknown thread exception")
            if issubclass(exc_type, KeyboardInterrupt):
                self._call_original_thread_hook(args)
                return
            self._log_uncaught_exception(
                stage="thread_unhandled_exception",
                exc_type=exc_type,
                exc_value=exc_value,
                exc_traceback=args.exc_traceback,
                thread_name=_safe_thread_name(args.thread),
            )
            self._call_original_thread_hook(args)

        def _unraisable_hook(args: sys.UnraisableHookArgs) -> None:
            exc_type = args.exc_type or RuntimeError
            exc_value = args.exc_value or RuntimeError("unknown unraisable exception")
            if issubclass(exc_type, KeyboardInterrupt):
                self._call_original_unraisable_hook(args)
                return
            object_repr = "unknown"
            try:
                if args.object is not None:
                    object_repr = repr(args.object)
            except Exception:
                object_repr = "<repr-failed>"
            self._log_uncaught_exception(
                stage="unraisable_exception",
                exc_type=exc_type,
                exc_value=exc_value,
                exc_traceback=args.exc_traceback,
                thread_name=threading.current_thread().name,
                context=f"object={object_repr}",
            )
            self._call_original_unraisable_hook(args)

        sys.excepthook = _main_thread_hook
        threading.excepthook = _thread_hook
        sys.unraisablehook = _unraisable_hook
        self.logger.info("event=crash_hooks_installed")

    def _run_install_stage(self, stage: str, callback: Callable[[], None]) -> None:
        try:
            callback()
        except Exception as exc:
            self.logger.warning(
                "event=crash_monitor_stage_failed stage=%s error_type=%s error=%s",
                stage,
                exc.__class__.__name__,
                exc,
            )
            append_emergency_marker(
                self.last_gasp_path,
                f"stage={stage} error_type={exc.__class__.__name__}",
            )

    def _log_uncaught_exception(
        self,
        *,
        stage: str,
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_traceback: TracebackType | None,
        thread_name: str,
        context: str | None = None,
    ) -> None:
        try:
            tb_text = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback)).strip()
            extra_context = f" {context}" if context else ""
            self.logger.critical(
                (
                    "event=crash_exception stage=%s pid=%s thread=%s error_type=%s error=%s%s\n%s"
                ),
                stage,
                os.getpid(),
                thread_name,
                exc_type.__name__,
                exc_value,
                extra_context,
                tb_text,
            )
            append_emergency_marker(
                self.last_gasp_path,
                f"stage={stage} pid={os.getpid()} thread={thread_name} error_type={exc_type.__name__}",
            )
            _flush_logging_handlers()
        except Exception as hook_exc:
            append_emergency_marker(self.last_gasp_path, f"stage={stage} hook_failure={hook_exc.__class__.__name__}")

    def _call_original_sys_hook(
        self,
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_tb: TracebackType | None,
    ) -> None:
        try:
            self._original_sys_excepthook(exc_type, exc_value, exc_tb)
        except Exception:
            return

    def _call_original_thread_hook(self, args: threading.ExceptHookArgs) -> None:
        try:
            self._original_threading_excepthook(args)
        except Exception:
            return

    def _call_original_unraisable_hook(self, args: sys.UnraisableHookArgs) -> None:
        try:
            self._original_unraisablehook(args)
        except Exception:
            return

    def _enable_faulthandler(self) -> None:
        self._fault_stream = self.faulthandler_path.open("a", encoding="utf-8", buffering=1)
        self._write_faulthandler_header()
        faulthandler.enable(file=self._fault_stream, all_threads=True)
        self._append_faulthandler_marker("faulthandler_enabled")
        self.logger.info(
            "event=crash_monitor_faulthandler_enabled session_id=%s path=%s",
            self._session_id,
            self.faulthandler_path,
        )

    def _log_previous_unclean_shutdown_if_present(self) -> None:
        state = self._load_session_state()
        if state is None:
            return

        was_clean = bool(state.get("clean_shutdown"))
        if was_clean:
            return

        self.logger.warning(
            (
                "event=previous_run_unexpected_exit detected=true last_session_id=%s last_pid=%s "
                "last_started_at_utc=%s last_heartbeat_utc=%s last_app_version=%s"
            ),
            state.get("session_id"),
            state.get("pid"),
            state.get("started_at_utc"),
            state.get("last_heartbeat_utc"),
            state.get("app_version"),
        )

    def _initialize_session_state(self, *, app_version: str | None = None) -> None:
        state = {
            "schema_version": SESSION_STATE_SCHEMA_VERSION,
            "session_id": self._session_id,
            "app_version": app_version,
            "pid": os.getpid(),
            "started_at_utc": self._started_at_utc,
            "started_at_local": self._started_at_local,
            "last_heartbeat_utc": self._started_at_utc,
            "clean_shutdown": False,
            "exit_reason": "unknown",
            "executable": sys.executable,
        }
        self._write_session_state(state)

    def _start_heartbeat(self) -> None:
        with self._session_lock:
            if self._session_finalizing or self._session_finalized:
                return

        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return

        self._heartbeat_stop.clear()

        def _loop() -> None:
            while not self._heartbeat_stop.wait(self._heartbeat_interval_seconds):
                with self._session_lock:
                    if self._session_finalizing or self._session_finalized:
                        return
                try:
                    state = self._load_session_state() or {}
                    state["schema_version"] = SESSION_STATE_SCHEMA_VERSION
                    state["clean_shutdown"] = False
                    state["exit_reason"] = "running"
                    state.setdefault("session_id", self._session_id)
                    state.setdefault("started_at_utc", self._started_at_utc)
                    state["pid"] = os.getpid()
                    state["last_heartbeat_utc"] = _utc_now()
                    self._write_session_state(state)
                except Exception as exc:
                    self.logger.warning(
                        "event=session_heartbeat_update_failed error_type=%s error=%s",
                        exc.__class__.__name__,
                        exc,
                    )

        self._heartbeat_thread = threading.Thread(target=_loop, name="CrashMonitorHeartbeat", daemon=True)
        self._heartbeat_thread.start()
        self.logger.info("event=session_heartbeat_started interval_seconds=%s", self._heartbeat_interval_seconds)

    def _stop_heartbeat(self) -> None:
        self._heartbeat_stop.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join()
        self._heartbeat_thread = None

    def _load_session_state(self) -> dict[str, Any] | None:
        with self._session_lock:
            if not self.session_state_path.exists():
                return None
            try:
                return json.loads(self.session_state_path.read_text(encoding="utf-8"))
            except Exception as exc:
                self.logger.warning(
                    "event=session_state_load_failed path=%s error_type=%s error=%s",
                    self.session_state_path,
                    exc.__class__.__name__,
                    exc,
                )
                append_emergency_marker(self.last_gasp_path, "stage=session_state_load_failed")
                return None

    def _write_session_state(self, state: dict[str, Any]) -> None:
        with self._session_lock:
            temp_path = self.session_state_path.with_name(
                f"{self.session_state_path.name}.{os.getpid()}.{uuid4().hex}.tmp"
            )
            payload = json.dumps(state, sort_keys=True, separators=(",", ":"))
            with temp_path.open("w", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            temp_path.replace(self.session_state_path)
            self._fsync_parent_directory(self.session_state_path.parent)

    def _fsync_parent_directory(self, directory: Path) -> None:
        try:
            dir_fd = os.open(str(directory), os.O_RDONLY)
        except Exception:
            return
        try:
            os.fsync(dir_fd)
        except Exception:
            return
        finally:
            try:
                os.close(dir_fd)
            except Exception:
                return

    def _fault_stream_handle(self) -> Any | None:
        if self._fault_stream is not None:
            return self._fault_stream
        try:
            self._fault_stream = self.faulthandler_path.open("a", encoding="utf-8", buffering=1)
            return self._fault_stream
        except Exception:
            return None

    def _write_fault_line(self, line: str) -> None:
        handle = self._fault_stream_handle()
        if handle is None:
            return
        try:
            handle.write(f"{line}\n")
            handle.flush()
            os.fsync(handle.fileno())
        except Exception:
            return

    def _write_faulthandler_header(self) -> None:
        header_lines = [
            "# WorkLog Diary faulthandler log",
            f"# session_id={self._session_id}",
            f"# process_start_utc={self._started_at_utc}",
            f"# process_start_local={self._started_at_local}",
            f"# pid={os.getpid()}",
            f"# app_version={self._app_version}",
        ]
        for line in header_lines:
            self._write_fault_line(line)

    def _append_faulthandler_marker(self, event: str) -> None:
        self._write_fault_line(f"# marker_utc={_utc_now()} event={event} session_id={self._session_id}")

    def _append_faulthandler_finalization_marker(self) -> None:
        self._write_fault_line(f"# clean_finalization_utc={_utc_now()} session_id={self._session_id}")
        self._write_fault_line(f"# clean_finalization_local={_local_now()} session_id={self._session_id}")



def run_protected(stage: str, logger: logging.Logger, func: Callable[[], int]) -> int:
    try:
        return int(func())
    except Exception as exc:
        logger.critical(
            "event=run_protected_exception stage=%s status=error pid=%s thread=%s error_type=%s error=%s",
            stage,
            os.getpid(),
            threading.current_thread().name,
            exc.__class__.__name__,
            exc,
            exc_info=True,
        )
        _flush_logging_handlers()
        raise
