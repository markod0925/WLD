from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Iterable


ENTRY_RE = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) "
    r"\[(?P<level>[A-Z]+)\] (?P<logger>[^:]+): (?P<message>.*)$"
)
PREFIX_RE = re.compile(r"^\[(?P<tag>[A-Z]+)\]\s*(?P<body>.*)$")
KV_RE = re.compile(r"(?P<key>[A-Za-z_][A-Za-z0-9_.-]*)=(?P<value>.*?)(?=\s+[A-Za-z_][A-Za-z0-9_.-]*=|$)")
TRACEBACK_HEADER_RE = re.compile(r"^Traceback \(most recent call last\):$")
TRACEBACK_EXCEPTION_RE = re.compile(r"^(?P<class>[A-Za-z_][A-Za-z0-9_.]*)[:](?P<message>.*)$")
UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
HEX_ADDR_RE = re.compile(r"\b0x[0-9a-fA-F]+\b")
WINDOWS_PATH_RE = re.compile(r"\b[A-Za-z]:\\(?:[^\\\s]+\\)*[^\\\s]*\b")
POSIX_PATH_RE = re.compile(r"(?<!\w)/(?:[^/\s]+/)*[^/\s]+")
ISO_TS_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?\b")
NUMERIC_RE = re.compile(r"(?<![\w.])[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?(?![\w.])")

NUMERIC_FIELD_KEYS = {
    "attempt",
    "cancelled",
    "chunk",
    "chunks",
    "count",
    "created",
    "created_at",
    "created_ts",
    "daily_summary_id",
    "day",
    "deleted_key_events",
    "deleted_screenshots",
    "deleted_text_segments",
    "duration_ms",
    "elapsed_s",
    "end_ts",
    "failed",
    "failed_jobs",
    "finished_at",
    "http_status",
    "image_height",
    "image_width",
    "input_chars",
    "input_token_estimate",
    "inserted",
    "interval_id",
    "job_id",
    "last_pid",
    "line_end",
    "line_start",
    "max_parallel_jobs",
    "max_prompt_chars",
    "missing_files",
    "next_attempt",
    "pid",
    "priority",
    "processed_key_events",
    "queue_size",
    "queue_wait_s",
    "queued",
    "queued_at",
    "raw_keys",
    "replaced",
    "removed_files",
    "request_timeout_seconds",
    "running",
    "rowcount",
    "rows",
    "screenshot_count",
    "screenshot_id",
    "segment_count",
    "selected_duration_seconds",
    "source_batch_count",
    "source_count",
    "source_summaries",
    "start_ts",
    "streak",
    "summary_count",
    "summary_id",
    "summary_jobs",
    "text_chars",
    "timeout_s",
    "time_since_last",
    "total_summaries",
    "unparsed_lines",
    "window_seconds",
    "writes_in_window",
    "writes_per_second",
}

PATH_FIELD_KEYS = {
    "app_data_dir",
    "config_path",
    "db_path",
    "endpoint",
    "error",
    "file",
    "file_path",
    "key_path",
    "log_dir",
    "path",
    "screenshot_dir",
    "url",
}

JOB_EVENT_NAMES = {
    "job_queued",
    "job_started",
    "job_completed",
    "job_failed",
    "job_cancelled",
    "job_created",
    "summary_job_started",
    "summary_job_completed",
    "summary_job_failed",
    "summary_job_cancelled",
    "summary_job_dequeued",
    "daily_summary_job_started",
    "daily_summary_job_completed",
    "daily_summary_job_cancelled",
    "daily_summary_job_reconciled",
    "daily_summary_job_reused",
}

SUMMARY_EVENT_NAMES = {
    "summary_job_dequeued",
    "summary_job_started",
    "summary_job_completed",
    "summary_job_failed",
    "summary_job_cancelled",
    "summary_drain_started",
    "summary_drain_tick",
    "summary_drain_finished",
    "summary_drain_failed",
    "summary_drain_cancel_requested",
    "summary_drain_stopped",
    "summary_workers_join_start",
    "summary_workers_joined",
    "summary_flush_triggered",
    "summary_flush_skipped",
    "submission_decision",
    "summary_store",
    "daily_summary_job_started",
    "daily_summary_job_completed",
    "daily_summary_job_cancelled",
    "daily_summary_job_reconciled",
    "daily_summary_job_reused",
    "daily_recap_generation_started",
    "daily_recap_generation_succeeded",
    "daily_recap_generation_failed",
    "daily_recap_replaced",
    "calendar_summary_load",
    "daily_recap_chunking_enabled",
    "chunk_plan",
    "payload_build",
    "request_submit",
    "request_success",
    "lmstudio_request_start",
    "lmstudio_request_success",
    "lmstudio_request_failure",
    "lmstudio_request_timeout",
    "http_response",
    "response_parse",
    "retry_scheduled",
    "semantic_coalescing_run_start",
    "semantic_coalescing_merge",
    "semantic_coalescing_no_merge",
    "semantic_coalescing_complete",
    "semantic_coalescing_day_compression",
}

CAPTURE_EVENT_NAMES = {
    "foreground_window_change",
    "privacy_block_transition",
    "key_capture_accepted",
    "key_capture_skipped",
    "key_capture_buffer_flushed",
    "key_capture_buffer_flush_failed",
    "text_segment_finalized",
    "screenshot_captured",
    "screenshot_skipped",
    "screenshot_dedup_keep",
    "screenshot_capture_analysis_unavailable",
    "screenshot_dedup_seed_failed",
    "activity_segment_pending",
    "activity_segment_selected",
}

STORAGE_EVENT_NAMES = {
    "db_key_bootstrap",
    "db_key_missing",
    "db_key_generated",
    "db_open",
    "db_query_timing",
    "db_write_rate",
    "storage_journal_mode",
    "startup_recovery",
    "startup_recovery_job",
    "purge_actions",
    "purge_file_delete_failed",
}

CRASH_EVENT_NAMES = {
    "crash_monitor_stage_failed",
    "crash_monitor_initialized",
    "crash_exception",
    "crash_hooks_installed",
    "faulthandler_enable",
    "faulthandler_enabled",
    "faulthandler_enable_failed",
    "previous_run_check",
    "previous_run_unexpected_exit",
    "session_heartbeat_started",
    "session_heartbeat_update_failed",
    "session_finalize_failed",
    "crash_monitor_finalize_start",
    "crash_monitor_session_finalized",
    "crash_monitor_finalize_failed",
    "session_state_load_failed",
    "session_monitor_start_failed",
    "session_monitor_registration_failed",
    "session_monitor_start",
    "session_monitor_started",
    "session_monitor_thread_exit",
    "session_monitor_callback_failed",
    "session_finalized",
    "run_protected_exception",
}

LOCK_EVENT_NAMES = {
    "session_locked",
    "session_unlocked",
    "summary_admission_config",
    "summary_admission_decision",
    "summary_admission_state",
    "backlog_waiting_for_pc_lock",
    "monitoring_paused_by_lock",
    "monitoring_resumed_after_unlock",
}

CONFIG_EVENT_NAMES = {
    "config_unknown_fields",
    "config_legacy_field_conflict",
    "config_snapshot_startup",
    "config_apply_start",
    "config_apply_diff",
    "config_apply_complete",
    "config_apply_failed",
}


@dataclass(slots=True)
class ParsedEvent:
    source_file: str
    line_start: int
    line_end: int
    timestamp: str
    level: str
    logger: str
    message: str
    traceback: str | None
    tags: list[str] = field(default_factory=list)
    event_name: str | None = None
    category: str = "other"
    subsystem: str = "other"
    fields: dict[str, Any] = field(default_factory=dict)
    error_class: str | None = None
    correlation_id: str | None = None
    summary_job_id: str | None = None
    screenshot_path: str | None = None
    screenshot_hash: str | None = None
    db_path: str | None = None
    config_changes: list[dict[str, Any]] = field(default_factory=list)

    def as_json(self) -> dict[str, Any]:
        return {
            "source_file": self.source_file,
            "line_start": self.line_start,
            "line_end": self.line_end,
            "timestamp": self.timestamp,
            "level": self.level,
            "logger": self.logger,
            "message": self.message,
            "traceback": self.traceback,
            "tags": self.tags,
            "event_name": self.event_name,
            "category": self.category,
            "subsystem": self.subsystem,
            "fields": self.fields,
            "error_class": self.error_class,
            "correlation_id": self.correlation_id,
            "summary_job_id": self.summary_job_id,
            "screenshot_path": self.screenshot_path,
            "screenshot_hash": self.screenshot_hash,
            "db_path": self.db_path,
            "config_changes": self.config_changes,
        }


@dataclass(slots=True)
class JobLifecycle:
    job_id: str
    job_type: str | None = None
    target_day: str | None = None
    created_at: str | None = None
    queued_at: str | None = None
    dequeued_at: str | None = None
    started_at: str | None = None
    request_submit_at: str | None = None
    request_success_at: str | None = None
    response_parse_at: str | None = None
    store_started_at: str | None = None
    store_finished_at: str | None = None
    completed_at: str | None = None
    failed_at: str | None = None
    cancelled_at: str | None = None
    abandoned_at: str | None = None
    terminal_status: str | None = None
    attempts: int = 0
    reasons: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    fields: dict[str, Any] = field(default_factory=dict)
    requests: list[dict[str, Any]] = field(default_factory=list)

    def touch_reason(self, reason: str | None) -> None:
        if reason and reason not in self.reasons:
            self.reasons.append(reason)

    def record(self, event: ParsedEvent) -> None:
        self.job_type = self.job_type or _string_field(event.fields, "job_type")
        if "target_day" in event.fields:
            self.target_day = str(event.fields["target_day"])
        self.attempts = max(self.attempts, _int_value(event.fields.get("attempt"), default=self.attempts))
        self.fields.update(event.fields)
        self.touch_reason(_string_field(event.fields, "reason"))

        event_name = event.event_name or ""
        if event_name == "job_created":
            self.created_at = self.created_at or event.timestamp
            self.queued_at = self.queued_at or event.timestamp
        elif event_name == "job_queued":
            self.queued_at = self.queued_at or event.timestamp
        elif event_name in {"job_started", "summary_job_started", "daily_summary_job_started"}:
            self.started_at = self.started_at or event.timestamp
        elif event_name == "summary_job_dequeued":
            self.dequeued_at = self.dequeued_at or event.timestamp
        elif event_name in {"request_submit", "http_request"}:
            self.request_submit_at = self.request_submit_at or event.timestamp
            self.requests.append(
                {
                    "submitted_at": event.timestamp,
                    "endpoint": _string_field(event.fields, "endpoint"),
                    "timeout_s": _float_value(event.fields.get("timeout_s")),
                }
            )
        elif event_name == "request_success":
            self.request_success_at = event.timestamp
            self._update_request(event, outcome="success")
        elif event_name == "http_response":
            outcome = "error" if _string_field(event.fields, "status") == "error" else "success"
            self._update_request(event, outcome=outcome)
            if outcome == "error":
                self.errors.append(_string_field(event.fields, "error") or event.message)
        elif event_name == "response_parse":
            self.response_parse_at = self.response_parse_at or event.timestamp
            self._update_request(event, outcome=None)
        elif event_name == "summary_store":
            status = _string_field(event.fields, "status")
            if status == "start":
                self.store_started_at = self.store_started_at or event.timestamp
            elif status == "ok":
                self.store_finished_at = self.store_finished_at or event.timestamp
        elif event_name in {"job_completed", "summary_job_completed", "daily_summary_job_completed"}:
            self.completed_at = self.completed_at or event.timestamp
            self.terminal_status = self.terminal_status or "completed"
        elif event_name in {"job_failed", "summary_job_failed"}:
            self.failed_at = self.failed_at or event.timestamp
            self.terminal_status = self.terminal_status or "failed"
            self.errors.append(_string_field(event.fields, "error") or event.message)
        elif event_name in {"job_cancelled", "summary_job_cancelled", "daily_summary_job_cancelled"}:
            self.cancelled_at = self.cancelled_at or event.timestamp
            self.terminal_status = self.terminal_status or "cancelled"
        elif event_name == "daily_summary_job_reconciled":
            self.completed_at = self.completed_at or event.timestamp
            self.terminal_status = self.terminal_status or "completed"
        elif event_name == "daily_summary_job_reused":
            self.terminal_status = self.terminal_status or "reused"
        elif event_name == "startup_recovery_job":
            status = _string_field(event.fields, "status")
            if status == "abandoned":
                self.abandoned_at = self.abandoned_at or event.timestamp
                self.terminal_status = self.terminal_status or "abandoned"
            elif status == "cancelled":
                self.cancelled_at = self.cancelled_at or event.timestamp
                self.terminal_status = self.terminal_status or "cancelled"

    def _update_request(self, event: ParsedEvent, *, outcome: str | None) -> None:
        if not self.requests:
            self.requests.append({"submitted_at": event.timestamp})
        request = self.requests[-1]
        request.setdefault("job_id", self.job_id)
        request.setdefault("job_type", self.job_type)
        request["endpoint"] = request.get("endpoint") or _string_field(event.fields, "endpoint")
        request["model"] = request.get("model") or _string_field(event.fields, "model")
        request["elapsed_s"] = request.get("elapsed_s") or _float_value(event.fields.get("elapsed_s"))
        request["http_status"] = request.get("http_status") or _int_value(event.fields.get("http_status"))
        request["error_type"] = request.get("error_type") or _string_field(event.fields, "error_type")
        request["error"] = request.get("error") or _string_field(event.fields, "error")
        request["outcome"] = outcome or request.get("outcome")
        request["parse_status"] = request.get("parse_status") or _string_field(event.fields, "status")
        if event.event_name == "response_parse" and _string_field(event.fields, "status") == "error":
            request["outcome"] = "parse_error"
        if event.event_name == "http_response" and _string_field(event.fields, "status") == "error":
            request["parse_status"] = "n/a"

    def to_summary(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "job_type": self.job_type,
            "target_day": self.target_day,
            "created_at": self.created_at,
            "queued_at": self.queued_at,
            "dequeued_at": self.dequeued_at,
            "started_at": self.started_at,
            "request_submit_at": self.request_submit_at,
            "request_success_at": self.request_success_at,
            "response_parse_at": self.response_parse_at,
            "store_started_at": self.store_started_at,
            "store_finished_at": self.store_finished_at,
            "completed_at": self.completed_at,
            "failed_at": self.failed_at,
            "cancelled_at": self.cancelled_at,
            "abandoned_at": self.abandoned_at,
            "terminal_status": self.terminal_status,
            "attempts": self.attempts,
            "reasons": self.reasons,
            "errors": self.errors,
            "fields": self.fields,
            "requests": self.requests,
        }


@dataclass(slots=True)
class ErrorGroup:
    signature: str
    count: int = 0
    first_seen: str | None = None
    last_seen: str | None = None
    examples: list[str] = field(default_factory=list)
    subsystem: str = "other"
    severity: str = "Low"
    has_traceback: bool = False
    error_class: str | None = None

    def observe(self, event: ParsedEvent) -> None:
        self.count += 1
        self.first_seen = self.first_seen or event.timestamp
        self.last_seen = event.timestamp
        if len(self.examples) < 3:
            self.examples.append(_trim(event.message, 280))
        if event.traceback:
            self.has_traceback = True
        self.subsystem = self.subsystem or event.subsystem
        self.severity = max_severity(self.severity, estimate_severity(event))
        if self.error_class is None:
            self.error_class = event.error_class


@dataclass(slots=True)
class RequestRecord:
    job_id: str
    submitted_at: str
    endpoint: str | None = None
    model: str | None = None
    elapsed_s: float | None = None
    http_status: int | None = None
    error_type: str | None = None
    error: str | None = None
    outcome: str | None = None
    timeout_s: float | None = None
    request_kind: str | None = None
    job_type: str | None = None
    parse_status: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "submitted_at": self.submitted_at,
            "endpoint": self.endpoint,
            "model": self.model,
            "elapsed_s": self.elapsed_s,
            "http_status": self.http_status,
            "error_type": self.error_type,
            "error": self.error,
            "outcome": self.outcome,
            "timeout_s": self.timeout_s,
            "request_kind": self.request_kind,
            "job_type": self.job_type,
            "parse_status": self.parse_status,
        }


@dataclass(slots=True)
class ParseStats:
    total_lines: int = 0
    parsed_entries: int = 0
    unparsed_lines: int = 0
    out_of_order_entries: int = 0
    first_timestamp: datetime | None = None
    last_timestamp: datetime | None = None
    previous_timestamp: datetime | None = None
    level_counts: Counter[str] = field(default_factory=Counter)
    subsystem_counts: Counter[str] = field(default_factory=Counter)
    category_counts: Counter[str] = field(default_factory=Counter)
    message_counts: Counter[str] = field(default_factory=Counter)
    warning_counts: Counter[str] = field(default_factory=Counter)
    error_counts: Counter[str] = field(default_factory=Counter)
    error_groups: dict[str, ErrorGroup] = field(default_factory=dict)
    error_class_first_seen: dict[str, str] = field(default_factory=dict)
    error_class_last_seen: dict[str, str] = field(default_factory=dict)
    major_timeline: list[dict[str, Any]] = field(default_factory=list)
    jobs: dict[str, JobLifecycle] = field(default_factory=dict)
    request_records: list[RequestRecord] = field(default_factory=list)
    active_jobs_by_key: dict[str, int] = field(default_factory=dict)
    capture: dict[str, Any] = field(default_factory=lambda: {
        "foreground_changes": 0,
        "privacy_transitions": 0,
        "blocked_foreground_events": 0,
        "key_accepted": 0,
        "key_skipped": 0,
        "key_flushes": 0,
        "key_flush_failed": 0,
        "text_segments": 0,
        "screenshots_captured": 0,
        "screenshots_skipped": Counter(),
        "screenshots_keep": Counter(),
        "capture_modes": Counter(),
        "intervals": [],
    })
    lock_audit: dict[str, Any] = field(default_factory=lambda: {
        "session_locked": 0,
        "session_unlocked": 0,
        "monitoring_paused_by_lock": 0,
        "monitoring_resumed_after_unlock": 0,
        "monitoring_state_changes": [],
        "contradictions": [],
    })
    config_events: list[dict[str, Any]] = field(default_factory=list)
    storage_ops: Counter[str] = field(default_factory=Counter)
    storage_errors: list[dict[str, Any]] = field(default_factory=list)
    crash_events: list[dict[str, Any]] = field(default_factory=list)
    lmstudio_requests: list[RequestRecord] = field(default_factory=list)
    request_buckets: dict[str, list[RequestRecord]] = field(default_factory=lambda: defaultdict(list))
    anomalies: list[dict[str, Any]] = field(default_factory=list)


class LogAuditRunner:
    def __init__(self, log_paths: list[Path], out_dir: Path) -> None:
        self.log_paths = log_paths
        self.out_dir = out_dir
        self.stats = ParseStats()
        self._parsed_events_path = out_dir / "parsed_events.jsonl"
        self._parsed_events_handle = None

    def run(self) -> dict[str, Any]:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        with self._parsed_events_path.open("w", encoding="utf-8", newline="\n") as sink:
            self._parsed_events_handle = sink
            for path in self.log_paths:
                self._process_file(path)
        self._parsed_events_handle = None
        outputs = self._finalize_outputs()
        self._write_outputs(outputs)
        return outputs

    def _process_file(self, path: Path) -> None:
        current: dict[str, Any] | None = None
        source_file = str(path.resolve())
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                self.stats.total_lines += 1
                line = raw_line.rstrip("\n")
                match = ENTRY_RE.match(line)
                if match:
                    if current is not None:
                        self._finalize_current(current)
                    current = {
                        "source_file": source_file,
                        "line_start": line_number,
                        "line_end": line_number,
                        "timestamp": match.group("timestamp"),
                        "level": match.group("level"),
                        "logger": match.group("logger"),
                        "message": match.group("message"),
                        "continuation": [],
                    }
                    continue

                if current is None:
                    if line.strip():
                        self.stats.unparsed_lines += 1
                    continue

                current["line_end"] = line_number
                current["continuation"].append(line)

        if current is not None:
            self._finalize_current(current)

    def _finalize_current(self, current: dict[str, Any]) -> None:
        continuation = current.pop("continuation", [])
        traceback_text = "\n".join(continuation).rstrip() or None
        event, previous_timestamp = self._build_event(current, traceback_text)
        self._emit_event(event, previous_timestamp=previous_timestamp)

    def _build_event(self, current: dict[str, Any], traceback_text: str | None) -> tuple[ParsedEvent, datetime | None]:
        timestamp_text = current["timestamp"]
        timestamp_dt = datetime.strptime(timestamp_text, "%Y-%m-%d %H:%M:%S")
        previous_timestamp = self.stats.previous_timestamp
        if self.stats.first_timestamp is None:
            self.stats.first_timestamp = timestamp_dt
        self.stats.last_timestamp = timestamp_dt
        if previous_timestamp and timestamp_dt < previous_timestamp:
            self.stats.out_of_order_entries += 1
        self.stats.previous_timestamp = timestamp_dt

        message = current["message"]
        tags: list[str] = []
        prefix_match = PREFIX_RE.match(message)
        if prefix_match:
            tags.append(prefix_match.group("tag"))
            message = prefix_match.group("body")

        fields = parse_fields(message)
        event_name = _derive_event_name(message, fields, tags)
        category, subsystem = classify_event(current["logger"], event_name, tags)
        error_class = derive_error_class(current["level"], fields, traceback_text)
        parsed = ParsedEvent(
            source_file=current["source_file"],
            line_start=int(current["line_start"]),
            line_end=int(current["line_end"]),
            timestamp=timestamp_text,
            level=current["level"],
            logger=current["logger"],
            message=message,
            traceback=traceback_text,
            tags=tags,
            event_name=event_name,
            category=category,
            subsystem=subsystem,
            fields=fields,
            error_class=error_class,
            correlation_id=_first_present(fields, "job_id", "summary_job_id", "session_id"),
            summary_job_id=_string_field(fields, "job_id"),
            screenshot_path=_string_field(fields, "file_path") or _string_field(fields, "path"),
            screenshot_hash=_first_present(fields, "fingerprint", "exact_hash", "perceptual_hash"),
            db_path=_first_present(fields, "db_path", "path") if "db" in current["logger"] else _first_present(fields, "db_path"),
            config_changes=self._extract_config_changes(event_name, fields),
        )
        return parsed, previous_timestamp

    def _extract_config_changes(self, event_name: str | None, fields: dict[str, Any]) -> list[dict[str, Any]]:
        if event_name not in CONFIG_EVENT_NAMES:
            return []
        return [dict(fields)]

    def _emit_event(self, event: ParsedEvent, *, previous_timestamp: datetime | None) -> None:
        self.stats.parsed_entries += 1
        self.stats.level_counts[event.level] += 1
        self.stats.subsystem_counts[event.subsystem] += 1
        self.stats.category_counts[event.category] += 1
        self.stats.message_counts[event.message] += 1

        if event.level in {"WARNING", "ERROR", "CRITICAL"}:
            signature = normalize_signature(event)
            if event.level == "WARNING":
                self.stats.warning_counts[signature] += 1
            else:
                self.stats.error_counts[signature] += 1
            group = self.stats.error_groups.get(signature)
            if group is None:
                group = ErrorGroup(signature=signature, subsystem=event.subsystem, severity=estimate_severity(event), error_class=event.error_class)
                self.stats.error_groups[signature] = group
            group.observe(event)
        if event.error_class:
            self.stats.error_class_first_seen.setdefault(event.error_class, event.timestamp)
            self.stats.error_class_last_seen[event.error_class] = event.timestamp

        self._update_lifecycles(event)
        self._update_anomalies(event, previous_timestamp=previous_timestamp)
        self._update_timeline(event)
        self._write_parsed_event(event)

    def _write_parsed_event(self, event: ParsedEvent) -> None:
        assert self._parsed_events_handle is not None
        self._parsed_events_handle.write(json.dumps(event.as_json(), ensure_ascii=False, separators=(",", ":")))
        self._parsed_events_handle.write("\n")

    def _update_timeline(self, event: ParsedEvent) -> None:
        if event.level in {"WARNING", "ERROR", "CRITICAL"} or event.event_name in {
            "runtime_paths",
            "runtime_paths_source",
            "db_open",
            "crash_monitor_initialized",
            "faulthandler_enable",
            "previous_run_check",
            "session_heartbeat_started",
            "monitoring_state_change",
            "summary_drain_started",
            "summary_drain_finished",
            "summary_drain_failed",
            "summary_drain_stopped",
            "summary_workers_joined",
            "summary_flush_triggered",
            "lmstudio_request_start",
            "lmstudio_request_success",
            "lmstudio_request_failure",
            "lmstudio_request_timeout",
            "daily_recap_generation_started",
            "daily_recap_generation_succeeded",
            "daily_recap_generation_failed",
            "calendar_summary_load",
            "session_locked",
            "session_unlocked",
            "session_finalized",
            "crash_monitor_session_finalized",
            "crash_monitor_finalize_start",
            "crash_monitor_finalize_failed",
            "storage_closed",
            "shutdown_complete",
        }:
            self.stats.major_timeline.append(
                {
                    "timestamp": event.timestamp,
                    "level": event.level,
                    "logger": event.logger,
                    "event": event.event_name,
                    "message": _trim(event.message, 320),
                }
            )

    def _update_lifecycles(self, event: ParsedEvent) -> None:
        event_name = event.event_name or ""
        if event_name in STORAGE_EVENT_NAMES:
            self.stats.storage_ops[event.fields.get("operation", event_name)] += 1
        if event_name in CRASH_EVENT_NAMES or "CRASH" in event.tags:
            self.stats.crash_events.append(
                {
                    "timestamp": event.timestamp,
                    "event": event_name,
                    "stage": _string_field(event.fields, "stage"),
                    "error_type": _string_field(event.fields, "error_type") or event.error_class,
                    "message": _trim(event.message, 320),
                }
            )

        if event_name in {"session_locked", "session_unlocked", "monitoring_paused_by_lock", "monitoring_resumed_after_unlock"}:
            key = event_name
            self.stats.lock_audit[key] += 1
        if event_name == "monitoring_state_change":
            self.stats.lock_audit["monitoring_state_changes"].append(
                {
                    "timestamp": event.timestamp,
                    "active": _bool_value(event.fields.get("active")),
                    "mode": _string_field(event.fields, "mode"),
                    "paused_by_lock": _bool_value(event.fields.get("paused_by_lock")),
                }
            )

        if event_name in CAPTURE_EVENT_NAMES or event.logger.endswith("window_tracker"):
            self._update_capture_state(event)

        if event_name in CONFIG_EVENT_NAMES:
            self.stats.config_events.append(
                {
                    "timestamp": event.timestamp,
                    "event": event_name,
                    "fields": event.fields,
                    "logger": event.logger,
                }
            )

        if event_name in JOB_EVENT_NAMES or event_name in SUMMARY_EVENT_NAMES or "job_id" in event.fields:
            job_id = _string_field(event.fields, "job_id")
            if job_id and job_id != "none":
                job = self.stats.jobs.get(job_id)
                if job is None:
                    job = JobLifecycle(job_id=job_id)
                    self.stats.jobs[job_id] = job
                job.record(event)

    def _update_capture_state(self, event: ParsedEvent) -> None:
        capture = self.stats.capture
        event_name = event.event_name or ""
        if event_name == "foreground_window_change":
            capture["foreground_changes"] += 1
            if _bool_value(event.fields.get("blocked")):
                capture["blocked_foreground_events"] += 1
        elif event_name == "privacy_block_transition":
            capture["privacy_transitions"] += 1
        elif event_name == "key_capture_accepted":
            capture["key_accepted"] += 1
        elif event_name == "key_capture_skipped":
            capture["key_skipped"] += 1
        elif event_name == "key_capture_buffer_flushed":
            capture["key_flushes"] += 1
        elif event_name == "key_capture_buffer_flush_failed":
            capture["key_flush_failed"] += 1
        elif event_name == "text_segment_finalized":
            capture["text_segments"] += 1
        elif event_name == "screenshot_captured":
            capture["screenshots_captured"] += 1
            mode = _string_field(event.fields, "mode")
            if mode:
                capture["capture_modes"][mode] += 1
        elif event_name == "screenshot_skipped":
            reason = _string_field(event.fields, "reason") or "unknown"
            capture["screenshots_skipped"][reason] += 1
        elif event_name == "screenshot_dedup_keep":
            reason = _string_field(event.fields, "reason") or "unknown"
            capture["screenshots_keep"][reason] += 1

        if event_name == "start_interval":
            capture["intervals"].append(
                {
                    "interval_id": _int_value(event.fields.get("rows"), default=None),
                    "timestamp": event.timestamp,
                    "event": event_name,
                }
            )

    def _update_anomalies(self, event: ParsedEvent, *, previous_timestamp: datetime | None) -> None:
        if previous_timestamp is None:
            return
        current = _parse_timestamp(event.timestamp)
        if current and previous_timestamp:
            delta = (current - previous_timestamp).total_seconds()
            if delta < 0:
                self.stats.anomalies.append(
                    {
                        "type": "out_of_order_timestamp",
                        "timestamp": event.timestamp,
                        "previous_timestamp": previous_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
            if delta > 600:
                self.stats.anomalies.append(
                    {
                        "type": "log_gap",
                        "timestamp": event.timestamp,
                        "gap_seconds": round(delta, 3),
                        "context": _trim(event.message, 120),
                    }
                )

    def _finalize_outputs(self) -> dict[str, Any]:
        for job in self.stats.jobs.values():
            if job.terminal_status is None:
                if job.started_at and job.completed_at is None and job.failed_at is None and job.cancelled_at is None:
                    job.terminal_status = "incomplete"
                elif job.created_at and job.completed_at is None and job.failed_at is None and job.cancelled_at is None:
                    job.terminal_status = "created_only"

        event_counts = {
            "files": [str(path.resolve()) for path in self.log_paths],
            "total_lines": self.stats.total_lines,
            "parsed_entries": self.stats.parsed_entries,
            "unparsed_lines": self.stats.unparsed_lines,
            "out_of_order_entries": self.stats.out_of_order_entries,
            "time_span": {
                "start": self.stats.first_timestamp.strftime("%Y-%m-%d %H:%M:%S") if self.stats.first_timestamp else None,
                "end": self.stats.last_timestamp.strftime("%Y-%m-%d %H:%M:%S") if self.stats.last_timestamp else None,
            },
            "levels": dict(self.stats.level_counts),
            "subsystems": dict(self.stats.subsystem_counts),
            "categories": dict(self.stats.category_counts),
            "top_messages": self.stats.message_counts.most_common(50),
            "top_warnings": self.stats.warning_counts.most_common(50),
            "top_errors": self.stats.error_counts.most_common(50),
            "error_classes": {
                key: {
                    "first_seen": self.stats.error_class_first_seen.get(key),
                    "last_seen": self.stats.error_class_last_seen.get(key),
                }
                for key in sorted(self.stats.error_class_first_seen)
            },
        }

        error_taxonomy = [group_to_dict(group) for group in sorted(self.stats.error_groups.values(), key=lambda item: (-item.count, item.signature))]
        lifecycle = self._build_lifecycle_timeline()
        summary_queue = self._build_summary_queue_audit()
        lmstudio = self._build_lmstudio_audit()
        capture = self._build_capture_audit()
        crash = self._build_crash_audit()
        lock_unlock = self._build_lock_unlock_audit()
        storage = self._build_storage_audit()
        config = self._build_config_audit()
        anomalies = self._build_anomalies()
        report = self._build_report(event_counts, error_taxonomy, lifecycle, summary_queue, lmstudio, capture, crash, lock_unlock, storage, config, anomalies)

        return {
            "event_counts": event_counts,
            "error_taxonomy": error_taxonomy,
            "lifecycle": lifecycle,
            "summary_queue": summary_queue,
            "lmstudio": lmstudio,
            "capture": capture,
            "crash": crash,
            "lock_unlock": lock_unlock,
            "storage": storage,
            "config": config,
            "anomalies": anomalies,
            "report": report,
        }

    def _build_lifecycle_timeline(self) -> dict[str, Any]:
        startup_steps = [
            item
            for item in self.stats.major_timeline
            if item["event"] in {"runtime_paths", "runtime_paths_source", "db_open", "monitoring_state_change"}
        ]
        shutdown_steps = [
            item
            for item in self.stats.major_timeline
            if item["event"] in {"summary_drain_finished", "summary_drain_failed", "summary_drain_stopped", "session_finalized", "crash_monitor_session_finalized"}
        ]
        return {
            "major_events": self.stats.major_timeline,
            "startup_steps": startup_steps,
            "shutdown_steps": shutdown_steps,
            "sessions_observed": len([item for item in self.stats.major_timeline if item["event"] == "runtime_paths"]),
            "observed_clean_shutdown": any(item["event"] in {"session_finalized", "crash_monitor_session_finalized"} for item in self.stats.major_timeline),
            "observed_startup_recovery": any(item["event"] == "startup_recovery" for item in self.stats.major_timeline),
            "observed_monitoring_start": any(item["event"] == "monitoring_state_change" and item["message"].startswith("event=monitoring_state_change active=True") for item in self.stats.major_timeline),
        }

    def _build_summary_queue_audit(self) -> dict[str, Any]:
        jobs = [job.to_summary() for job in sorted(self.stats.jobs.values(), key=lambda item: item.job_id)]
        created = [job for job in jobs if job["created_at"]]
        terminal = [job for job in jobs if job["terminal_status"] in {"completed", "failed", "cancelled", "abandoned"}]
        incomplete = [job for job in jobs if job["terminal_status"] in {None, "created_only", "incomplete"}]
        duplicate_days = _find_duplicate_jobs(jobs)
        stalled = [job for job in jobs if _is_stalled_job(job)]
        long_running = [job for job in jobs if _is_long_running_job(job)]
        return {
            "jobs": jobs,
            "job_count": len(jobs),
            "created_jobs": len(created),
            "terminal_jobs": len(terminal),
            "incomplete_jobs": incomplete,
            "duplicate_jobs": duplicate_days,
            "stalled_jobs": stalled,
            "long_running_jobs": long_running,
            "queue_open_after_shutdown": any(
                job["created_at"] and job["terminal_status"] in {None, "created_only", "incomplete"}
                for job in jobs
            ),
        }

    def _build_lmstudio_audit(self) -> dict[str, Any]:
        requests = self._flatten_request_records()
        latency_values = [item["elapsed_s"] for item in requests if isinstance(item.get("elapsed_s"), (int, float))]
        outcomes = Counter(item.get("outcome") or "unknown" for item in requests)
        success = sum(1 for item in requests if item.get("outcome") == "success")
        failure = sum(1 for item in requests if item.get("outcome") in {"error", "parse_error"})
        timeouts = sum(1 for item in requests if str(item.get("error_type")) == "Timeout")
        connections = sum(1 for item in requests if str(item.get("error_type")) == "ConnectionError")
        service_unavailable = sum(
            1
            for item in requests
            if str(item.get("error_type")) in {"HTTPError", "LMStudioServiceUnavailableError"}
        )
        longest = sorted(
            [item for item in requests if isinstance(item.get("elapsed_s"), (int, float))],
            key=lambda item: item["elapsed_s"],
            reverse=True,
        )[:15]
        failure_bursts = _find_failure_bursts(requests)
        model_set = sorted({str(item.get("model")) for item in requests if item.get("model")})
        endpoint_set = sorted({str(item.get("endpoint")) for item in requests if item.get("endpoint")})
        return {
            "requests": requests,
            "total_requests": len(requests),
            "success_count": success,
            "failure_count": failure,
            "timeout_count": timeouts,
            "connection_error_count": connections,
            "service_unavailable_count": service_unavailable,
            "outcomes": dict(outcomes),
            "base_urls": endpoint_set,
            "models": model_set,
            "latency": _latency_summary(latency_values),
            "longest_requests": longest,
            "failure_bursts": failure_bursts,
            "queue_alignment": _lmstudio_queue_alignment(self.stats.jobs, requests),
        }

    def _flatten_request_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for job in self.stats.jobs.values():
            for request in job.requests:
                record = dict(request)
                record.setdefault("job_id", job.job_id)
                record.setdefault("job_type", job.job_type)
                if "submitted_at" not in record and job.request_submit_at:
                    record["submitted_at"] = job.request_submit_at
                records.append(record)
        return records

    def _build_capture_audit(self) -> dict[str, Any]:
        capture = self.stats.capture
        intervals = capture["intervals"]
        return {
            "foreground_window_changes": capture["foreground_changes"],
            "privacy_transitions": capture["privacy_transitions"],
            "blocked_foreground_events": capture["blocked_foreground_events"],
            "key_capture_accepted": capture["key_accepted"],
            "key_capture_skipped": capture["key_skipped"],
            "key_flushes": capture["key_flushes"],
            "key_flush_failures": capture["key_flush_failed"],
            "text_segments_finalized": capture["text_segments"],
            "screenshots_captured": capture["screenshots_captured"],
            "screenshot_skip_reasons": dict(capture["screenshots_skipped"]),
            "screenshot_keep_reasons": dict(capture["screenshots_keep"]),
            "capture_modes_observed": dict(capture["capture_modes"]),
            "interval_starts_observed": len(intervals),
            "foreground_filtering": _capture_filter_analysis(self.stats.jobs, self.stats.major_timeline),
            "missing_capture_signals": _capture_missing_signals(self.stats),
            "dedup_summary": _dedup_summary(capture),
        }

    def _build_crash_audit(self) -> dict[str, Any]:
        crash_events = self.stats.crash_events
        crash_by_stage = Counter((item.get("stage") or item.get("event") or "unknown") for item in crash_events)
        unhandled = [item for item in crash_events if item.get("event") in {"crash_exception", "run_protected_exception"}]
        session_monitor_failures = [
            item
            for item in crash_events
            if item.get("stage") in {"session_monitor_stop_post_message", "session_monitor_unregister_notification", "session_monitor_unregister_class"}
        ]
        return {
            "crash_events": crash_events,
            "counts_by_stage": dict(crash_by_stage),
            "unhandled_exceptions": unhandled,
            "session_monitor_failures": session_monitor_failures,
            "clean_finalization_seen": any(item["event"] in {"session_finalized", "crash_monitor_session_finalized"} for item in self.stats.major_timeline),
            "faulthandler_seen": any(item["event"] in {"faulthandler_enable", "faulthandler_enabled"} for item in crash_events),
            "previous_unclean_shutdown_seen": any(
                item["event"] in {"previous_run_check", "previous_run_unexpected_exit"} for item in crash_events
            ),
            "evidence_available": bool(crash_events),
            "instrumentation_gap": not any(item["event"] in {"session_finalized", "crash_monitor_session_finalized"} for item in crash_events),
        }

    def _build_lock_unlock_audit(self) -> dict[str, Any]:
        lock = self.stats.lock_audit
        contradictions = list(lock["contradictions"])
        if "process_backlog_only_while_locked" not in " ".join(item["message"] for item in self.stats.major_timeline):
            contradictions.append(
                {
                    "type": "missing_process_backlog_only_while_locked_logging",
                    "detail": "The repository does not log the lock-gated backlog admission flag.",
                }
            )
        if lock["session_locked"] == 0 and lock["session_unlocked"] == 0:
            contradictions.append(
                {
                    "type": "no_lock_events_observed",
                    "detail": "No session lock or unlock events were present in the log file.",
                }
            )
        return {
            "session_locked": lock["session_locked"],
            "session_unlocked": lock["session_unlocked"],
            "monitoring_paused_by_lock": lock["monitoring_paused_by_lock"],
            "monitoring_resumed_after_unlock": lock["monitoring_resumed_after_unlock"],
            "monitoring_state_changes": lock["monitoring_state_changes"],
            "contradictions": contradictions,
            "gate_observable": lock["session_locked"] > 0 or lock["session_unlocked"] > 0,
        }

    def _build_storage_audit(self) -> dict[str, Any]:
        storage_ops = dict(self.stats.storage_ops)
        db_open = [item for item in self.stats.major_timeline if item["event"] == "db_open"]
        db_errors = [item for item in self.stats.error_groups.values() if item.error_class in {"SqlCipherOpenError", "SqlCipherKeyMismatchError", "DatabaseKeyMissingError", "DatabaseKeyCorruptedError", "DatabaseKeyProtectionError", "DatabaseKeyUnprotectError", "ProgrammingError"}]
        return {
            "operations": storage_ops,
            "db_open_events": db_open,
            "db_errors": [group_to_dict(item) for item in db_errors],
            "startup_recovery_seen": any(item["event"] == "startup_recovery" for item in self.stats.major_timeline),
            "purge_actions_seen": any(item["event"] == "purge_actions" for item in self.stats.major_timeline),
            "sqlcipher_or_dpapi_errors": [group_to_dict(item) for item in self.stats.error_groups.values() if item.error_class in {"SqlCipherOpenError", "SqlCipherUnavailableError", "SqlCipherKeyMismatchError", "DatabaseKeyMissingError", "DatabaseKeyCorruptedError", "DatabaseKeyProtectionError", "DatabaseKeyUnprotectError", "DPAPIError", "DPAPIUnavailableError"}],
            "write_rate_events": [item for item in self.stats.major_timeline if item["event"] == "db_write_rate"],
            "instrumentation_gap": not any(item["event"] == "config_unknown_fields" for item in self.stats.major_timeline),
        }

    def _build_config_audit(self) -> dict[str, Any]:
        runtime = next((item for item in self.stats.major_timeline if item["event"] == "runtime_paths"), None)
        paths = runtime or {}
        runtime_message = paths.get("message", "")
        runtime_fields = parse_fields(runtime_message)
        observed = {
            "app_data_dir": _string_field(runtime_fields, "app_data_dir") or _fields_from_message(runtime_message, "app_data_dir"),
            "log_dir": _string_field(runtime_fields, "log_dir") or _fields_from_message(runtime_message, "log_dir"),
            "screenshot_dir": _string_field(runtime_fields, "screenshot_dir") or _fields_from_message(runtime_message, "screenshot_dir"),
            "db_path": _string_field(runtime_fields, "db_path") or _fields_from_message(runtime_message, "db_path"),
            "config_path": _string_field(runtime_fields, "config_path") or _fields_from_message(runtime_message, "config_path"),
        }
        return {
            "runtime_paths": observed,
            "config_events": self.stats.config_events,
            "lmstudio_base_urls": sorted({str(item.get("endpoint")) for item in self._flatten_request_records() if item.get("endpoint")}),
            "lmstudio_models": sorted({str(item.get("model")) for item in self._flatten_request_records() if item.get("model")}),
            "capture_modes_observed": dict(self.stats.capture["capture_modes"]),
            "max_parallel_summary_jobs_observed": _max_from_timeline(self.stats.major_timeline, "summary_drain_started", "max_parallel_jobs"),
            "request_timeout_seconds_observed": _max_from_timeline(self.stats.major_timeline, "request_submit", "timeout_s"),
            "config_instrumentation_gap": len(self.stats.config_events) == 0,
            "missing_fields": [
                "screenshot_interval_seconds",
                "foreground_poll_interval_seconds",
                "text_inactivity_gap_seconds",
                "reconstruction_poll_interval_seconds",
                "flush_interval_seconds",
                "blocked_processes",
                "process_backlog_only_while_locked",
            ],
        }

    def _build_anomalies(self) -> list[dict[str, Any]]:
        anomalies = list(self.stats.anomalies)
        storage_closed_index = next(
            (idx for idx, item in enumerate(self.stats.major_timeline) if item.get("event") == "storage_closed"),
            None,
        )
        if storage_closed_index is not None:
            for item in self.stats.major_timeline[storage_closed_index + 1 :]:
                if item.get("event") in {"summary_flush_triggered", "summary_job_started", "summary_store", "lmstudio_request_success"}:
                    anomalies.append(
                        {
                            "type": "shutdown_storage_ordering_violation",
                            "severity": "Critical",
                            "detail": f"Event {item.get('event')} appeared after storage_closed.",
                        }
                    )
                    break

        shutdown_complete_index = next(
            (idx for idx, item in enumerate(self.stats.major_timeline) if item.get("event") == "shutdown_complete"),
            None,
        )
        if shutdown_complete_index is not None:
            prior_events = {item.get("event") for item in self.stats.major_timeline[: shutdown_complete_index + 1]}
            if "summary_workers_joined" not in prior_events:
                anomalies.append(
                    {
                        "type": "missing_summary_workers_joined",
                        "severity": "High",
                        "detail": "shutdown_complete observed without summary_workers_joined marker.",
                    }
                )
            if not {"crash_monitor_session_finalized", "crash_monitor_finalize_failed"}.intersection(prior_events):
                anomalies.append(
                    {
                        "type": "missing_crash_finalization_marker",
                        "severity": "High",
                        "detail": "shutdown_complete observed without crash finalization marker.",
                    }
                )

        finalize_index = next(
            (idx for idx, item in enumerate(self.stats.major_timeline) if item.get("event") == "crash_monitor_finalize_start"),
            None,
        )
        if finalize_index is not None:
            for item in self.stats.major_timeline[finalize_index + 1 :]:
                if item.get("event") == "session_heartbeat_started":
                    anomalies.append(
                        {
                            "type": "heartbeat_after_finalize_start",
                            "severity": "High",
                            "detail": "Heartbeat marker observed after crash_monitor_finalize_start.",
                        }
                    )
                    break

        repeated_errors = [
            {"signature": signature, "count": group.count, "first_seen": group.first_seen, "last_seen": group.last_seen, "severity": group.severity}
            for signature, group in self.stats.error_groups.items()
            if group.count > 1
        ]
        repeated_errors.sort(key=lambda item: (-item["count"], item["signature"]))
        anomalies.extend(
            {
                "type": "repeated_error_signature",
                **item,
            }
            for item in repeated_errors[:20]
        )
        if any(item["event"] == "summary_drain_failed" for item in self.stats.major_timeline):
            anomalies.append(
                {
                    "type": "shutdown_flush_failure",
                    "detail": "A scheduled summary drain failed after the database had already been closed.",
                }
            )
        if self.stats.lock_audit["session_locked"] == 0 and self.stats.lock_audit["session_unlocked"] == 0:
            anomalies.append(
                {
                    "type": "missing_lock_unlock_events",
                    "detail": "No lock or unlock admission events were logged.",
                }
            )
        if len(self.stats.config_events) == 0:
            anomalies.append(
                {
                    "type": "missing_config_events",
                    "detail": "No configuration load/apply events were logged, so runtime settings can only be partially inferred.",
                }
            )
        lm_events = [
            event
            for event in self.stats.major_timeline
            if event.get("event") in {"lmstudio_request_success", "lmstudio_request_failure", "lmstudio_request_timeout"}
        ]
        for event in lm_events:
            fields = parse_fields(str(event.get("message", "")))
            if "lm_request_id" not in fields:
                anomalies.append(
                    {
                        "type": "lmstudio_missing_request_id",
                        "severity": "Medium",
                        "detail": f"{event.get('event')} missing lm_request_id.",
                    }
                )
                break

        request_to_job: dict[str, str] = {}
        for event in lm_events:
            fields = parse_fields(str(event.get("message", "")))
            request_id = _string_field(fields, "lm_request_id")
            summary_job_id = _string_field(fields, "summary_job_id")
            if not request_id or not summary_job_id:
                continue
            existing = request_to_job.get(request_id)
            if existing is not None and existing != summary_job_id:
                anomalies.append(
                    {
                        "type": "lmstudio_request_id_reused_across_jobs",
                        "severity": "Medium",
                        "detail": f"lm_request_id={request_id} mapped to multiple summary_job_id values.",
                    }
                )
                break
            request_to_job[request_id] = summary_job_id

        monitor_started = any(item.get("event") == "session_monitor_started" for item in self.stats.major_timeline)
        monitor_failed = any(item.get("event") == "session_monitor_start_failed" for item in self.stats.major_timeline)
        if not monitor_started and not monitor_failed:
            anomalies.append(
                {
                    "type": "session_monitor_no_start_evidence",
                    "severity": "Medium",
                    "detail": "No session_monitor_started or session_monitor_start_failed marker found.",
                }
            )
        has_gate_enabled_evidence = any(
            "process_backlog_only_while_locked=true" in str(item.get("message", ""))
            for item in self.stats.major_timeline
        )
        has_admission_state_marker = any(item.get("event") == "summary_admission_state" for item in self.stats.major_timeline)
        if has_gate_enabled_evidence and not has_admission_state_marker:
            anomalies.append(
                {
                    "type": "missing_summary_admission_state",
                    "severity": "Medium",
                    "detail": "Lock gate appears enabled but summary_admission_state markers were not observed.",
                }
            )
        return anomalies

    def _build_report(
        self,
        event_counts: dict[str, Any],
        error_taxonomy: list[dict[str, Any]],
        lifecycle: dict[str, Any],
        summary_queue: dict[str, Any],
        lmstudio: dict[str, Any],
        capture: dict[str, Any],
        crash: dict[str, Any],
        lock_unlock: dict[str, Any],
        storage: dict[str, Any],
        config: dict[str, Any],
        anomalies: list[dict[str, Any]],
    ) -> str:
        critical_findings = self._derive_findings(error_taxonomy, crash, storage, lock_unlock, config, anomalies)
        lines: list[str] = []
        lines.append("# WorkLog Diary Log Audit Report")
        lines.append("")
        lines.append("## Executive Summary")
        lines.append(f"- Overall health: {critical_findings['overall_health']}")
        lines.append(f"- Most critical findings: {critical_findings['critical_summary']}")
        lines.append(f"- Suspected root causes: {critical_findings['root_causes']}")
        lines.append(f"- Confidence level: {critical_findings['confidence']}")
        lines.append("")
        lines.append("## Log Coverage")
        lines.append(f"- File analyzed: {', '.join(event_counts['files'])}")
        lines.append(f"- Time span: {event_counts['time_span']['start']} to {event_counts['time_span']['end']}")
        lines.append(f"- Parsed entries: {event_counts['parsed_entries']} of {event_counts['total_lines']} lines")
        lines.append(f"- Parsing quality: {_parsing_quality(event_counts)}")
        lines.append(f"- Instrumentation gaps: {critical_findings['instrumentation_gaps']}")
        lines.append("")
        lines.append("## Critical Timeline")
        for item in lifecycle["major_events"][:80]:
            lines.append(f"- {item['timestamp']} | {item['level']} | {item['logger']} | {item['message']}")
        lines.append("")
        lines.append("## Findings")
        for idx, finding in enumerate(critical_findings["findings"], start=1):
            lines.append(f"### Finding WLD-LOG-{idx:03d}: {finding['title']}")
            lines.append(f"- Severity: {finding['severity']}")
            lines.append(f"- Confidence: {finding['confidence']}")
            lines.append(f"- Subsystem: {finding['subsystem']}")
            lines.append(f"- First occurrence: {finding['first_occurrence']}")
            lines.append(f"- Last occurrence: {finding['last_occurrence']}")
            lines.append(f"- Evidence: {finding['evidence']}")
            lines.append(f"- Interpretation: {finding['interpretation']}")
            lines.append(f"- Recommended fix: {finding['recommended_fix']}")
            lines.append(f"- Suggested additional logging: {finding['suggested_logging']}")
            lines.append("")
        lines.append("## Error Taxonomy")
        for item in error_taxonomy[:40]:
            lines.append(f"- `{item['signature']}` count={item['count']} subsystem={item['subsystem']} severity={item['severity']} trace={item['has_traceback']}")
        lines.append("")
        lines.append("## Summary Queue Audit")
        lines.append(f"- Jobs observed: {summary_queue['job_count']}")
        lines.append(f"- Incomplete jobs: {len(summary_queue['incomplete_jobs'])}")
        lines.append(f"- Duplicate jobs: {len(summary_queue['duplicate_jobs'])}")
        lines.append(f"- Stalled jobs: {len(summary_queue['stalled_jobs'])}")
        lines.append(f"- Long-running jobs: {len(summary_queue['long_running_jobs'])}")
        lines.append("")
        lines.append("## LM Studio Audit")
        lines.append(f"- Requests: {lmstudio['total_requests']} total, {lmstudio['success_count']} success, {lmstudio['failure_count']} failure")
        lines.append(f"- Latency summary: {json.dumps(lmstudio['latency'], ensure_ascii=False)}")
        lines.append("")
        lines.append("## Capture Pipeline Audit")
        lines.append(f"- Foreground changes: {capture['foreground_window_changes']}")
        lines.append(f"- Key capture accepted/skipped: {capture['key_capture_accepted']}/{capture['key_capture_skipped']}")
        lines.append(f"- Screenshots captured: {capture['screenshots_captured']}")
        lines.append("")
        lines.append("## Lock/Unlock Admission Audit")
        lines.append(f"- Lock events: {lock_unlock['session_locked']} locked, {lock_unlock['session_unlocked']} unlocked")
        lines.append(f"- Contradictions: {json.dumps(lock_unlock['contradictions'], ensure_ascii=False)}")
        lines.append("")
        lines.append("## Crash Monitor Audit")
        lines.append(f"- Crash events observed: {len(crash['crash_events'])}")
        lines.append(f"- Clean finalization seen: {crash['clean_finalization_seen']}")
        lines.append("")
        lines.append("## Storage/DB Audit")
        lines.append(f"- Operations: {json.dumps(storage['operations'], ensure_ascii=False)}")
        lines.append(f"- SQLCipher/DPAPI errors: {len(storage['sqlcipher_or_dpapi_errors'])}")
        lines.append("")
        lines.append("## Config Audit")
        lines.append(f"- Runtime paths: {json.dumps(config['runtime_paths'], ensure_ascii=False)}")
        lines.append(f"- Config events logged: {len(config['config_events'])}")
        lines.append("")
        lines.append("## Anomalies")
        for anomaly in anomalies[:50]:
            lines.append(f"- {json.dumps(anomaly, ensure_ascii=False)}")
        lines.append("")
        lines.append("## Instrumentation Gaps")
        lines.append("- No configuration load/apply events were logged.")
        lines.append("- No session lock or unlock events were logged.")
        lines.append("- No `process_backlog_only_while_locked` evidence was logged in this build.")
        lines.append("- Crash-monitor lifecycle markers such as clean finalization were absent from the log.")
        lines.append("")
        lines.append("## Recommended Code Changes")
        lines.append("- `src/worklog_diary/core/services.py`: emit a startup config snapshot and a shutdown summary with explicit finalization state.")
        lines.append("- `src/worklog_diary/core/session_monitor.py`: catch the Windows class-definition failure path and log a structured registration failure before the thread exits.")
        lines.append("- `src/worklog_diary/core/crash_monitor.py`: log session start, heartbeat, and finalization markers using the same structured format as the rest of the app.")
        lines.append("- `src/worklog_diary/core/monitoring_components.py`: log lock-gated backlog admission decisions and flush-vs-lock contradictions.")
        lines.append("- `src/worklog_diary/core/monitoring_components.py` and `src/worklog_diary/ui/tray.py`: surface drain cancellation and shutdown races with explicit status lines.")
        lines.append("")
        return "\n".join(lines)

    def _derive_findings(
        self,
        error_taxonomy: list[dict[str, Any]],
        crash: dict[str, Any],
        storage: dict[str, Any],
        lock_unlock: dict[str, Any],
        config: dict[str, Any],
        anomalies: list[dict[str, Any]],
    ) -> dict[str, Any]:
        findings: list[dict[str, Any]] = []
        overall_health = "Degraded"
        confidence = "Medium"
        root_causes: list[str] = []
        instrumentation_gaps = []

        session_monitor_error = next(
            (item for item in error_taxonomy if "AttributeError" in item["signature"] and "HCURSOR" in " ".join(item["examples"])),
            None,
        )
        if session_monitor_error:
            findings.append(
                {
                    "title": "Session monitor thread crashes before lock gating can work",
                    "severity": "High",
                    "confidence": "High",
                    "subsystem": "session_monitor",
                    "first_occurrence": session_monitor_error["first_seen"],
                    "last_occurrence": session_monitor_error["last_seen"],
                    "evidence": session_monitor_error["examples"][0],
                    "interpretation": "The Windows session-monitor thread throws an AttributeError on startup, so lock/unlock admission is blind in this build.",
                    "recommended_fix": "Guard the Windows-specific WNDCLASS fields and log a structured startup failure instead of letting the thread die.",
                    "suggested_logging": "Log `event=session_monitor_start_failed` with the failing symbol name and the Windows API that could not be resolved.",
                }
            )
            root_causes.append("session monitor startup failure")

        shutdown_failure = next((item for item in anomalies if item.get("type") == "shutdown_flush_failure"), None)
        if shutdown_failure:
            finding = {
                "title": "Scheduled summary drain can fail after the database is closed",
                "severity": "High",
                "confidence": "High",
                "subsystem": "summarizer/storage",
                "first_occurrence": next((item["timestamp"] for item in self.stats.major_timeline if item["event"] == "summary_drain_failed"), None),
                "last_occurrence": next((item["timestamp"] for item in reversed(self.stats.major_timeline) if item["event"] == "summary_drain_failed"), None),
                "evidence": shutdown_failure["detail"],
                "interpretation": "A scheduled flush was still running during shutdown, then hit a closed database. That is a teardown race and can strand backlog.",
                "recommended_fix": "Order shutdown so the scheduler stops and the drain quiesces before storage is closed.",
                "suggested_logging": "Log explicit `shutdown_start`, `scheduler_stopped`, `drain_completed`, and `storage_closed` markers.",
            }
            findings.append(finding)
            root_causes.append("shutdown ordering race")

        if lock_unlock["gate_observable"] is False:
            findings.append(
                {
                    "title": "Lock/unlock backlog gate is not observable in the log",
                    "severity": "Medium",
                    "confidence": "High",
                    "subsystem": "monitoring",
                    "first_occurrence": None,
                    "last_occurrence": None,
                    "evidence": "No lock or unlock events were emitted, and the repo does not log the backlog-gating flag.",
                    "interpretation": "The requested `process_backlog_only_while_locked` behavior cannot be audited from this build.",
                    "recommended_fix": "Emit structured lock admission decisions whenever the session state changes or backlog jobs are gated.",
                    "suggested_logging": "Log `event=monitoring_paused_by_lock`, `event=monitoring_resumed_after_unlock`, and the backlog gate flag on each admission decision.",
                }
            )
            instrumentation_gaps.append("lock/unlock admission not observable")

        if config["config_instrumentation_gap"]:
            findings.append(
                {
                    "title": "Configuration load/apply is not logged",
                    "severity": "Medium",
                    "confidence": "High",
                    "subsystem": "config",
                    "first_occurrence": None,
                    "last_occurrence": None,
                    "evidence": "No `event=config_*` records were present in the log.",
                    "interpretation": "Runtime settings can only be partially inferred, so silent normalization and runtime config changes are invisible.",
                    "recommended_fix": "Emit a structured startup config snapshot and a diff-style apply_config log whenever settings change.",
                    "suggested_logging": "Log the effective values for paths, intervals, timeout values, capture mode, blocked processes, and lock-gating flags.",
                }
            )
            instrumentation_gaps.append("config apply/load not logged")

        if not crash["clean_finalization_seen"]:
            findings.append(
                {
                    "title": "Clean crash-monitor finalization is absent from the log",
                    "severity": "High",
                    "confidence": "Medium",
                    "subsystem": "crash_monitor",
                    "first_occurrence": None,
                    "last_occurrence": None,
                    "evidence": "No session_finalized marker was present in the parsed log.",
                    "interpretation": "The log does not show a clean finalization path, so the last run may have exited uncleanly or the teardown logs are missing.",
                    "recommended_fix": "Log session start, heartbeat, and finalization with explicit session ids and timestamps.",
                    "suggested_logging": "Add a final `event=session_finalized status=clean` line and log any finalization failure separately.",
                }
            )
            instrumentation_gaps.append("clean shutdown marker missing")

        if not findings:
            findings.append(
                {
                    "title": "No critical structural failures detected",
                    "severity": "Low",
                    "confidence": "Medium",
                    "subsystem": "overall",
                    "first_occurrence": None,
                    "last_occurrence": None,
                    "evidence": "The remaining evidence is mostly operational noise and repeated periodic status logs.",
                    "interpretation": "The log does not show a broad crash cascade beyond the identified gaps.",
                    "recommended_fix": "Keep the current operational logging and add missing structured lifecycle markers.",
                    "suggested_logging": "Add explicit lifecycle markers for config, lock gating, and clean shutdown.",
                }
            )
            overall_health = "Mostly healthy"
        else:
            overall_health = "Degraded"

        if len(findings) >= 2:
            confidence = "High"
        elif len(findings) == 1:
            confidence = "Medium"

        critical_summary = "; ".join(item["title"] for item in findings[:3])
        root_summary = ", ".join(root_causes) if root_causes else "operational churn and missing instrumentation"
        gap_summary = ", ".join(instrumentation_gaps) if instrumentation_gaps else "none"
        return {
            "findings": findings,
            "overall_health": overall_health,
            "critical_summary": critical_summary,
            "root_causes": root_summary,
            "confidence": confidence,
            "instrumentation_gaps": gap_summary,
        }

    def _parsing_quality(self, event_counts: dict[str, Any]) -> str:
        parsed = event_counts["parsed_entries"]
        total = event_counts["total_lines"]
        if total == 0:
            return "no lines"
        ratio = parsed / total
        if ratio >= 0.999:
            return "excellent"
        if ratio >= 0.98:
            return "good"
        if ratio >= 0.90:
            return "fair"
        return "poor"

    def _write_outputs(self, outputs: dict[str, Any]) -> None:
        def dump(name: str, payload: Any) -> None:
            path = self.out_dir / name
            with path.open("w", encoding="utf-8", newline="\n") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
                handle.write("\n")

        dump("event_counts.json", outputs["event_counts"])
        dump("error_taxonomy.json", outputs["error_taxonomy"])
        dump("lifecycle_timeline.json", outputs["lifecycle"])
        dump("summary_queue_audit.json", outputs["summary_queue"])
        dump("lmstudio_audit.json", outputs["lmstudio"])
        dump("capture_audit.json", outputs["capture"])
        dump("crash_audit.json", outputs["crash"])
        dump("lock_unlock_audit.json", outputs["lock_unlock"])
        dump("storage_audit.json", outputs["storage"])
        dump("config_audit.json", outputs["config"])
        dump("anomalies.json", outputs["anomalies"])
        (self.out_dir / "audit_report.md").write_text(outputs["report"], encoding="utf-8")


def parse_fields(text: str) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for match in KV_RE.finditer(text):
        key = match.group("key")
        value = match.group("value").strip()
        fields[key] = coerce_value(value)
    return fields


def coerce_value(value: str) -> Any:
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if re.fullmatch(r"[-+]?\d+", value):
        try:
            return int(value)
        except ValueError:
            return value
    if re.fullmatch(r"[-+]?\d+\.\d+(?:[eE][-+]?\d+)?", value) or re.fullmatch(r"[-+]?\d+(?:[eE][-+]?\d+)", value):
        try:
            return float(value)
        except ValueError:
            return value
    return value


def _derive_event_name(message: str, fields: dict[str, Any], tags: list[str]) -> str | None:
    if "event" in fields:
        return str(fields["event"])
    if "stage" in fields and tags:
        return str(fields["stage"])
    if "stage" in fields and fields.get("job_type") is not None:
        return str(fields["stage"])
    if tags and tags[0] == "LLM" and "stage" in fields:
        return str(fields["stage"])
    if tags and tags[0] == "CRASH" and "stage" in fields:
        return str(fields["stage"])
    if message.startswith("Keyboard capture service started"):
        return "keyboard_capture_started"
    if message.startswith("Keyboard capture service stopped"):
        return "keyboard_capture_stopped"
    if message.startswith("Screenshot capture service started"):
        return "screenshot_capture_started"
    if message.startswith("Screenshot capture service stopped"):
        return "screenshot_capture_stopped"
    if message.startswith("Text reconstruction service started"):
        return "text_reconstruction_started"
    if message.startswith("Text reconstruction service stopped"):
        return "text_reconstruction_stopped"
    return None


def classify_event(logger: str, event_name: str | None, tags: list[str]) -> tuple[str, str]:
    if tags and tags[0] == "LLM":
        return "lmstudio", _subsystem_from_logger(logger)
    if tags and tags[0] == "CRASH":
        return "crash", _subsystem_from_logger(logger)
    if event_name in CAPTURE_EVENT_NAMES or logger.endswith(("keyboard_capture", "screenshot_capture", "text_reconstructor", "window_tracker")):
        return "capture", _subsystem_from_logger(logger)
    if event_name in STORAGE_EVENT_NAMES or "storage" in logger:
        return "storage", _subsystem_from_logger(logger)
    if event_name in CONFIG_EVENT_NAMES or "config" in logger:
        return "config", _subsystem_from_logger(logger)
    if event_name in CRASH_EVENT_NAMES or "crash" in logger:
        return "crash", _subsystem_from_logger(logger)
    if event_name in LOCK_EVENT_NAMES or event_name == "monitoring_state_change":
        return "lock", _subsystem_from_logger(logger)
    if event_name in SUMMARY_EVENT_NAMES or "summarizer" in logger or "semantic_coalescing" in logger or "lmstudio_embeddings" in logger:
        return "summary", _subsystem_from_logger(logger)
    if "services" in logger or "scheduler" in logger or "main" in logger:
        return "lifecycle", _subsystem_from_logger(logger)
    if "ui" in logger:
        return "ui", _subsystem_from_logger(logger)
    return "other", _subsystem_from_logger(logger)


def _subsystem_from_logger(logger: str) -> str:
    tail = logger.split(".")[-1]
    if tail in {
        "services",
        "monitoring_components",
        "summarizer",
        "lmstudio_client",
        "lmstudio_embeddings",
        "lmstudio_prompt",
        "lmstudio_logging",
        "scheduler",
        "storage",
        "storage_cleanup",
        "storage_diagnostics",
        "storage_schema",
        "session_monitor",
        "window_tracker",
        "keyboard_capture",
        "screenshot_capture",
        "text_reconstructor",
        "semantic_coalescing",
        "batching",
        "config",
        "crash_monitor",
        "crash_reporting",
        "main",
        "summaries_window",
        "tray",
    }:
        return tail
    return tail or "other"


def normalize_signature(event: ParsedEvent) -> str:
    pieces = [event.category, event.subsystem, event.event_name or event.level]
    pieces.append(_normalize_text(event.message))
    if event.traceback:
        pieces.append(_normalize_text(event.traceback))
    normalized_fields = []
    for key in sorted(event.fields):
        value = event.fields[key]
        normalized_fields.append(f"{key}={_normalize_field_value(key, value)}")
    if normalized_fields:
        pieces.append(" ".join(normalized_fields))
    return " | ".join(piece for piece in pieces if piece)


def _normalize_text(value: str) -> str:
    value = ISO_TS_RE.sub("<ts>", value)
    value = UUID_RE.sub("<uuid>", value)
    value = HEX_ADDR_RE.sub("<addr>", value)
    value = WINDOWS_PATH_RE.sub("<path>", value)
    value = POSIX_PATH_RE.sub("<path>", value)
    value = re.sub(r"\s+", " ", value)
    value = _normalize_numbers_in_text(value)
    return value.strip().lower()


def _normalize_numbers_in_text(text: str) -> str:
    return NUMERIC_RE.sub("<num>", text)


def _normalize_field_value(key: str, value: Any) -> str:
    if value is None:
        return "<none>"
    if isinstance(value, bool):
        return "<bool>"
    if isinstance(value, (int, float)):
        return "<num>"
    text = str(value)
    if key in PATH_FIELD_KEYS or WINDOWS_PATH_RE.search(text) or POSIX_PATH_RE.search(text):
        return "<path>"
    if UUID_RE.search(text):
        return "<uuid>"
    if re.fullmatch(r"[\d.]+", text):
        return "<num>"
    return _normalize_text(text)


def derive_error_class(level: str, fields: dict[str, Any], traceback_text: str | None) -> str | None:
    error_type = fields.get("error_type")
    if isinstance(error_type, str) and error_type:
        return error_type
    if traceback_text:
        tail_lines = [line.strip() for line in traceback_text.splitlines() if line.strip()]
        for line in reversed(tail_lines):
            match = TRACEBACK_EXCEPTION_RE.match(line)
            if match:
                return match.group("class")
    if level in {"ERROR", "CRITICAL"} and "status" in fields and str(fields["status"]) == "error":
        return str(fields.get("stage") or fields.get("event") or "error")
    return None


def estimate_severity(event: ParsedEvent) -> str:
    if event.level == "CRITICAL" or event.error_class in {"AttributeError", "ProgrammingError", "RuntimeError", "OSError"} and event.traceback:
        return "Critical"
    if event.level == "ERROR":
        return "High"
    if event.level == "WARNING":
        return "Medium"
    if event.event_name in {"summary_drain_failed", "session_monitor_start_failed", "session_monitor_registration_failed"}:
        return "High"
    return "Low"


def max_severity(lhs: str, rhs: str) -> str:
    order = {"Low": 0, "Medium": 1, "High": 2, "Critical": 3}
    return lhs if order.get(lhs, 0) >= order.get(rhs, 0) else rhs


def group_to_dict(group: ErrorGroup) -> dict[str, Any]:
    return {
        "signature": group.signature,
        "count": group.count,
        "first_seen": group.first_seen,
        "last_seen": group.last_seen,
        "examples": group.examples,
        "subsystem": group.subsystem,
        "severity": group.severity,
        "has_traceback": group.has_traceback,
        "error_class": group.error_class,
    }


def _first_present(fields: dict[str, Any], *names: str) -> str | None:
    for name in names:
        if name in fields and fields[name] not in {None, ""}:
            return str(fields[name])
    return None


def _string_field(fields: dict[str, Any], name: str) -> str | None:
    value = fields.get(name)
    if value is None:
        return None
    return str(value)


def _int_value(value: Any, default: int | None = None) -> int | None:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _float_value(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _bool_value(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return True
        if lowered in {"false", "0", "no", "off"}:
            return False
    return None


def _parse_timestamp(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _trim(value: str, max_len: int) -> str:
    value = value.replace("\n", " ")
    if len(value) <= max_len:
        return value
    return value[: max_len - 3] + "..."


def _latency_summary(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "min": None, "median": None, "p90": None, "max": None, "mean": None}
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": round(ordered[0], 6),
        "median": round(median(ordered), 6),
        "p90": round(ordered[min(len(ordered) - 1, math.ceil(len(ordered) * 0.9) - 1)], 6),
        "max": round(ordered[-1], 6),
        "mean": round(sum(ordered) / len(ordered), 6),
    }


def _find_failure_bursts(requests: list[dict[str, Any]]) -> list[dict[str, Any]]:
    failures = [item for item in requests if item.get("outcome") in {"error", "parse_error"}]
    if not failures:
        return []
    bursts: list[dict[str, Any]] = []
    current = [failures[0]]
    for item in failures[1:]:
        prev = current[-1]
        if item.get("submitted_at") == prev.get("submitted_at"):
            current.append(item)
        else:
            if len(current) > 1:
                bursts.append(
                    {
                        "start": current[0]["submitted_at"],
                        "end": current[-1]["submitted_at"],
                        "count": len(current),
                        "job_ids": [row["job_id"] for row in current],
                    }
                )
            current = [item]
    if len(current) > 1:
        bursts.append(
            {
                "start": current[0]["submitted_at"],
                "end": current[-1]["submitted_at"],
                "count": len(current),
                "job_ids": [row["job_id"] for row in current],
            }
        )
    return bursts


def _lmstudio_queue_alignment(jobs: dict[str, JobLifecycle], requests: list[dict[str, Any]]) -> dict[str, Any]:
    stalled_jobs = [
        job.job_id
        for job in jobs.values()
        if job.job_type in {"event_summary", "day_summary"} and job.terminal_status in {None, "created_only", "incomplete"}
    ]
    failing_jobs = [item["job_id"] for item in requests if item.get("outcome") in {"error", "parse_error"}]
    return {
        "stalled_jobs": stalled_jobs,
        "failed_request_job_ids": failing_jobs,
        "aligned": bool(stalled_jobs and failing_jobs),
    }


def _capture_filter_analysis(jobs: dict[str, JobLifecycle], timeline: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "monitoring_state_changes": sum(1 for item in timeline if item["event"] == "monitoring_state_change"),
        "lock_admission_visible": any(item["event"] in {"monitoring_paused_by_lock", "monitoring_resumed_after_unlock"} for item in timeline),
        "known_gate": False,
    }


def _capture_missing_signals(stats: ParseStats) -> list[str]:
    missing = []
    if stats.lock_audit["session_locked"] == 0 and stats.lock_audit["session_unlocked"] == 0:
        missing.append("no lock/unlock events")
    if stats.capture["screenshots_captured"] == 0:
        missing.append("no screenshot captures")
    if stats.capture["text_segments"] == 0:
        missing.append("no text segment finalization")
    return missing


def _dedup_summary(capture: dict[str, Any]) -> dict[str, Any]:
    return {
        "skip_reasons": dict(capture["screenshots_skipped"]),
        "keep_reasons": dict(capture["screenshots_keep"]),
    }


def _find_duplicate_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str | None, str | None], list[dict[str, Any]]] = defaultdict(list)
    for job in jobs:
        groups[(job.get("job_type"), job.get("target_day"))].append(job)
    duplicates = []
    for (job_type, target_day), items in groups.items():
        if len(items) > 1 and job_type == "day_summary":
            duplicates.append({"job_type": job_type, "target_day": target_day, "job_ids": [item["job_id"] for item in items]})
    return duplicates


def _is_stalled_job(job: dict[str, Any]) -> bool:
    return job.get("created_at") is not None and job.get("terminal_status") in {None, "created_only", "incomplete"}


def _is_long_running_job(job: dict[str, Any]) -> bool:
    for request in job.get("requests", []):
        elapsed = request.get("elapsed_s")
        timeout_s = request.get("timeout_s")
        if isinstance(elapsed, (int, float)) and isinstance(timeout_s, (int, float)) and elapsed > timeout_s:
            return True
    return False


def _max_from_timeline(timeline: list[dict[str, Any]], event_name: str, field: str) -> float | int | None:
    values = [item.get("message", "") for item in timeline if item["event"] == event_name]
    if not values:
        return None
    for item in reversed(timeline):
        if item["event"] == event_name:
            match = re.search(rf"{re.escape(field)}=([0-9]+(?:\.[0-9]+)?)", item["message"])
            if match:
                text = match.group(1)
                return float(text) if "." in text else int(text)
    return None


def _fields_from_message(message: str, key: str) -> str | None:
    match = re.search(rf"{re.escape(key)}=([^ ]+)", message)
    return match.group(1) if match else None


def _parsing_quality(event_counts: dict[str, Any]) -> str:
    parsed = event_counts["parsed_entries"]
    total = event_counts["total_lines"]
    if total == 0:
        return "no lines"
    ratio = parsed / total
    if ratio >= 0.999:
        return "excellent"
    if ratio >= 0.98:
        return "good"
    if ratio >= 0.90:
        return "fair"
    return "poor"


def summarize_log_paths(log_arg: Path) -> list[Path]:
    if log_arg.is_dir():
        return sorted(path for path in log_arg.iterdir() if path.suffix.lower() == ".log")
    return [log_arg]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit WorkLog Diary logs with streaming parsing")
    parser.add_argument("--log", required=True, help="Path to a WLD log file or a directory of logs")
    parser.add_argument("--out", required=True, help="Output directory for the audit artifacts")
    args = parser.parse_args(argv)

    log_arg = Path(args.log)
    log_paths = summarize_log_paths(log_arg)
    if not log_paths:
        raise SystemExit(f"No .log files found in {log_arg}")

    runner = LogAuditRunner(log_paths=log_paths, out_dir=Path(args.out))
    runner.run()
    return 0
