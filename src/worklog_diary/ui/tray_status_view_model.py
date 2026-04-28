from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


MAX_TOOLTIP_LINES = 6
MAX_TOOLTIP_LINE_LENGTH = 96


@dataclass(slots=True, frozen=True)
class TrayMenuActionSpec:
    command: str
    label: str
    enabled: bool = True
    separator_before: bool = False


@dataclass(slots=True, frozen=True)
class TrayStatusSnapshot:
    state_label: str
    detail_lines: tuple[str, ...]
    monitoring_active: bool
    monitoring_requested: bool
    manual_pause: bool
    paused_by_lock: bool
    shutdown_in_progress: bool
    flush_drain_active: bool


def build_tray_status_snapshot(status: Mapping[str, Any]) -> TrayStatusSnapshot:
    monitoring_active = bool(status.get("monitoring_active"))
    monitoring_requested = bool(status.get("monitoring_requested"))
    manual_pause = bool(status.get("manual_pause"))
    paused_by_lock = bool(status.get("paused_by_lock"))
    shutdown_in_progress = bool(status.get("shutdown_in_progress"))
    flush_drain_active = bool(status.get("flush_drain_active"))
    blocked = bool(status.get("blocked"))
    summary_admission_paused = bool(status.get("summary_admission_paused"))
    process_backlog_only_while_locked = bool(status.get("process_backlog_only_while_locked"))

    pending_text_segments = _coerce_int(status.get("pending_text_segment_count"))
    pending_screenshots = _coerce_int(status.get("pending_screenshot_count"))
    pending_summary_jobs = _coerce_int(status.get("pending_summary_job_count"))

    summary_jobs = status.get("summary_jobs")
    if isinstance(summary_jobs, Mapping):
        queued_jobs = _coerce_int(summary_jobs.get("queued"))
        running_jobs = _coerce_int(summary_jobs.get("running"))
    else:
        queued_jobs = 0
        running_jobs = 0

    llm_queue = status.get("llm_queue")
    llm_accepting_jobs = True
    llm_closing = False
    llm_closed = False
    if isinstance(llm_queue, Mapping):
        llm_accepting_jobs = bool(llm_queue.get("accepting_jobs", True))
        llm_closing = bool(llm_queue.get("closing"))
        llm_closed = bool(llm_queue.get("closed"))
        llm_max_concurrent = _coerce_int(llm_queue.get("max_concurrent"))
    else:
        llm_max_concurrent = 0

    unrecoverable_error = status.get("unrecoverable_summary_error")
    has_pending_activity = (
        pending_text_segments > 0
        or pending_screenshots > 0
        or queued_jobs > 0
        or running_jobs > 0
        or pending_summary_jobs > 0
    )

    if shutdown_in_progress:
        state_label = "Shutting down"
    elif not monitoring_requested:
        state_label = "Stopped"
    elif not monitoring_active:
        state_label = "Paused"
    else:
        state_label = "Active"

    detail_lines: list[str] = []
    if shutdown_in_progress:
        detail_lines.append("Reason: shutdown in progress")
    elif state_label == "Paused":
        if paused_by_lock:
            detail_lines.append("Reason: capture paused while PC is locked")
        elif manual_pause:
            detail_lines.append("Reason: capture paused by user")
        else:
            detail_lines.append("Reason: capture paused")
    elif state_label == "Stopped":
        detail_lines.append("Reason: capture stopped")

    if state_label == "Active":
        if blocked:
            capture_line = "Capture: blocked by foreground app"
        elif has_pending_activity:
            capture_line = _format_capture_line(pending_screenshots, pending_text_segments)
        else:
            capture_line = "Capture: no pending activity"
        detail_lines.append(capture_line)
        if blocked and has_pending_activity:
            detail_lines.append(_format_pending_line(pending_screenshots, pending_text_segments))
    elif has_pending_activity and not shutdown_in_progress:
        detail_lines.append(_format_capture_line(pending_screenshots, pending_text_segments))

    if shutdown_in_progress or unrecoverable_error or not llm_accepting_jobs or llm_closing or llm_closed:
        llm_line = "LLM: unavailable"
    elif running_jobs > 0:
        llm_line = f"LLM: processing {_format_count_label(running_jobs, 'summary', 'summaries')}"
    else:
        llm_line = "LLM: idle"
    detail_lines.append(llm_line)

    if llm_max_concurrent > 0:
        detail_lines.append(f"Queue: {queued_jobs} queued, {running_jobs} in flight, max {llm_max_concurrent}")
    else:
        detail_lines.append(f"Queue: {queued_jobs} queued, {running_jobs} running")

    if (
        process_backlog_only_while_locked
        and summary_admission_paused
        and has_pending_activity
        and not shutdown_in_progress
        and state_label == "Active"
    ):
        detail_lines.append("Backlog: waiting for PC lock")

    normalized_lines = tuple(
        _truncate_line(_normalize_whitespace(line), MAX_TOOLTIP_LINE_LENGTH)
        for line in detail_lines
        if _normalize_whitespace(line)
    )
    return TrayStatusSnapshot(
        state_label=state_label,
        detail_lines=normalized_lines,
        monitoring_active=monitoring_active,
        monitoring_requested=monitoring_requested,
        manual_pause=manual_pause,
        paused_by_lock=paused_by_lock,
        shutdown_in_progress=shutdown_in_progress,
        flush_drain_active=flush_drain_active,
    )


def format_tray_tooltip(snapshot: TrayStatusSnapshot) -> str:
    lines = [f"WorkLog Diary: {snapshot.state_label}", *snapshot.detail_lines]
    normalized = [_truncate_line(_normalize_whitespace(line), MAX_TOOLTIP_LINE_LENGTH) for line in lines if line]
    return "\n".join(normalized[:MAX_TOOLTIP_LINES])


def build_tray_menu_actions(snapshot: TrayStatusSnapshot) -> list[TrayMenuActionSpec]:
    enabled = not snapshot.shutdown_in_progress
    if snapshot.state_label == "Active":
        capture_command = "pause_capture"
        capture_label = "Pause Capture"
    elif snapshot.monitoring_requested:
        capture_command = "resume_capture"
        capture_label = "Resume Capture"
    else:
        capture_command = "start_capture"
        capture_label = "Start Capture"

    if snapshot.flush_drain_active:
        flush_command = "stop_flush_drain"
        flush_label = "Stop Flush Drain"
        flush_enabled = enabled
    else:
        flush_command = "flush_now"
        flush_label = "Flush Now"
        flush_enabled = enabled

    return [
        TrayMenuActionSpec(command="show_summaries", label="Show Summaries", enabled=enabled),
        TrayMenuActionSpec(command="search_summaries", label="Search Summaries", enabled=enabled),
        TrayMenuActionSpec(command=capture_command, label=capture_label, enabled=enabled, separator_before=True),
        TrayMenuActionSpec(command=flush_command, label=flush_label, enabled=flush_enabled, separator_before=True),
        TrayMenuActionSpec(command="settings", label="Settings", enabled=enabled, separator_before=True),
        TrayMenuActionSpec(command="quit", label="Quit", enabled=True, separator_before=True),
    ]


def _format_capture_line(pending_screenshots: int, pending_text_segments: int) -> str:
    screenshot_label = _format_count_label(pending_screenshots, "screenshot buffered", "screenshots buffered")
    text_label = _format_count_label(pending_text_segments, "text segment buffered", "text segments buffered")
    return f"Capture: {screenshot_label}, {text_label}"


def _format_pending_line(pending_screenshots: int, pending_text_segments: int) -> str:
    screenshot_label = _format_count_label(pending_screenshots, "screenshot buffered", "screenshots buffered")
    text_label = _format_count_label(pending_text_segments, "text segment buffered", "text segments buffered")
    return f"Pending: {screenshot_label}, {text_label}"


def _format_count_label(count: int, singular: str, plural: str) -> str:
    if count == 1:
        return f"1 {singular}"
    return f"{count} {plural}"


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _normalize_whitespace(value: str) -> str:
    return " ".join(value.split())


def _truncate_line(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)].rstrip() + "..."
