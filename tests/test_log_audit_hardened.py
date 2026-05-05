from __future__ import annotations

from pathlib import Path

from worklog_diary.tools.log_audit import LogAuditRunner


def _write_log(tmp_path: Path, name: str, lines: list[str]) -> Path:
    path = tmp_path / name
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_log_audit_reports_session_monitor_active_and_lock_counts(tmp_path: Path) -> None:
    log_path = _write_log(
        tmp_path,
        "session-monitor.log",
        [
            "2026-04-24 09:00:00 [INFO] worklog_diary.core.session_monitor: event=session_monitor_starting",
            "2026-04-24 09:00:01 [INFO] worklog_diary.core.session_monitor: event=session_monitor_started notifications_registered=true degraded_lock_monitoring=false hwnd_created=true lock_state=unknown",
            "2026-04-24 09:00:02 [INFO] worklog_diary.core.session_monitor: event=session_locked source=wts_session_change previous_lock_state=unknown new_lock_state=locked",
            "2026-04-24 09:00:03 [INFO] worklog_diary.core.summarizer: event=summary_backlog_gate_state process_backlog_only_while_locked=true lock_state=locked backlog_processing_allowed=true reason=allowed_pc_locked",
            "2026-04-24 09:00:04 [INFO] worklog_diary.core.session_monitor: event=session_unlocked source=wts_session_change previous_lock_state=locked new_lock_state=unlocked",
            "2026-04-24 09:00:05 [INFO] worklog_diary.core.summarizer: event=summary_backlog_gate_state process_backlog_only_while_locked=true lock_state=unlocked backlog_processing_allowed=false reason=blocked_pc_unlocked",
        ],
    )
    out_dir = tmp_path / "out"

    outputs = LogAuditRunner([log_path], out_dir).run()

    session_monitor = outputs["session_monitor"]
    backlog_gate = outputs["backlog_gate"]

    assert session_monitor["monitor_status"] == "active"
    assert session_monitor["start_failure_count"] == 0
    assert session_monitor["stop_failure_count"] == 0
    assert session_monitor["lock_event_count"] == 1
    assert session_monitor["unlock_event_count"] == 1
    assert session_monitor["first_observed_lock_state"] == "locked"
    assert session_monitor["last_observed_lock_state"] == "unlocked"
    assert backlog_gate["allowed_count"] == 1
    assert backlog_gate["blocked_count"] == 1
    assert backlog_gate["blocked_by_reason"] == {"blocked_pc_unlocked": 1}
    assert backlog_gate["unknown_or_degraded_count"] == 0


def test_log_audit_reports_session_monitor_degraded_unavailable_and_unknown(tmp_path: Path) -> None:
    degraded_log = _write_log(
        tmp_path,
        "degraded.log",
        [
            "2026-04-24 09:10:00 [INFO] worklog_diary.core.session_monitor: event=session_monitor_starting",
            "2026-04-24 09:10:01 [WARNING] worklog_diary.core.session_monitor: event=session_monitor_start_failed error_category=kernel32_get_module_handle exception_type=OSError exception_message=GetModuleHandleW failed error_type=OSError detail=GetModuleHandleW failed degraded_lock_monitoring=true last_error=5",
            "2026-04-24 09:10:02 [INFO] worklog_diary.core.session_monitor: event=summary_backlog_gate_state process_backlog_only_while_locked=true lock_state=degraded backlog_processing_allowed=false reason=blocked_lock_monitor_degraded",
        ],
    )
    unavailable_log = _write_log(
        tmp_path,
        "unavailable.log",
        [
            "2026-04-24 09:20:00 [INFO] worklog_diary.core.session_monitor: event=session_monitor_starting",
            "2026-04-24 09:20:01 [INFO] worklog_diary.core.session_monitor: event=session_monitor_started notifications_registered=true degraded_lock_monitoring=false hwnd_created=true lock_state=unknown",
            "2026-04-24 09:20:02 [INFO] worklog_diary.core.session_monitor: event=session_monitor_stopped monitor_state=unavailable",
        ],
    )
    unknown_log = _write_log(
        tmp_path,
        "unknown.log",
        [
            "2026-04-24 09:30:00 [INFO] worklog_diary.core.services: event=runtime_paths mode=dev",
            "2026-04-24 09:30:01 [INFO] worklog_diary.core.services: event=shutdown_complete",
        ],
    )
    out_dir = tmp_path / "out"

    degraded = LogAuditRunner([degraded_log], out_dir / "degraded").run()["session_monitor"]
    unavailable = LogAuditRunner([unavailable_log], out_dir / "unavailable").run()["session_monitor"]
    unknown = LogAuditRunner([unknown_log], out_dir / "unknown").run()["session_monitor"]

    assert degraded["monitor_status"] == "degraded"
    assert degraded["start_failure_count"] == 1
    assert unavailable["monitor_status"] == "unavailable"
    assert unavailable["stopped_count"] == 1
    assert unknown["monitor_status"] == "unknown"


def test_log_audit_reports_backlog_gate_reasons(tmp_path: Path) -> None:
    log_path = _write_log(
        tmp_path,
        "backlog-gate.log",
        [
            "2026-04-24 11:00:00 [INFO] worklog_diary.core.summarizer: event=summary_backlog_gate_state process_backlog_only_while_locked=false lock_state=unknown backlog_processing_allowed=true reason=allowed_config_disabled",
            "2026-04-24 11:00:01 [INFO] worklog_diary.core.summarizer: event=summary_backlog_gate_state process_backlog_only_while_locked=true lock_state=locked backlog_processing_allowed=true reason=allowed_pc_locked",
            "2026-04-24 11:00:02 [INFO] worklog_diary.core.summarizer: event=summary_backlog_gate_state process_backlog_only_while_locked=true lock_state=unknown backlog_processing_allowed=false reason=blocked_lock_state_unknown",
            "2026-04-24 11:00:03 [INFO] worklog_diary.core.summarizer: event=summary_backlog_gate_state process_backlog_only_while_locked=true lock_state=degraded backlog_processing_allowed=false reason=blocked_lock_monitor_degraded",
            "2026-04-24 11:00:04 [INFO] worklog_diary.core.summarizer: event=summary_backlog_gate_state process_backlog_only_while_locked=true lock_state=unlocked backlog_processing_allowed=false reason=blocked_pc_unlocked",
            "2026-04-24 11:00:05 [INFO] worklog_diary.core.summarizer: event=summary_backlog_gate_state process_backlog_only_while_locked=true lock_state=unknown backlog_processing_allowed=false reason=blocked_shutdown",
            "2026-04-24 11:00:06 [INFO] worklog_diary.core.summarizer: event=summary_backlog_gate_state process_backlog_only_while_locked=true lock_state=locked backlog_processing_allowed=true reason=allowed_manual_override",
        ],
    )
    out_dir = tmp_path / "out"

    backlog_gate = LogAuditRunner([log_path], out_dir).run()["backlog_gate"]

    assert backlog_gate["allowed_count"] == 3
    assert backlog_gate["blocked_count"] == 4
    assert backlog_gate["blocked_by_reason"] == {
        "blocked_lock_monitor_degraded": 1,
        "blocked_lock_state_unknown": 1,
        "blocked_pc_unlocked": 1,
        "blocked_shutdown": 1,
    }
    assert backlog_gate["unknown_or_degraded_count"] == 3
