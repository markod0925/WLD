from __future__ import annotations

from worklog_diary.ui.tray_status_view_model import (
    TrayStatusSnapshot,
    build_tray_menu_actions,
    build_tray_status_snapshot,
    format_tray_tooltip,
)


def _base_status(**overrides: object) -> dict[str, object]:
    status: dict[str, object] = {
        "monitoring_active": True,
        "monitoring_requested": True,
        "manual_pause": False,
        "paused_by_lock": False,
        "shutdown_in_progress": False,
        "flush_drain_active": False,
        "blocked": False,
        "pending_text_segment_count": 0,
        "pending_screenshot_count": 0,
        "pending_summary_job_count": 0,
        "summary_jobs": {
            "queued": 0,
            "running": 0,
        },
        "llm_queue": {
            "accepting_jobs": True,
            "closing": False,
            "closed": False,
            "max_concurrent": 2,
        },
        "summary_admission_paused": False,
        "process_backlog_only_while_locked": False,
        "unrecoverable_summary_error": None,
    }
    status.update(overrides)
    return status


def test_tray_snapshot_and_menu_share_the_same_state_model() -> None:
    snapshot = build_tray_status_snapshot(
        _base_status(
            pending_screenshot_count=1,
            pending_text_segment_count=0,
        )
    )

    tooltip_lines = format_tray_tooltip(snapshot).splitlines()
    assert tooltip_lines == [
        "WorkLog Diary: Active",
        "Capture: 1 screenshot buffered, 0 text segments buffered",
        "LLM: idle",
        "Queue: 0 queued, 0 in flight, max 2",
    ]

    menu_actions = build_tray_menu_actions(snapshot)
    assert [action.label for action in menu_actions] == [
        "Show Summaries",
        "Search Summaries",
        "Pause Capture",
        "Flush Now",
        "Settings",
        "Quit",
    ]
    assert all("batch" not in action.label.lower() for action in menu_actions)
    assert all("Estimated summaries" not in line for line in tooltip_lines)


def test_tray_tooltip_is_bounded_and_truncates_long_lines() -> None:
    snapshot = TrayStatusSnapshot(
        state_label="Active",
        detail_lines=("X" * 200,),
        monitoring_active=True,
        monitoring_requested=True,
        manual_pause=False,
        paused_by_lock=False,
        shutdown_in_progress=False,
        flush_drain_active=False,
    )

    tooltip_lines = format_tray_tooltip(snapshot).splitlines()
    assert len(tooltip_lines) == 2
    assert len(tooltip_lines[0]) <= 96
    assert len(tooltip_lines[1]) <= 96


def test_tray_snapshot_omits_estimated_summaries_for_small_backlogs() -> None:
    snapshot = build_tray_status_snapshot(
        _base_status(
            pending_screenshot_count=1,
            pending_text_segment_count=0,
            summary_admission_paused=True,
            process_backlog_only_while_locked=True,
        )
    )

    tooltip = format_tray_tooltip(snapshot)
    assert "Estimated summaries:" not in tooltip
    assert "approx" not in tooltip.lower()


def test_tray_snapshot_shows_pc_lock_backlog_gate() -> None:
    snapshot = build_tray_status_snapshot(
        _base_status(
            pending_screenshot_count=1,
            pending_text_segment_count=0,
            summary_admission_paused=True,
            process_backlog_only_while_locked=True,
        )
    )

    assert "Backlog: waiting for PC lock" in snapshot.detail_lines


def test_tray_snapshot_marks_llm_unavailable_compactly() -> None:
    snapshot = build_tray_status_snapshot(
        _base_status(
            unrecoverable_summary_error="LM Studio unavailable",
        )
    )

    tooltip_lines = format_tray_tooltip(snapshot).splitlines()
    assert "LLM: unavailable" in tooltip_lines


def test_tray_snapshot_distinguishes_paused_and_stopped_states() -> None:
    paused_snapshot = build_tray_status_snapshot(
        _base_status(
            monitoring_active=False,
            manual_pause=True,
        )
    )
    stopped_snapshot = build_tray_status_snapshot(
        _base_status(
            monitoring_active=False,
            monitoring_requested=False,
        )
    )

    assert paused_snapshot.state_label == "Paused"
    assert "Reason: capture paused by user" in paused_snapshot.detail_lines
    assert stopped_snapshot.state_label == "Stopped"
    assert "Reason: capture stopped" in stopped_snapshot.detail_lines


def test_tray_menu_disables_operational_actions_while_shutting_down() -> None:
    snapshot = build_tray_status_snapshot(
        _base_status(
            shutdown_in_progress=True,
        )
    )

    tooltip_lines = format_tray_tooltip(snapshot).splitlines()
    assert tooltip_lines[0] == "WorkLog Diary: Shutting down"
    assert "Reason: shutdown in progress" in tooltip_lines

    actions = build_tray_menu_actions(snapshot)
    assert actions[-1].label == "Quit"
    assert actions[-1].enabled is True
    assert all(action.enabled is False for action in actions[:-1])
