from __future__ import annotations

from worklog_diary.ui.tray_status_view_model import (
    MAX_TOOLTIP_LINE_LENGTH,
    MAX_TOOLTIP_LINES,
    MAX_TOOLTIP_TOTAL_CHARS,
    TrayStatusSnapshot,
    _format_compact_number,
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
        "flush_state": "idle",
        "flush_blocker": None,
        "blocked": False,
        "pending_text_segment_count": 0,
        "pending_screenshot_count": 0,
        "pending_summary_job_count": 0,
        "summary_jobs": {"queued": 0, "running": 0},
        "llm_queue": {"accepting_jobs": True, "closing": False, "closed": False, "max_concurrent": 2},
        "summary_admission_paused": False,
        "process_backlog_only_while_locked": False,
        "lmstudio_state": "ok",
        "unrecoverable_summary_error": None,
        "pending_key_event_buffer_count": 0,
        "open_text_segment_active": False,
        "open_text_segment_char_count": 0,
    }
    status.update(overrides)
    return status


def test_tray_snapshot_and_menu_share_the_same_state_model() -> None:
    snapshot = build_tray_status_snapshot(_base_status(pending_screenshot_count=1))
    assert format_tray_tooltip(snapshot).splitlines() == ["WLD: Active", "Cap: 1 img, 0 txt", "LM: idle, Q 0"]
    assert [a.label for a in build_tray_menu_actions(snapshot)] == [
        "Show Summaries", "Search Summaries", "Pause Capture", "Flush Now", "Settings", "Quit"
    ]


def test_compact_number_policy_is_stable() -> None:
    assert _format_compact_number(0) == "0"
    assert _format_compact_number(999) == "999"
    assert _format_compact_number(1000) == "1.0k"
    assert _format_compact_number(1200) == "1.2k"
    assert _format_compact_number(9999) == "9.9k"
    assert _format_compact_number(10000) == "10k"
    assert _format_compact_number(12500) == "12k"
    assert _format_compact_number(999999) == "999k"


def test_active_capture_includes_current_buffer_when_it_fits() -> None:
    snapshot = build_tray_status_snapshot(
        _base_status(
            pending_screenshot_count=5,
            open_text_segment_active=True,
            open_text_segment_char_count=123,
            pending_key_event_buffer_count=45,
            summary_jobs={"queued": 0, "running": 1},
        )
    )
    tooltip = format_tray_tooltip(snapshot)
    assert "Cur: 123 ch, 45 key" in tooltip
    assert "LM: 1 run, Q 0" in tooltip


def test_crowded_state_keeps_priority_lines_and_drops_cur_first() -> None:
    snapshot = TrayStatusSnapshot(
        state_label="Active",
        detail_lines=(
            "Cap: 12k img, 12k txt with extra compact metadata",
            "Cur: 12k ch, 12k key with extra compact metadata",
            "LM: 12k run, wait lock with extra compact metadata",
        ),
        monitoring_active=True,
        monitoring_requested=True,
        manual_pause=False,
        paused_by_lock=False,
        shutdown_in_progress=False,
        flush_drain_active=False,
        flush_state="idle",
        flush_blocker=None,
        lmstudio_state="ok",
    )
    tooltip = format_tray_tooltip(snapshot)
    lines = tooltip.splitlines()

    assert len(tooltip) <= MAX_TOOLTIP_TOTAL_CHARS
    assert len(lines) <= MAX_TOOLTIP_LINES
    assert all(len(line) <= MAX_TOOLTIP_LINE_LENGTH for line in lines)
    assert "WLD:" in tooltip
    assert "Cap:" in tooltip
    assert "LM:" in tooltip
    assert "wait lock" in tooltip
    assert "Cur:" not in tooltip


def test_tooltip_budget_counts_newlines_in_total_string() -> None:
    snapshot = TrayStatusSnapshot(
        state_label="Active",
        detail_lines=("Cap: 9.9k img, 9.9k txt", "Cur: 9.9k ch, 9.9k key", "LM: 9.9k run, Q 9.9k"),
        monitoring_active=True,
        monitoring_requested=True,
        manual_pause=False,
        paused_by_lock=False,
        shutdown_in_progress=False,
        flush_drain_active=False,
        flush_state="idle",
        flush_blocker=None,
        lmstudio_state="ok",
    )
    tooltip = format_tray_tooltip(snapshot)
    assert len(tooltip) <= MAX_TOOLTIP_TOTAL_CHARS


def test_verbose_legacy_phrases_absent() -> None:
    tooltip = format_tray_tooltip(build_tray_status_snapshot(_base_status(pending_screenshot_count=1)))
    assert "screenshots buffered" not in tooltip
    assert "finalized text segments buffered" not in tooltip
    assert "processing 1 summary" not in tooltip
    assert "Backlog waiting for PC lock" not in tooltip
    assert "raw keys buffered" not in tooltip


def test_keyboard_hook_unavailable_warning_is_present() -> None:
    tooltip = format_tray_tooltip(
        build_tray_status_snapshot(
            _base_status(
                keyboard_hook_installed=False,
                pending_screenshot_count=1,
            )
        )
    )
    assert "Warn: kb hook unavailable" in tooltip


def test_flush_and_lm_degraded_states_are_exposed_compactly() -> None:
    tooltip = format_tray_tooltip(
        build_tray_status_snapshot(
            _base_status(
                flush_state="running",
                flush_blocker="waiting_for_daily_recap",
                lmstudio_state="offline",
                summary_jobs={"queued": 2, "running": 1},
                pending_screenshot_count=1,
            )
        )
    )
    assert "Flush: wait daily" in tooltip
    assert "LM: off, Q 2" in tooltip
