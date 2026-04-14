from __future__ import annotations

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
