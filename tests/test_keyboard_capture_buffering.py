from __future__ import annotations

from worklog_diary.core.keyboard_capture import KeyboardCaptureService
from worklog_diary.core.models import ForegroundInfo, KeyEvent, SharedState
from worklog_diary.core.privacy import PrivacyPolicyEngine


class RecordingStorage:
    def __init__(self) -> None:
        self.batches: list[list[KeyEvent]] = []

    def insert_key_events(self, events: list[KeyEvent]) -> int:
        self.batches.append(list(events))
        return len(events)


def _foreground(process_name: str = "code.exe", hwnd: int = 42, pid: int = 24) -> ForegroundInfo:
    return ForegroundInfo(
        timestamp=100.0,
        hwnd=hwnd,
        pid=pid,
        process_name=process_name,
        window_title="Editor",
    )


def test_keyboard_capture_batches_down_events_before_persisting() -> None:
    storage = RecordingStorage()
    state = SharedState()
    state.set_monitoring_active(True)
    info = _foreground()
    state.update_foreground(info, blocked=False, active_interval_id=11)

    service = KeyboardCaptureService(
        storage=storage,  # type: ignore[arg-type]
        state=state,
        privacy=PrivacyPolicyEngine(set()),
        foreground_provider=lambda: info,
        batch_size=2,
        flush_interval_seconds=60.0,
    )

    service._handle_event("a", "down")
    assert storage.batches == []

    service._handle_event("b", "down")
    assert len(storage.batches) == 1
    assert [event.key for event in storage.batches[0]] == ["a", "b"]


def test_keyboard_capture_ignores_key_up_persistence_but_releases_modifiers() -> None:
    storage = RecordingStorage()
    state = SharedState()
    state.set_monitoring_active(True)
    info = _foreground()
    state.update_foreground(info, blocked=False, active_interval_id=12)

    service = KeyboardCaptureService(
        storage=storage,  # type: ignore[arg-type]
        state=state,
        privacy=PrivacyPolicyEngine(set()),
        foreground_provider=lambda: info,
        batch_size=10,
        flush_interval_seconds=60.0,
    )

    service._handle_event("Key.ctrl", "down")
    service._handle_event("c", "down")
    service._handle_event("Key.ctrl", "up")
    service._handle_event("d", "down")
    service.flush_pending_events()

    assert len(storage.batches) == 1
    persisted = storage.batches[0]
    assert [event.event_type for event in persisted] == ["down", "down", "down"]
    assert persisted[1].modifiers == ["ctrl"]
    assert persisted[2].modifiers == []
