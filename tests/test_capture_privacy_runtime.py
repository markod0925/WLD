from __future__ import annotations

import threading
from pathlib import Path

from worklog_diary.core.config import native_hooks_disabled
from worklog_diary.core.keyboard_capture import KeyboardCaptureService
from worklog_diary.core.models import ForegroundInfo, SharedState
from worklog_diary.core.privacy import PrivacyPolicyEngine
from worklog_diary.core.session_monitor import SessionMonitor
from worklog_diary.core.screenshot_capture import ScreenshotCaptureService
from worklog_diary.core.storage import SQLiteStorage


def _foreground(process_name: str, hwnd: int = 42, pid: int = 24) -> ForegroundInfo:
    return ForegroundInfo(
        timestamp=100.0,
        hwnd=hwnd,
        pid=pid,
        process_name=process_name,
        window_title="Sensitive Window",
    )


def test_blocked_app_never_creates_key_events_even_if_state_is_stale(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "test.db"))
    state = SharedState()
    state.set_monitoring_active(True)
    blocked_info = _foreground("secret.exe")
    state.update_foreground(blocked_info, blocked=False, active_interval_id=10)

    service = KeyboardCaptureService(
        storage=storage,
        state=state,
        privacy=PrivacyPolicyEngine({"secret.exe"}),
        foreground_provider=lambda: blocked_info,
    )
    service._handle_event("a", "down")
    service.flush_pending_events()

    assert storage.get_diagnostics_snapshot()["table_counts"]["key_events"] == 0
    storage.close()


def test_blocked_app_never_creates_screenshots_even_if_state_is_stale(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "test.db"))
    state = SharedState()
    state.set_monitoring_active(True)
    blocked_info = _foreground("secret.exe")
    state.update_foreground(blocked_info, blocked=False, active_interval_id=10)

    service = ScreenshotCaptureService(
        storage=storage,
        state=state,
        privacy=PrivacyPolicyEngine({"secret.exe"}),
        screenshot_dir=str(tmp_path / "shots"),
        interval_seconds=30,
        foreground_provider=lambda: blocked_info,
    )

    assert service.capture_once() is False
    assert storage.get_diagnostics_snapshot()["table_counts"]["screenshots"] == 0
    storage.close()


def test_update_dedup_config_waits_for_dedup_lock(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "test.db"))
    state = SharedState()
    service = ScreenshotCaptureService(
        storage=storage,
        state=state,
        privacy=PrivacyPolicyEngine(set()),
        screenshot_dir=str(tmp_path / "shots"),
        interval_seconds=30,
    )

    service._dedup_lock.acquire()
    started = threading.Event()
    finished = threading.Event()

    def worker() -> None:
        started.set()
        service.update_dedup_config(
            exact_hash_enabled=False,
            perceptual_hash_enabled=False,
            phash_threshold=11,
            ssim_enabled=False,
            ssim_threshold=0.91,
            resize_width=48,
            compare_recent_count=4,
            min_keep_interval_seconds=30.0,
        )
        finished.set()

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    try:
        assert started.wait(timeout=1.0)
        assert not finished.wait(timeout=0.1)
    finally:
        service._dedup_lock.release()

    assert finished.wait(timeout=1.0)
    thread.join(timeout=1.0)
    assert service._dedup_state.phash_threshold == 11
    assert service._dedup_resize_width == 48
    storage.close()


def test_native_hooks_can_be_disabled_explicitly(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("WORKLOG_DIARY_DISABLE_NATIVE_HOOKS", "1")

    storage = SQLiteStorage(str(tmp_path / "test.db"))
    state = SharedState()
    service = KeyboardCaptureService(
        storage=storage,
        state=state,
        privacy=PrivacyPolicyEngine(set()),
    )

    monitor = SessionMonitor(on_locked=lambda: None, on_unlocked=lambda: None)

    assert native_hooks_disabled() is True
    service.start()
    monitor.start()
    assert service._listener is None
    assert monitor._thread is None
    storage.close()
