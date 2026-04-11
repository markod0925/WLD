from __future__ import annotations

from worklog_diary.core.screenshot_capture import resolve_capture_region



def test_full_screen_mode_returns_full_monitor_region() -> None:
    monitor = {"left": -100, "top": 20, "width": 1920, "height": 1080}
    region = resolve_capture_region("full_screen", monitor, window_rect=(0, 0, 10, 10))
    assert region == monitor



def test_active_window_mode_clips_to_visible_monitor_area() -> None:
    monitor = {"left": 0, "top": 0, "width": 100, "height": 100}
    region = resolve_capture_region("active_window", monitor, window_rect=(-20, 10, 40, 140))
    assert region == {"left": 0, "top": 10, "width": 40, "height": 90}



def test_active_window_mode_rejects_invalid_or_offscreen_rect() -> None:
    monitor = {"left": 0, "top": 0, "width": 100, "height": 100}

    assert resolve_capture_region("active_window", monitor, window_rect=None) is None
    assert resolve_capture_region("active_window", monitor, window_rect=(10, 10, 10, 40)) is None
    assert resolve_capture_region("active_window", monitor, window_rect=(200, 200, 260, 260)) is None
