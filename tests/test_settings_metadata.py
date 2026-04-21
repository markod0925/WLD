from __future__ import annotations

from worklog_diary.ui.settings_metadata import EDITABLE_SETTINGS, READONLY_SETTINGS, UI_SETTINGS_BY_KEY


def test_settings_metadata_covers_all_exposed_settings() -> None:
    editable_keys = {item.key for item in EDITABLE_SETTINGS}
    readonly_keys = {item.key for item in READONLY_SETTINGS}

    assert editable_keys == {
        "blocked_processes",
        "screenshot_interval_seconds",
        "capture_mode",
        "foreground_poll_interval_seconds",
        "text_inactivity_gap_seconds",
        "reconstruction_poll_interval_seconds",
        "flush_interval_seconds",
        "max_parallel_summary_jobs",
        "max_screenshots_per_summary",
        "max_text_segments_per_summary",
        "lmstudio_base_url",
        "lmstudio_model",
        "request_timeout_seconds",
    }
    assert readonly_keys == {"app_data_dir", "db_path", "log_dir", "screenshot_dir"}


def test_each_exposed_setting_has_multiline_tooltip() -> None:
    for item in UI_SETTINGS_BY_KEY.values():
        lines = [line for line in item.tooltip.splitlines() if line.strip()]
        assert len(lines) >= 4
        assert all(":" in line for line in lines[:3])
