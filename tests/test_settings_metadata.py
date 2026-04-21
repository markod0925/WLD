from __future__ import annotations

from dataclasses import fields

from worklog_diary.core.config import AppConfig
from worklog_diary.ui.settings_metadata import (
    ADVANCED_SETTINGS,
    DEBUG_SETTINGS,
    EXPOSURE_BY_KEY,
    UI_SETTINGS_BY_KEY,
    USER_SETTINGS,
    modified_debug_keys,
)


def test_all_app_config_fields_are_classified_by_exposure_level() -> None:
    app_config_keys = {field.name for field in fields(AppConfig)}
    assert set(EXPOSURE_BY_KEY) == app_config_keys


def test_exposed_settings_have_metadata_and_tooltips() -> None:
    exposed_keys = {
        key for key, exposure in EXPOSURE_BY_KEY.items() if exposure in {"user", "advanced", "debug"}
    }
    assert set(UI_SETTINGS_BY_KEY) == exposed_keys

    for item in UI_SETTINGS_BY_KEY.values():
        lines = [line for line in item.tooltip.splitlines() if line.strip()]
        assert len(lines) >= 4
        assert all(":" in line for line in lines[:3])


def test_settings_are_grouped_by_exposure_level() -> None:
    assert {item.key for item in USER_SETTINGS} == {
        "blocked_processes",
        "screenshot_interval_seconds",
        "capture_mode",
        "flush_interval_seconds",
        "lmstudio_base_url",
        "lmstudio_model",
        "start_monitoring_on_launch",
    }

    assert {item.key for item in ADVANCED_SETTINGS} == {
        "foreground_poll_interval_seconds",
        "text_inactivity_gap_seconds",
        "reconstruction_poll_interval_seconds",
        "app_data_dir",
        "screenshot_dir",
        "log_dir",
        "db_path",
        "max_parallel_summary_jobs",
        "max_screenshots_per_summary",
        "max_text_segments_per_summary",
        "request_timeout_seconds",
    }

    assert {item.key for item in DEBUG_SETTINGS} == {
        "lmstudio_max_prompt_chars",
        "activity_segment_min_duration_seconds",
        "activity_segment_max_duration_seconds",
        "activity_segment_idle_gap_seconds",
        "activity_segment_title_similarity_threshold",
        "summary_similarity_suppress_threshold",
        "summary_similarity_merge_threshold",
        "summary_cooldown_seconds",
        "recent_summary_compare_count",
        "screenshot_dedup_enabled",
        "screenshot_dedup_exact_hash_enabled",
        "screenshot_dedup_perceptual_hash_enabled",
        "screenshot_dedup_phash_threshold",
        "screenshot_dedup_ssim_enabled",
        "screenshot_dedup_ssim_threshold",
        "screenshot_dedup_resize_width",
        "screenshot_dedup_compare_recent_count",
        "screenshot_min_keep_interval_seconds",
    }


def test_path_settings_are_visible_and_require_restart() -> None:
    for key in ("app_data_dir", "screenshot_dir", "log_dir", "db_path"):
        metadata = UI_SETTINGS_BY_KEY[key]
        assert EXPOSURE_BY_KEY[key] == "advanced"
        assert metadata.requires_restart is True
        assert metadata.tooltip


def test_debug_settings_are_marked_experimental() -> None:
    for setting in DEBUG_SETTINGS:
        assert setting.is_experimental is True


def test_modified_debug_keys_detects_changes() -> None:
    unchanged = {
        "screenshot_dedup_enabled": True,
        "summary_cooldown_seconds": 240,
        "screenshot_dedup_ssim_threshold": 0.985,
    }
    assert modified_debug_keys(unchanged) == []

    changed = unchanged | {"summary_cooldown_seconds": 300, "screenshot_dedup_enabled": False}
    assert set(modified_debug_keys(changed)) == {"summary_cooldown_seconds", "screenshot_dedup_enabled"}
