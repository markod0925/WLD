from __future__ import annotations

from pathlib import Path

import worklog_diary.core.config as config_module


def test_app_config_default_settings_match_ui_defaults() -> None:
    cfg = config_module.AppConfig()

    assert cfg.screenshot_interval_seconds == 60
    assert cfg.capture_mode == "active_window"
    assert cfg.max_text_segments_per_summary == 400
    assert cfg.request_timeout_seconds == 600


def test_app_config_normalize_invalid_capture_mode_falls_back_to_active_window() -> None:
    cfg = config_module.AppConfig(capture_mode="totally-invalid")
    cfg.normalize()

    assert cfg.capture_mode == "active_window"


def test_load_config_in_frozen_mode_creates_portable_data_tree(tmp_path: Path, monkeypatch) -> None:
    exe_dir = tmp_path / "portable"
    exe_dir.mkdir()
    exe_path = exe_dir / "WLD.exe"

    monkeypatch.setattr(config_module.sys, "frozen", True, raising=False)
    monkeypatch.setattr(config_module.sys, "executable", str(exe_path), raising=False)
    monkeypatch.delenv("WORKLOG_DIARY_APP_DATA_DIR", raising=False)
    monkeypatch.delenv("WORKLOG_DIARY_CONFIG", raising=False)

    cfg = config_module.load_config()

    assert cfg.app_data_dir == str(exe_dir / "data")
    assert cfg.config_path == str(exe_dir / "data" / "config.json")
    assert cfg.db_path == str(exe_dir / "data" / "worklog_diary.db")
    assert cfg.screenshot_dir == str(exe_dir / "data" / "screenshots")
    assert cfg.log_dir == str(exe_dir / "data" / "logs")

    assert Path(cfg.config_path).exists()
    assert Path(cfg.db_path).parent.exists()
    assert Path(cfg.screenshot_dir).exists()
    assert Path(cfg.log_dir).exists()
