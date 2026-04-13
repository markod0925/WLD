from __future__ import annotations

from pathlib import Path

import worklog_diary.core.config as config_module


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
