from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest

from worklog_diary import app as app_module
from worklog_diary.core.config import AppConfig, save_config
from worklog_diary.core.security.db_key_manager import DatabaseKeyMissingError
from worklog_diary.core.services import MonitoringServices


def _write_config(tmp_path: Path) -> tuple[Path, Path, Path]:
    app_data_dir = tmp_path / "data"
    db_path = app_data_dir / "worklog_diary.db"
    config_path = tmp_path / "config.json"
    config = AppConfig(
        app_data_dir=str(app_data_dir),
        screenshot_dir=str(app_data_dir / "screenshots"),
        log_dir=str(app_data_dir / "logs"),
        db_path=str(db_path),
        config_path=str(config_path),
    )
    save_config(config, config_path)
    return config_path, db_path, app_data_dir


def test_run_desktop_app_shows_startup_error_for_missing_database_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, db_path, app_data_dir = _write_config(tmp_path)
    db_path.write_bytes(b"existing encrypted database")
    shown: list[BaseException] = []

    fake_startup_errors = types.ModuleType("worklog_diary.ui.startup_errors")
    fake_startup_errors.show_encrypted_database_startup_error = shown.append
    monkeypatch.setitem(sys.modules, "worklog_diary.ui.startup_errors", fake_startup_errors)

    fake_tray = types.ModuleType("worklog_diary.ui.tray")

    def _unexpected_tray_start(_services: object) -> int:
        raise AssertionError("normal tray startup should not run after a missing key")

    fake_tray.run_tray_app = _unexpected_tray_start
    monkeypatch.setitem(sys.modules, "worklog_diary.ui.tray", fake_tray)

    def _unexpected_run_protected(*_args: object, **_kwargs: object) -> int:
        raise AssertionError("known startup errors should not enter run_protected")

    monkeypatch.setattr(app_module, "run_protected", _unexpected_run_protected)

    assert app_module.run_desktop_app(config_path=str(config_path)) == 1

    assert len(shown) == 1
    assert isinstance(shown[0], DatabaseKeyMissingError)
    assert not (app_data_dir / "db_key.bin").exists()
    assert not db_path.with_name(f"{db_path.name}.cipherkey").exists()


def test_missing_database_key_marks_crash_monitor_clean_and_stops_service_startup(tmp_path: Path) -> None:
    config_path, db_path, app_data_dir = _write_config(tmp_path)
    db_path.write_bytes(b"existing encrypted database")
    config = AppConfig.from_dict(json.loads(config_path.read_text(encoding="utf-8")), source=str(config_path))
    config.config_path = str(config_path)

    with pytest.raises(DatabaseKeyMissingError):
        MonitoringServices(config)

    state = json.loads((app_data_dir / "session_state.json").read_text(encoding="utf-8"))
    assert state["clean_shutdown"] is True
    assert state["exit_reason"] == "clean_shutdown"
    assert not (app_data_dir / "db_key.bin").exists()
    assert not db_path.with_name(f"{db_path.name}.cipherkey").exists()
