from __future__ import annotations

from pathlib import Path

from worklog_diary.core.config import AppConfig
from worklog_diary.core.services import MonitoringServices


def _config_for_tmp(tmp_path: Path) -> AppConfig:
    data = AppConfig().to_dict()
    data.update(
        {
            "app_data_dir": str(tmp_path / "app"),
            "screenshot_dir": str(tmp_path / "app" / "screenshots"),
            "db_path": str(tmp_path / "app" / "worklog.db"),
            "config_path": str(tmp_path / "app" / "config.json"),
        }
    )
    return AppConfig.from_dict(data)


def test_diagnostics_service_delegates_status_and_snapshot(tmp_path: Path) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path))
    try:
        assert services.diagnostics_service.get_status() == services.get_status()
        assert services.diagnostics_service.get_diagnostics_snapshot() == services.storage.get_diagnostics_snapshot()
    finally:
        services.shutdown()
