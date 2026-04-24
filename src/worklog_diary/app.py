from __future__ import annotations

from pathlib import Path

from .core.config import load_config
from .core.crash_reporting import run_protected
from .core.services import MonitoringServices
from .core.startup_errors import ENCRYPTED_DATABASE_STARTUP_ERRORS


def create_services(config_path: str | None = None) -> MonitoringServices:
    config = load_config(Path(config_path) if config_path else None)
    return MonitoringServices(config)



def run_desktop_app(config_path: str | None = None) -> int:
    from .ui.startup_errors import show_encrypted_database_startup_error

    try:
        services = create_services(config_path)
    except ENCRYPTED_DATABASE_STARTUP_ERRORS as exc:
        show_encrypted_database_startup_error(exc)
        return 1

    from .ui.tray import run_tray_app

    def _run() -> int:
        return run_tray_app(services)

    try:
        return run_protected("desktop_tray_loop", services.logger, _run)
    finally:
        services.shutdown()
