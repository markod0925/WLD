from __future__ import annotations

from pathlib import Path

from .core.config import load_config
from .core.crash_reporting import run_protected
from .core.services import MonitoringServices



def create_services(config_path: str | None = None) -> MonitoringServices:
    config = load_config(Path(config_path) if config_path else None)
    return MonitoringServices(config)



def run_desktop_app(config_path: str | None = None) -> int:
    from .ui.tray import run_tray_app

    services = create_services(config_path)

    def _run() -> int:
        return run_tray_app(services)

    try:
        return run_protected("desktop_tray_loop", services.logger, _run)
    finally:
        services.shutdown()
