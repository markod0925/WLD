from __future__ import annotations

import argparse
import logging
import signal
import sys
import time

from .app import create_services, run_desktop_app
from .core.crash_reporting import run_protected



def main() -> int:
    parser = argparse.ArgumentParser(description="WorkLog Diary")
    parser.add_argument("--config", dest="config_path", default=None, help="Path to config.json")
    parser.add_argument("--headless", action="store_true", help="Run without tray UI")
    args = parser.parse_args()

    logger = logging.getLogger(__name__)

    def _run() -> int:
        if not args.headless:
            return run_desktop_app(config_path=args.config_path)

        services = create_services(config_path=args.config_path)
        services.start_monitoring()

        stop_requested = False

        def _handle_signal(_signum: int, _frame: object) -> None:
            nonlocal stop_requested
            stop_requested = True

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        try:
            while not stop_requested:
                time.sleep(0.5)
        finally:
            services.shutdown()

        return 0

    return run_protected("app_main_loop", logger, _run)


if __name__ == "__main__":
    sys.exit(main())
