from __future__ import annotations

import argparse
import signal
import sys
import time

from .app import create_services, run_desktop_app



def main() -> int:
    parser = argparse.ArgumentParser(description="WorkLog Diary")
    parser.add_argument("--config", dest="config_path", default=None, help="Path to config.json")
    parser.add_argument("--headless", action="store_true", help="Run without tray UI")
    args = parser.parse_args()

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


if __name__ == "__main__":
    sys.exit(main())
