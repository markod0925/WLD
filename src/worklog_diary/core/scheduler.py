from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

from .models import SharedState


class FlushScheduler:
    def __init__(
        self,
        interval_seconds: int,
        flush_callback: Callable[[str], object | None],
        state: SharedState,
    ) -> None:
        self.interval_seconds = max(30, interval_seconds)
        self.flush_callback = flush_callback
        self.state = state
        self.logger = logging.getLogger(__name__)

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="FlushScheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)

    def _run(self) -> None:
        next_run = time.time() + self.interval_seconds
        self.state.set_flush_times(last_flush_ts=self.state.snapshot().last_flush_ts, next_flush_ts=next_run)

        while not self._stop_event.is_set():
            now = time.time()
            self.state.set_flush_times(last_flush_ts=self.state.snapshot().last_flush_ts, next_flush_ts=next_run)

            if now >= next_run:
                try:
                    self.logger.info("event=summary_flush_triggered reason=scheduled")
                    self.flush_callback("scheduled")
                except Exception as exc:
                    self.logger.exception("Scheduled flush failed: %s", exc)
                next_run = time.time() + self.interval_seconds
                self.state.set_flush_times(last_flush_ts=self.state.snapshot().last_flush_ts, next_flush_ts=next_run)

            self._stop_event.wait(1)
