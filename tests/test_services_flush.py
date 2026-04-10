from __future__ import annotations

import logging
import threading
import time
from types import SimpleNamespace

from worklog_diary.core.services import MonitoringServices


class DummyTextService:
    def process_once(self, force_flush: bool = False) -> int:
        return 0


class DummyStorage:
    def count_unprocessed_key_events(self) -> int:
        return 0


class DummyState:
    def __init__(self) -> None:
        self.last_flush_ts: float | None = None
        self.next_flush_ts: float | None = None

    def set_flush_times(self, last_flush_ts: float | None, next_flush_ts: float | None) -> None:
        self.last_flush_ts = last_flush_ts
        self.next_flush_ts = next_flush_ts


class SlowSummarizer:
    def __init__(self, started: threading.Event, calls: list[str]) -> None:
        self.started = started
        self.calls = calls

    def flush_pending(self, reason: str = "manual") -> int | None:
        self.calls.append(reason)
        self.started.set()
        time.sleep(0.15)
        return 99


def test_flush_now_is_effectively_idempotent_when_called_concurrently() -> None:
    started = threading.Event()
    calls: list[str] = []

    services = MonitoringServices.__new__(MonitoringServices)
    services._flush_lock = threading.Lock()
    services.text_service = DummyTextService()
    services.storage = DummyStorage()
    services.summarizer = SlowSummarizer(started=started, calls=calls)
    services.state = DummyState()
    services.config = SimpleNamespace(flush_interval_seconds=300)
    services.logger = logging.getLogger("test.services")

    results: list[int | None] = []

    def _run(reason: str) -> None:
        results.append(services.flush_now(reason=reason))

    first = threading.Thread(target=_run, args=("scheduled",))
    second = threading.Thread(target=_run, args=("manual",))
    first.start()
    assert started.wait(timeout=1)
    second.start()
    first.join(timeout=2)
    second.join(timeout=2)

    assert len(calls) == 1
    assert sorted(results, key=lambda item: (item is None, item)) == [99, None]

