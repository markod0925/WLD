from __future__ import annotations

import threading
import time
from pathlib import Path

from worklog_diary.core.batching import BatchBuilder
from worklog_diary.core.models import TextSegment
from worklog_diary.core.storage import SQLiteStorage
from worklog_diary.core.summarizer import Summarizer


class SlowClient:
    def __init__(self) -> None:
        self.release = threading.Event()
        self._lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def summarize_batch(self, _batch: object) -> tuple[str, dict]:
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        self.release.wait(timeout=5)
        with self._lock:
            self.active -= 1
        return "ok", {"summary_text": "ok", "key_points": [], "blocked_activity": []}



def _seed_segments(storage: SQLiteStorage, count: int) -> None:
    segments = [
        TextSegment(
            id=None,
            start_ts=10.0 + i,
            end_ts=10.1 + i,
            process_name="code.exe",
            window_title="Editor",
            text=f"text-{i}",
            hotkeys=[],
            raw_key_count=1,
        )
        for i in range(count)
    ]
    storage.insert_text_segments(segments)



def _wait_for_running_jobs(summarizer: Summarizer, expected: int, timeout: float = 3.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if int(summarizer.get_runtime_status()["running_jobs"]) >= expected:
            return
        time.sleep(0.02)
    raise AssertionError("timed out waiting for expected running summary jobs")



def test_summary_dispatch_respects_max_parallel_jobs(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    _seed_segments(storage, count=4)

    client = SlowClient()
    summarizer = Summarizer(
        storage=storage,
        batch_builder=BatchBuilder(storage=storage, max_text_segments=1, max_screenshots=1),
        lm_client=client,
        max_parallel_jobs=2,
    )

    try:
        first_dispatch = summarizer.dispatch_pending_jobs(reason="test")
        assert first_dispatch == 2

        _wait_for_running_jobs(summarizer, expected=2)
        assert summarizer.dispatch_pending_jobs(reason="test") == 0

        client.release.set()
        assert summarizer.wait_for_idle(timeout_seconds=5.0)

        second_dispatch = summarizer.dispatch_pending_jobs(reason="test")
        assert second_dispatch == 2
        assert summarizer.wait_for_idle(timeout_seconds=5.0)

        counts = storage.get_summary_job_status_counts()
        assert counts["succeeded"] == 4
        assert counts["failed"] == 0
        assert client.max_active <= 2
    finally:
        summarizer.stop()
        storage.close()


def test_summary_worker_pool_shrinks_and_stops_cleanly(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    _seed_segments(storage, count=4)

    client = SlowClient()
    summarizer = Summarizer(
        storage=storage,
        batch_builder=BatchBuilder(storage=storage, max_text_segments=1, max_screenshots=1),
        lm_client=client,
        max_parallel_jobs=2,
    )

    try:
        assert summarizer.dispatch_pending_jobs(reason="test") == 2
        _wait_for_running_jobs(summarizer, expected=2)

        summarizer.update_max_parallel_jobs(1)
        assert summarizer.get_runtime_status()["max_parallel_summary_jobs"] == 1
        assert len(summarizer._workers) == 1

        client.release.set()
        assert summarizer.wait_for_idle(timeout_seconds=5.0)

        _seed_segments(storage, count=2)
        assert summarizer.dispatch_pending_jobs(reason="test") == 1
        assert summarizer.wait_for_idle(timeout_seconds=5.0)
    finally:
        summarizer.stop()
        assert not any(thread.name.startswith("SummaryWorker-") for thread in threading.enumerate())
        storage.close()
