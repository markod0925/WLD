from __future__ import annotations

import threading
import time

import pytest

from worklog_diary.core.llm_job_queue import LLMJobCancelledError, LLMJobQueue


def test_llm_job_queue_tracks_state_and_rejects_late_enqueue() -> None:
    queue = LLMJobQueue(max_concurrent_jobs=1)
    started = threading.Event()
    release = threading.Event()
    result: dict[str, object] = {}

    def operation() -> str:
        started.set()
        assert release.wait(timeout=5)
        return "ok"

    def submit_job() -> None:
        result["value"] = queue.submit(job_type="summary", operation=operation, priority=1)

    thread = threading.Thread(target=submit_job)
    thread.start()

    try:
        assert started.wait(timeout=5)
        deadline = time.time() + 5
        while time.time() < deadline:
            snapshot = queue.snapshot()
            if snapshot["running_jobs"] == 1:
                assert snapshot["queued_jobs"] == 0
                assert snapshot["pending_jobs"] == 1
                assert snapshot["accepting_jobs"] is True
                break
            time.sleep(0.02)
        else:
            raise AssertionError("queue never reported a running job")

        release.set()
        thread.join(timeout=5)
        assert thread.is_alive() is False
        assert result["value"] == "ok"

        snapshot = queue.snapshot()
        assert snapshot["pending_jobs"] == 0
        assert snapshot["closed"] is False
        assert snapshot["closing"] is False
    finally:
        queue.stop()

    snapshot = queue.snapshot()
    assert snapshot["accepting_jobs"] is False
    assert snapshot["closed"] is True
    assert snapshot["stopped"] is True

    with pytest.raises(LLMJobCancelledError):
        queue.submit(job_type="summary", operation=lambda: "late")

