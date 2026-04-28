from __future__ import annotations

import threading
import time
from collections.abc import Callable

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


def test_llm_job_queue_respects_serial_and_parallel_limits() -> None:
    serial_queue = LLMJobQueue(max_concurrent_jobs=1)
    serial_first_started = threading.Event()
    serial_second_started = threading.Event()
    serial_release = threading.Event()
    serial_results: list[str] = []

    def _serial_job(started: threading.Event, value: str) -> str:
        started.set()
        serial_release.wait()
        return value

    def _run_serial(started: threading.Event, value: str) -> None:
        serial_results.append(serial_queue.submit(job_type="summary", operation=lambda: _serial_job(started, value)))

    serial_threads = [
        threading.Thread(target=_run_serial, args=(serial_first_started, "one")),
        threading.Thread(target=_run_serial, args=(serial_second_started, "two")),
    ]
    for thread in serial_threads:
        thread.start()
    try:
        assert serial_first_started.wait(timeout=5)
        assert not serial_second_started.wait(timeout=0.3)
        assert serial_queue.snapshot()["running_jobs"] == 1
        serial_release.set()
        for thread in serial_threads:
            thread.join(timeout=5)
            assert not thread.is_alive()
        assert sorted(serial_results) == ["one", "two"]
    finally:
        serial_queue.stop()

    parallel_queue = LLMJobQueue(max_concurrent_jobs=2)
    started = [threading.Event(), threading.Event(), threading.Event()]
    release = threading.Event()
    active_lock = threading.Lock()
    active_jobs = 0
    peak_jobs = 0

    def _parallel_job(index: int) -> str:
        nonlocal active_jobs, peak_jobs
        with active_lock:
            active_jobs += 1
            peak_jobs = max(peak_jobs, active_jobs)
        started[index].set()
        release.wait()
        with active_lock:
            active_jobs -= 1
        return f"job-{index}"

    parallel_threads = [
        threading.Thread(target=lambda i=i: parallel_queue.submit(job_type="summary", operation=lambda: _parallel_job(i)))
        for i in range(3)
    ]
    for thread in parallel_threads:
        thread.start()
    try:
        assert started[0].wait(timeout=5)
        assert started[1].wait(timeout=5)
        assert not started[2].wait(timeout=0.3)
        snapshot = parallel_queue.snapshot()
        assert snapshot["running_jobs"] == 2
        assert snapshot["queued_jobs"] == 1
        assert peak_jobs == 2
        release.set()
        for thread in parallel_threads:
            thread.join(timeout=5)
            assert not thread.is_alive()
    finally:
        parallel_queue.stop()


def test_llm_job_queue_shutdown_cancels_queued_jobs_without_starting_new_ones() -> None:
    queue = LLMJobQueue(max_concurrent_jobs=1)
    started = threading.Event()
    release = threading.Event()
    outcomes: dict[str, str] = {}

    def _blocking_job() -> str:
        started.set()
        assert release.wait(timeout=5)
        return "finished"

    def _submit(name: str, operation: Callable[[], str]) -> None:
        try:
            queue.submit(job_type="summary", operation=operation)
            outcomes[name] = "completed"
        except LLMJobCancelledError:
            outcomes[name] = "cancelled"

    first = threading.Thread(target=lambda: _submit("first", _blocking_job))
    second = threading.Thread(target=lambda: _submit("second", lambda: "second"))
    third = threading.Thread(target=lambda: _submit("third", lambda: "third"))

    first.start()
    second.start()
    third.start()
    try:
        assert started.wait(timeout=5)
        snapshot = queue.snapshot()
        assert snapshot["running_jobs"] == 1
        assert snapshot["queued_jobs"] == 2

        cancelled = queue.stop(reason="shutdown")
        assert cancelled == 2

        release.set()
        first.join(timeout=5)
        second.join(timeout=5)
        third.join(timeout=5)
        assert outcomes["first"] == "completed"
        assert outcomes["second"] == "cancelled"
        assert outcomes["third"] == "cancelled"
        assert queue.snapshot()["closed"] is True
    finally:
        release.set()


def test_llm_job_queue_runtime_downgrade_grandfathers_running_jobs_and_delays_new_starts() -> None:
    queue = LLMJobQueue(max_concurrent_jobs=2)
    start_events = [threading.Event(), threading.Event(), threading.Event()]
    release_events = [threading.Event(), threading.Event(), threading.Event()]
    active_lock = threading.Lock()
    active_jobs = 0
    peak_jobs = 0
    results: dict[int, str] = {}
    started_order: list[int] = []
    order_lock = threading.Lock()

    def _operation(index: int) -> str:
        nonlocal active_jobs, peak_jobs
        with active_lock:
            active_jobs += 1
            peak_jobs = max(peak_jobs, active_jobs)
        with order_lock:
            started_order.append(index)
        start_events[index].set()
        release_events[index].wait()
        with active_lock:
            active_jobs -= 1
        return f"job-{index}"

    def _submit(index: int) -> None:
        results[index] = queue.submit(job_type="summary", operation=lambda: _operation(index))

    threads = [threading.Thread(target=lambda i=i: _submit(i)) for i in range(3)]
    for thread in threads:
        thread.start()
    try:
        deadline = time.time() + 5
        while time.time() < deadline:
            with order_lock:
                if len(started_order) >= 2:
                    break
            time.sleep(0.02)
        else:
            raise AssertionError("did not start two in-flight jobs")
        with order_lock:
            first_running, second_running = started_order[0], started_order[1]
            queued_index = ({0, 1, 2} - {first_running, second_running}).pop()
        assert not start_events[queued_index].wait(timeout=0.3)

        snapshot = queue.snapshot()
        assert snapshot["max_concurrent_jobs"] == 2
        assert snapshot["running_jobs"] == 2
        assert snapshot["queued_jobs"] == 1

        queue.set_max_concurrent_jobs(1)
        snapshot = queue.snapshot()
        assert snapshot["max_concurrent_jobs"] == 1
        assert snapshot["running_jobs"] == 2
        assert snapshot["queued_jobs"] == 1

        release_events[first_running].set()
        assert not start_events[queued_index].wait(timeout=0.3)
        snapshot = queue.snapshot()
        assert snapshot["max_concurrent_jobs"] == 1
        assert snapshot["running_jobs"] == 1
        assert snapshot["queued_jobs"] == 1

        release_events[second_running].set()
        assert start_events[queued_index].wait(timeout=5)
        snapshot = queue.snapshot()
        assert snapshot["max_concurrent_jobs"] == 1
        assert snapshot["running_jobs"] == 1
        assert snapshot["queued_jobs"] == 0

        release_events[queued_index].set()
        for thread in threads:
            thread.join(timeout=5)
            assert not thread.is_alive()

        assert sorted(results.values()) == ["job-0", "job-1", "job-2"]
        assert peak_jobs == 2
        assert queue.snapshot()["pending_jobs"] == 0
    finally:
        for event in release_events:
            event.set()
        queue.stop()


def test_llm_job_queue_runtime_upgrade_allows_additional_inflight_work() -> None:
    queue = LLMJobQueue(max_concurrent_jobs=1)
    start_events = [threading.Event(), threading.Event(), threading.Event()]
    release_events = [threading.Event(), threading.Event(), threading.Event()]

    started_order: list[int] = []
    order_lock = threading.Lock()

    def _operation(index: int) -> str:
        with order_lock:
            started_order.append(index)
        start_events[index].set()
        release_events[index].wait()
        return f"job-{index}"

    threads = [
        threading.Thread(target=lambda i=i: queue.submit(job_type="summary", operation=lambda: _operation(i)))
        for i in range(3)
    ]
    for thread in threads:
        thread.start()
    try:
        deadline = time.time() + 5
        while time.time() < deadline:
            with order_lock:
                if len(started_order) >= 1:
                    first_running = started_order[0]
                    break
            time.sleep(0.02)
        else:
            raise AssertionError("did not start initial running job")
        remaining = [idx for idx in (0, 1, 2) if idx != first_running]
        assert not start_events[remaining[0]].wait(timeout=0.3)
        assert not start_events[remaining[1]].wait(timeout=0.3)
        snapshot = queue.snapshot()
        assert snapshot["max_concurrent_jobs"] == 1
        assert snapshot["running_jobs"] == 1
        assert snapshot["queued_jobs"] == 2

        queue.set_max_concurrent_jobs(2)
        deadline = time.time() + 5
        second_running: int | None = None
        while time.time() < deadline:
            with order_lock:
                if len(started_order) >= 2:
                    second_running = started_order[1]
                    break
            time.sleep(0.02)
        assert second_running is not None
        assert second_running != first_running
        third_index = ({0, 1, 2} - {first_running, second_running}).pop()
        snapshot = queue.snapshot()
        assert snapshot["max_concurrent_jobs"] == 2
        assert snapshot["running_jobs"] == 2
        assert snapshot["queued_jobs"] == 1
        assert not start_events[third_index].wait(timeout=0.3)

        release_events[first_running].set()
        assert start_events[third_index].wait(timeout=5)
        snapshot = queue.snapshot()
        assert snapshot["max_concurrent_jobs"] == 2
        assert snapshot["running_jobs"] in {1, 2}
        assert snapshot["queued_jobs"] == 0

        release_events[second_running].set()
        release_events[third_index].set()
        for thread in threads:
            thread.join(timeout=5)
            assert not thread.is_alive()
        assert queue.snapshot()["pending_jobs"] == 0
    finally:
        for event in release_events:
            event.set()
        queue.stop()
