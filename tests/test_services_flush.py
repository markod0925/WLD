from __future__ import annotations

from pathlib import Path

from worklog_diary.core.config import AppConfig
from worklog_diary.core.models import ForegroundInfo, KeyEvent, ScreenshotRecord, TextSegment
from worklog_diary.core.monitoring_components import FlushCoordinator
from worklog_diary.core.services import MonitoringServices


class SuccessfulClient:
    def summarize_batch(self, *_args: object, **_kwargs: object) -> tuple[str, dict]:
        return "ok", {"summary_text": "ok", "key_points": [], "blocked_activity": []}


class _FakeSummarizer:
    def __init__(self, wait_values: list[float], running_values: list[int]) -> None:
        self.wait_values = wait_values
        self.running_values = running_values
        self._idx = 0

    def clear_unrecoverable_error(self) -> None: ...
    def cancel_queued_jobs(self, reason: str = "") -> int: return 0
    def dispatch_pending_jobs(self, reason: str = "") -> int: return 0
    def wait_for_idle(self, timeout_seconds: float) -> None: ...
    def get_runtime_status(self) -> dict[str, int | bool]:
        idx = min(self._idx, len(self.running_values) - 1)
        pending_jobs = 0 if idx >= 3 else 1
        return {"queued_jobs": 0, "running_jobs": self.running_values[idx], "pending_summary_jobs": pending_jobs, "has_unrecoverable_error": False, "summary_admission_paused": False, "max_concurrent_summary_llm_requests": 1}
    def wait_for_activity(self, timeout_seconds: float) -> None:
        self.wait_values.append(timeout_seconds)
        self._idx += 1



def _config_for_tmp(tmp_path: Path, **overrides: object) -> AppConfig:
    data = AppConfig().to_dict()
    data.update(
        {
            "app_data_dir": str(tmp_path / "app"),
            "screenshot_dir": str(tmp_path / "app" / "screenshots"),
            "db_path": str(tmp_path / "app" / "worklog.db"),
            "config_path": str(tmp_path / "app" / "config.json"),
            "max_text_segments_per_summary": 1,
            "max_screenshots_per_summary": 1,
            "max_concurrent_summary_llm_requests": 2,
        }
    )
    data.update(overrides)
    return AppConfig.from_dict(data)


def test_daily_request_timeout_explicit_value_is_honored(tmp_path: Path) -> None:
    services = MonitoringServices(
        _config_for_tmp(
            tmp_path,
            request_timeout_seconds=300,
            daily_request_timeout_seconds=900,
        )
    )
    try:
        assert services.lmstudio_client.daily_timeout_seconds == 900
    finally:
        services.shutdown()


def test_daily_request_timeout_fallback_uses_double_request_timeout_when_above_minimum(tmp_path: Path) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path, request_timeout_seconds=400))
    try:
        assert services.lmstudio_client.daily_timeout_seconds == 800
    finally:
        services.shutdown()


def test_daily_request_timeout_fallback_honors_minimum(tmp_path: Path) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path, request_timeout_seconds=200))
    try:
        assert services.lmstudio_client.daily_timeout_seconds == 600
    finally:
        services.shutdown()



def test_flush_now_returns_none_when_drain_already_running(tmp_path: Path) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path))
    assert services.flush_coordinator._flush_lock.acquire(blocking=False)
    try:
        assert services.flush_now(reason="manual") is None
    finally:
        services.flush_coordinator._flush_lock.release()
        services.shutdown()



def test_flush_now_drains_until_buffer_is_empty(tmp_path: Path) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path))
    services.summarizer.lm_client = SuccessfulClient()

    try:
        services.storage.insert_text_segments(
            [
                TextSegment(
                    id=None,
                    start_ts=1.0,
                    end_ts=1.1,
                    process_name="code.exe",
                    window_title="Editor",
                    text="a",
                    hotkeys=[],
                    raw_key_count=1,
                ),
                TextSegment(
                    id=None,
                    start_ts=2.0,
                    end_ts=2.1,
                    process_name="code.exe",
                    window_title="Editor",
                    text="b",
                    hotkeys=[],
                    raw_key_count=1,
                ),
                TextSegment(
                    id=None,
                    start_ts=3.0,
                    end_ts=3.1,
                    process_name="code.exe",
                    window_title="Editor",
                    text="c",
                    hotkeys=[],
                    raw_key_count=1,
                ),
            ]
        )

        result = services.flush_now(reason="test-drain")
        assert result is not None
        assert result.stop_reason == "empty"
        assert result.summaries_created == 3

        pending = services.storage.get_pending_counts()
        assert pending["text_segments"] == 0

        jobs = services.storage.get_summary_job_status_counts()
        assert jobs["succeeded"] == 3
        assert jobs["failed"] == 0
    finally:
        services.shutdown()


def test_flush_now_runs_full_pipeline_on_real_sqlite(tmp_path: Path) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path))
    services.summarizer.lm_client = SuccessfulClient()
    shot_path = Path(services.config.screenshot_dir) / "shot.png"
    shot_path.parent.mkdir(parents=True, exist_ok=True)
    shot_path.write_bytes(b"fake-image")

    try:
        info = ForegroundInfo(
            timestamp=10.0,
            hwnd=11,
            pid=12,
            process_name="code.exe",
            window_title="Editor",
        )
        interval_id = services.storage.start_interval(info, blocked=False)
        services.state.update_foreground(info, blocked=False, active_interval_id=interval_id)
        services.storage.insert_key_event(
            KeyEvent(
                id=None,
                ts=11.0,
                key="a",
                event_type="down",
                modifiers=[],
                process_name="code.exe",
                window_title="Editor",
                hwnd=11,
                active_interval_id=interval_id,
                processed=False,
            )
        )
        services.storage.insert_screenshot(
            ScreenshotRecord(
                id=None,
                ts=12.0,
                file_path=str(shot_path),
                process_name="code.exe",
                window_title="Editor",
                active_interval_id=interval_id,
            )
        )
        services.storage.close_interval(interval_id, end_ts=13.0)

        result = services.flush_now(reason="integration")

        assert result is not None
        assert result.stop_reason == "empty"
        assert result.summaries_created >= 1
        assert services.storage.get_pending_counts()["text_segments"] == 0
        assert services.storage.get_pending_counts()["screenshots"] == 0
        assert services.storage.get_pending_counts()["intervals"] == 0
        assert services.storage.get_summary_job_status_counts()["succeeded"] == result.summaries_created
        assert not shot_path.exists()
    finally:
        services.shutdown()


def test_flush_now_releases_lock_after_paused_by_lock_skip(tmp_path: Path) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path))
    services.summarizer.lm_client = SuccessfulClient()
    try:
        services.handle_session_locked()
        assert services.flush_now(reason="manual") is None

        services.handle_session_unlocked()
        services.storage.insert_text_segments(
            [
                TextSegment(
                    id=None,
                    start_ts=1.0,
                    end_ts=1.1,
                    process_name="code.exe",
                    window_title="Editor",
                    text="post-skip",
                    hotkeys=[],
                    raw_key_count=1,
                )
            ]
        )
        result = services.flush_now(reason="manual")
        assert result is not None
        assert result.summaries_created == 1
    finally:
        services.shutdown()


def test_manual_flush_bypasses_lock_gate_when_unlocked(tmp_path: Path) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path, process_backlog_only_while_locked=True))
    services.summarizer.lm_client = SuccessfulClient()
    try:
        services.handle_session_unlocked()
        services.storage.insert_text_segments(
            [
                TextSegment(
                    id=None,
                    start_ts=1.0,
                    end_ts=1.1,
                    process_name="code.exe",
                    window_title="Editor",
                    text="manual",
                    hotkeys=[],
                    raw_key_count=1,
                )
            ]
        )
        result = services.flush_now(reason="manual")
        assert result is not None
        assert result.summaries_created == 1
    finally:
        services.shutdown()


def test_scheduled_flush_stops_when_admission_paused(tmp_path: Path) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path, process_backlog_only_while_locked=True))
    services.summarizer.lm_client = SuccessfulClient()
    try:
        services.handle_session_unlocked()
        services.storage.insert_text_segments(
            [
                TextSegment(
                    id=None,
                    start_ts=1.0,
                    end_ts=1.1,
                    process_name="code.exe",
                    window_title="Editor",
                    text="scheduled",
                    hotkeys=[],
                    raw_key_count=1,
                )
            ]
        )

        result = services.flush_now(reason="scheduled")
        assert result is not None
        assert result.stop_reason == "paused"
        assert services.storage.get_pending_counts()["text_segments"] == 1
    finally:
        services.shutdown()


def test_flush_coordinator_adaptive_wait_increases_and_resets() -> None:
    class _FakeStorage:
        def __init__(self) -> None:
            self.calls = 0
        def get_summary_job_status_counts(self): return {}
        def count_unprocessed_key_events(self): return 0
        def get_pending_counts(self):
            self.calls += 1
            if self.calls == 4:
                return {"text_segments": 0, "screenshots": 0, "intervals": 0, "key_events": 0, "processed_key_events": 0}
            return {"text_segments": 1, "screenshots": 0, "intervals": 0, "key_events": 0, "processed_key_events": 0}

    class _FakeState:
        def set_flush_times(self, **_kwargs): ...

    class _FakeLifecycle:
        paused_by_lock = False
        def set_draining(self): ...
        def set_idle(self): ...

    class _FakeKeyboard:
        def flush_pending_events(self, reason: str): ...
    class _FakeTextService:
        def process_once(self, force_flush: bool = False): ...

    class _FakeNotifier:
        def resolve(self, _key: str): ...
        def notify(self, *_args, **_kwargs): ...

    wait_values: list[float] = []
    summarizer = _FakeSummarizer(wait_values=wait_values, running_values=[0, 0, 1, 0])
    services = type("S", (), {"shutdown_event": type("E", (), {"is_set": lambda self: False})(), "summarizer": summarizer, "storage": _FakeStorage(), "state": _FakeState(), "keyboard_capture": _FakeKeyboard(), "text_service": _FakeTextService(), "error_notifier": _FakeNotifier()})()
    coordinator = FlushCoordinator(services, _FakeLifecycle(), 60, __import__("logging").getLogger(__name__))
    result = coordinator.flush_now("manual")
    assert result is not None
    assert wait_values[:2] == [0.8, 1.6]
    assert wait_values[2] == 0.4


def test_late_scheduler_callback_after_shutdown_is_ignored(tmp_path: Path, caplog) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path))
    try:
        caplog.set_level("INFO")
        services.shutdown()
        assert services.flush_now(reason="scheduled") is None
        assert any("event=summary_flush_skipped reason=shutdown_in_progress" in rec.message for rec in caplog.records)
    finally:
        pass
