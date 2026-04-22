from __future__ import annotations

from pathlib import Path

from worklog_diary.core.config import AppConfig
from worklog_diary.core.models import ForegroundInfo, KeyEvent, ScreenshotRecord, TextSegment
from worklog_diary.core.services import MonitoringServices


class SuccessfulClient:
    def summarize_batch(self, *_args: object, **_kwargs: object) -> tuple[str, dict]:
        return "ok", {"summary_text": "ok", "key_points": [], "blocked_activity": []}



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
            "max_parallel_summary_jobs": 2,
        }
    )
    data.update(overrides)
    return AppConfig.from_dict(data)



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
