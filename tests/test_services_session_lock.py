from __future__ import annotations

from pathlib import Path

from worklog_diary.core.config import AppConfig
from worklog_diary.core.keyboard_capture import KeyboardCaptureService
from worklog_diary.core.models import ForegroundInfo
from worklog_diary.core.privacy import PrivacyPolicyEngine
from worklog_diary.core.services import MonitoringServices


def _config_for_tmp(tmp_path: Path, **overrides: object) -> AppConfig:
    data = AppConfig().to_dict()
    data.update(
        {
            "app_data_dir": str(tmp_path / "app"),
            "screenshot_dir": str(tmp_path / "app" / "screenshots"),
            "db_path": str(tmp_path / "app" / "worklog.db"),
            "config_path": str(tmp_path / "app" / "config.json"),
            "start_monitoring_on_launch": False,
            "flush_interval_seconds": 9999,
        }
    )
    data.update(overrides)
    return AppConfig.from_dict(data)


def test_lock_then_unlock_transitions_back_to_monitoring(tmp_path: Path) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path))
    try:
        services.start_monitoring()
        assert services.get_status()["monitoring_state"] == "Monitoring"

        services.handle_session_locked()
        locked_status = services.get_status()
        assert locked_status["paused_by_lock"] is True
        assert locked_status["monitoring_active"] is False
        assert locked_status["monitoring_state"] == "Paused (PC locked)"

        services.handle_session_unlocked()
        resumed_status = services.get_status()
        assert resumed_status["paused_by_lock"] is False
        assert resumed_status["monitoring_active"] is True
        assert resumed_status["monitoring_state"] == "Monitoring"
    finally:
        services.shutdown()


def test_manual_pause_is_not_overridden_by_unlock(tmp_path: Path) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path))
    try:
        services.start_monitoring()
        services.pause_monitoring()
        assert services.get_status()["monitoring_state"] == "Paused"

        services.handle_session_locked()
        services.handle_session_unlocked()
        status = services.get_status()
        assert status["monitoring_active"] is False
        assert status["monitoring_state"] == "Paused"
    finally:
        services.shutdown()


def test_paused_by_lock_suppresses_key_capture_logic(tmp_path: Path) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path))
    try:
        services.start_monitoring()
        info = ForegroundInfo(
            timestamp=100.0,
            hwnd=88,
            pid=99,
            process_name="code.exe",
            window_title="Editor",
        )
        services.state.update_foreground(info, blocked=False, active_interval_id=123)

        capture = KeyboardCaptureService(
            storage=services.storage,
            state=services.state,
            privacy=PrivacyPolicyEngine(set()),
            foreground_provider=lambda: info,
        )

        capture._handle_event("a", "down")
        capture.flush_pending_events()
        baseline = services.storage.get_diagnostics_snapshot()["table_counts"]["key_events"]
        assert baseline == 1

        services.handle_session_locked()
        capture._handle_event("b", "down")
        capture.flush_pending_events()
        after_lock = services.storage.get_diagnostics_snapshot()["table_counts"]["key_events"]
        assert after_lock == baseline
    finally:
        services.shutdown()
