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


def test_apply_config_emits_diff_logs(tmp_path: Path, caplog) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path))
    try:
        caplog.set_level("INFO")
        updated = _config_for_tmp(tmp_path, process_backlog_only_while_locked=False)
        services.apply_config(updated)
        assert any("event=config_apply_start" in rec.message for rec in caplog.records)
        assert any("event=config_apply_diff key=process_backlog_only_while_locked" in rec.message for rec in caplog.records)
        assert any("event=config_apply_complete changed_count=" in rec.message for rec in caplog.records)
    finally:
        services.shutdown()


def test_apply_config_noop_reports_zero_changes(tmp_path: Path, caplog) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path))
    try:
        caplog.set_level("INFO")
        services.apply_config(_config_for_tmp(tmp_path))
        assert any("event=config_apply_complete changed_count=0" in rec.message for rec in caplog.records)
        assert not any("event=config_apply_diff" in rec.message for rec in caplog.records)
    finally:
        services.shutdown()


def test_apply_config_updates_screenshot_dedup_through_public_method(tmp_path: Path) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path))
    try:
        captured: list[dict[str, object]] = []
        initial_resize_width = services.screenshot_capture._dedup_resize_width
        initial_phash_threshold = services.screenshot_capture._dedup_state.phash_threshold

        def record_update(**kwargs: object) -> None:
            captured.append(kwargs)

        services.screenshot_capture.update_dedup_config = record_update  # type: ignore[method-assign]

        updated = _config_for_tmp(
            tmp_path,
            screenshot_dedup_exact_hash_enabled=False,
            screenshot_dedup_perceptual_hash_enabled=False,
            screenshot_dedup_phash_threshold=11,
            screenshot_dedup_ssim_enabled=False,
            screenshot_dedup_ssim_threshold=0.91,
            screenshot_dedup_resize_width=48,
            screenshot_dedup_compare_recent_count=4,
            screenshot_min_keep_interval_seconds=45,
        )
        services.apply_config(updated)

        assert len(captured) == 1
        assert captured[0]["phash_threshold"] == 11
        assert captured[0]["resize_width"] == 48
        assert captured[0]["compare_recent_count"] == 4
        assert services.screenshot_capture._dedup_resize_width == initial_resize_width
        assert services.screenshot_capture._dedup_state.phash_threshold == initial_phash_threshold
    finally:
        services.shutdown()


def test_apply_config_delegates_to_reconfigure_methods(tmp_path: Path) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path))
    try:
        calls: dict[str, int] = {"batch": 0, "client": 0, "prompt": 0, "summarizer": 0}
        services.batch_builder.reconfigure = lambda **_kwargs: calls.__setitem__("batch", calls["batch"] + 1)  # type: ignore[method-assign]
        services.lmstudio_client.reconfigure = lambda **_kwargs: calls.__setitem__("client", calls["client"] + 1)  # type: ignore[method-assign]
        services.lmstudio_client.prompt_builder.update_limits = lambda **_kwargs: calls.__setitem__("prompt", calls["prompt"] + 1)  # type: ignore[method-assign]
        services.summarizer.reconfigure = lambda **_kwargs: calls.__setitem__("summarizer", calls["summarizer"] + 1)  # type: ignore[method-assign]
        services.apply_config(_config_for_tmp(tmp_path, lmstudio_max_prompt_chars=25000))
        assert calls == {"batch": 1, "client": 1, "prompt": 1, "summarizer": 1}
    finally:
        services.shutdown()


def test_apply_config_updates_prompt_builder_derived_text_limit(tmp_path: Path) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path, max_text_segments_per_summary=3))
    try:
        services.apply_config(_config_for_tmp(tmp_path, max_text_segments_per_summary=7))
        builder = services.lmstudio_client.prompt_builder
        assert builder.max_summary_text_segments == 7
        assert builder.max_text_chars == 35
    finally:
        services.shutdown()


def test_summary_admission_state_logs_transition_without_spam(tmp_path: Path, caplog) -> None:
    services = MonitoringServices(_config_for_tmp(tmp_path, process_backlog_only_while_locked=True))
    try:
        caplog.set_level("INFO")
        services.handle_session_unlocked()
        services.handle_session_unlocked()
        services.handle_session_locked()
        state_logs = [rec.message for rec in caplog.records if "event=summary_admission_state" in rec.message]
        assert any("state=allowed" in line for line in state_logs)
        assert any("state=blocked" in line for line in state_logs)
        assert len([line for line in state_logs if "state=blocked" in line]) == 1
    finally:
        services.shutdown()
