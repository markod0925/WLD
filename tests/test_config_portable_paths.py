from __future__ import annotations

import json
from pathlib import Path

import worklog_diary.core.config as config_module


def test_app_config_default_settings_match_ui_defaults() -> None:
    cfg = config_module.AppConfig()

    assert cfg.screenshot_interval_seconds == 60
    assert cfg.capture_mode == "active_window"
    assert cfg.max_text_segments_per_summary == 400
    assert cfg.request_timeout_seconds == 600
    assert cfg.daily_request_timeout_seconds is None
    assert cfg.lmstudio_max_prompt_chars == 20000
    assert cfg.screenshot_dedup_enabled is True
    assert cfg.screenshot_min_keep_interval_seconds == 120


def test_app_config_normalize_invalid_capture_mode_falls_back_to_active_window() -> None:
    cfg = config_module.AppConfig(capture_mode="totally-invalid")
    cfg.normalize()

    assert cfg.capture_mode == "active_window"


def test_load_config_in_frozen_mode_creates_portable_data_tree(tmp_path: Path, monkeypatch) -> None:
    exe_dir = tmp_path / "portable"
    exe_dir.mkdir()
    exe_path = exe_dir / "WLD.exe"

    monkeypatch.setattr(config_module.sys, "frozen", True, raising=False)
    monkeypatch.setattr(config_module.sys, "executable", str(exe_path), raising=False)
    monkeypatch.delenv("WORKLOG_DIARY_APP_DATA_DIR", raising=False)
    monkeypatch.delenv("WORKLOG_DIARY_CONFIG", raising=False)

    cfg = config_module.load_config()

    assert cfg.app_data_dir == str(exe_dir / "data")
    assert cfg.config_path == str(exe_dir / "data" / "config.json")
    assert cfg.db_path == str(exe_dir / "data" / "worklog_diary.db")
    assert cfg.screenshot_dir == str(exe_dir / "data" / "screenshots")
    assert cfg.log_dir == str(exe_dir / "data" / "logs")

    assert Path(cfg.config_path).exists()
    assert Path(cfg.db_path).parent.exists()
    assert Path(cfg.screenshot_dir).exists()
    assert Path(cfg.log_dir).exists()


def test_load_config_migrates_version_and_warns_about_unknown_fields(
    tmp_path: Path, caplog
) -> None:
    config_path = tmp_path / "config.json"
    payload = config_module.AppConfig().to_dict()
    payload.pop("config_version", None)
    payload["unexpected_field"] = "ignored"
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    caplog.clear()
    cfg = config_module.load_config(config_path)

    assert cfg.config_version == config_module.CONFIG_VERSION
    assert any("event=config_unknown_fields" in record.message for record in caplog.records)

    saved = json.loads(config_path.read_text(encoding="utf-8"))
    assert saved["config_version"] == config_module.CONFIG_VERSION
    assert "unexpected_field" not in saved


def test_load_config_rejects_future_config_version(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    payload = config_module.AppConfig().to_dict()
    payload["config_version"] = config_module.CONFIG_VERSION + 1
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    try:
        config_module.load_config(config_path)
    except ValueError as exc:
        assert "Unsupported config_version" in str(exc)
    else:
        raise AssertionError("load_config() should reject future config versions")


def test_legacy_dedup_threshold_maps_to_canonical_when_canonical_missing(tmp_path: Path) -> None:
    payload = config_module.AppConfig().to_dict()
    payload.pop("screenshot_dedup_phash_threshold", None)
    payload["screenshot_dedup_threshold"] = 13

    cfg = config_module.AppConfig.from_dict(payload, source=str(tmp_path / "config.json"))
    assert cfg.screenshot_dedup_phash_threshold == 13
    assert "screenshot_dedup_threshold" not in cfg.to_dict()


def test_legacy_dedup_threshold_conflict_prefers_canonical_and_logs_warning(tmp_path: Path, caplog) -> None:
    payload = config_module.AppConfig().to_dict()
    payload["screenshot_dedup_phash_threshold"] = 7
    payload["screenshot_dedup_threshold"] = 15

    caplog.clear()
    cfg = config_module.AppConfig.from_dict(payload, source=str(tmp_path / "config.json"))

    assert cfg.screenshot_dedup_phash_threshold == 7
    assert "screenshot_dedup_threshold" not in cfg.to_dict()
    assert any("event=config_legacy_field_conflict" in record.message for record in caplog.records)


def test_matching_legacy_dedup_threshold_does_not_log_conflict_or_force_save(caplog) -> None:
    normalized = config_module.AppConfig()
    normalized.normalize()
    payload = normalized.to_dict()
    payload["screenshot_dedup_phash_threshold"] = 9
    payload["screenshot_dedup_threshold"] = 9

    caplog.clear()
    _, needs_save = config_module._build_config_from_mapping(payload, source="in-memory")

    assert needs_save is False
    assert not any("event=config_legacy_field_conflict" in record.message for record in caplog.records)


def test_legacy_summary_concurrency_key_migrates_to_canonical(tmp_path: Path, caplog) -> None:
    payload = config_module.AppConfig().to_dict()
    payload.pop("max_concurrent_summary_llm_requests", None)
    payload["max_parallel_summary_jobs"] = 2

    caplog.clear()
    cfg = config_module.AppConfig.from_dict(payload, source=str(tmp_path / "config.json"))
    assert cfg.max_concurrent_summary_llm_requests == 2
    assert any("event=config_deprecated_field_migrated" in record.message for record in caplog.records)


def test_canonical_summary_concurrency_key_takes_precedence_over_legacy(tmp_path: Path, caplog) -> None:
    payload = config_module.AppConfig().to_dict()
    payload["max_concurrent_summary_llm_requests"] = 3
    payload["max_parallel_summary_jobs"] = 2

    caplog.clear()
    cfg = config_module.AppConfig.from_dict(payload, source=str(tmp_path / "config.json"))
    assert cfg.max_concurrent_summary_llm_requests == 3
    assert any("event=config_legacy_field_conflict" in record.message for record in caplog.records)


def test_save_config_writes_only_canonical_summary_concurrency_key(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    payload = config_module.AppConfig().to_dict()
    payload.pop("max_concurrent_summary_llm_requests", None)
    payload["max_parallel_summary_jobs"] = 4
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    loaded = config_module.load_config(config_path)
    assert loaded.max_concurrent_summary_llm_requests == 4

    saved = json.loads(config_path.read_text(encoding="utf-8"))
    assert saved["max_concurrent_summary_llm_requests"] == 4
    assert "max_parallel_summary_jobs" not in saved
