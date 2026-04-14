from __future__ import annotations

import logging
import json
import os
import sys
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from collections.abc import Mapping
from typing import Any

DEFAULT_BLOCKED_PROCESSES = ["chrome.exe", "msedge.exe", "webex.exe", "lm studio.exe"]
SUPPORTED_CAPTURE_MODES = {"full_screen", "active_window"}
CONFIG_VERSION = 2

_LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class AppConfig:
    config_version: int = CONFIG_VERSION
    blocked_processes: list[str] = field(default_factory=lambda: DEFAULT_BLOCKED_PROCESSES.copy())
    screenshot_interval_seconds: int = 60
    capture_mode: str = "active_window"
    foreground_poll_interval_seconds: float = 1.0
    text_inactivity_gap_seconds: float = 8.0
    reconstruction_poll_interval_seconds: float = 2.0
    flush_interval_seconds: int = 300
    lmstudio_base_url: str = "http://127.0.0.1:1234/v1"
    lmstudio_model: str = "local-model"
    app_data_dir: str = ""
    screenshot_dir: str = ""
    log_dir: str = ""
    db_path: str = ""
    config_path: str = ""
    start_monitoring_on_launch: bool = False
    max_screenshots_per_summary: int = 3
    max_text_segments_per_summary: int = 400
    max_parallel_summary_jobs: int = 2
    request_timeout_seconds: int = 600

    def normalize(self) -> None:
        if not self.app_data_dir:
            self.app_data_dir = str(default_app_data_dir())
        app_data_path = Path(self.app_data_dir)

        if not self.screenshot_dir:
            self.screenshot_dir = str(app_data_path / "screenshots")
        if not self.log_dir:
            self.log_dir = str(app_data_path / "logs")
        if not self.db_path:
            self.db_path = str(app_data_path / "worklog_diary.db")
        if not self.config_path:
            self.config_path = str(default_config_path())

        self.blocked_processes = [p.strip().lower() for p in self.blocked_processes if p.strip()]

        mode = self.capture_mode.strip().lower()
        self.capture_mode = mode if mode in SUPPORTED_CAPTURE_MODES else "active_window"

        self.max_parallel_summary_jobs = max(1, int(self.max_parallel_summary_jobs))
        self.config_version = CONFIG_VERSION

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], *, source: str | None = None) -> AppConfig:
        config, _ = _build_config_from_mapping(data, source=source)
        return config

    def ensure_paths(self) -> None:
        self.normalize()
        Path(self.app_data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.screenshot_dir).mkdir(parents=True, exist_ok=True)
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> dict:
        return asdict(self)


def _config_field_names() -> set[str]:
    return {field.name for field in fields(AppConfig)}


def _coerce_int(value: Any, field_name: str, *, source: str | None = None) -> int:
    if isinstance(value, bool):
        raise ValueError(_format_config_error(source, field_name, "expected an integer", value))
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError as exc:
            raise ValueError(_format_config_error(source, field_name, "expected an integer", value)) from exc
    raise ValueError(_format_config_error(source, field_name, "expected an integer", value))


def _coerce_float(value: Any, field_name: str, *, source: str | None = None) -> float:
    if isinstance(value, bool):
        raise ValueError(_format_config_error(source, field_name, "expected a number", value))
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError as exc:
            raise ValueError(_format_config_error(source, field_name, "expected a number", value)) from exc
    raise ValueError(_format_config_error(source, field_name, "expected a number", value))


def _coerce_str(value: Any, field_name: str, *, source: str | None = None) -> str:
    if isinstance(value, str):
        return value
    raise ValueError(_format_config_error(source, field_name, "expected a string", value))


def _coerce_bool(value: Any, field_name: str, *, source: str | None = None) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return True
        if lowered in {"false", "0", "no", "off"}:
            return False
    raise ValueError(_format_config_error(source, field_name, "expected a boolean", value))


def _coerce_blocked_processes(value: Any, field_name: str, *, source: str | None = None) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(_format_config_error(source, field_name, "expected a list of strings", value))
    coerced: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError(_format_config_error(source, field_name, "expected a list of strings", value))
        stripped = item.strip()
        if stripped:
            coerced.append(stripped)
    return coerced


def _format_config_error(source: str | None, field_name: str, expected: str, value: Any) -> str:
    location = f" in {source}" if source else ""
    return f"Invalid config value{location} for '{field_name}': {expected}, got {value!r}"


def _build_config_from_mapping(data: Mapping[str, Any], *, source: str | None = None) -> tuple[AppConfig, bool]:
    if not isinstance(data, Mapping):
        raise ValueError(f"Config{f' in {source}' if source else ''} must be a JSON object.")

    field_names = _config_field_names()
    defaults = AppConfig().to_dict()
    values = defaults.copy()
    needs_save = False

    unknown_fields = sorted(name for name in data.keys() if name not in field_names)
    if unknown_fields:
        needs_save = True
        _LOGGER.warning(
            "event=config_unknown_fields source=%s fields=%s",
            source or "config",
            ",".join(unknown_fields),
        )

    raw_version = data.get("config_version", 1)
    version = _coerce_int(raw_version, "config_version", source=source)
    if version > CONFIG_VERSION:
        raise ValueError(
            f"Unsupported config_version {version}{f' in {source}' if source else ''}; "
            f"this build supports up to {CONFIG_VERSION}."
        )
    if version != CONFIG_VERSION:
        needs_save = True

    converters: dict[str, Any] = {
        "blocked_processes": _coerce_blocked_processes,
        "screenshot_interval_seconds": _coerce_int,
        "capture_mode": _coerce_str,
        "foreground_poll_interval_seconds": _coerce_float,
        "text_inactivity_gap_seconds": _coerce_float,
        "reconstruction_poll_interval_seconds": _coerce_float,
        "flush_interval_seconds": _coerce_int,
        "lmstudio_base_url": _coerce_str,
        "lmstudio_model": _coerce_str,
        "app_data_dir": _coerce_str,
        "screenshot_dir": _coerce_str,
        "log_dir": _coerce_str,
        "db_path": _coerce_str,
        "config_path": _coerce_str,
        "start_monitoring_on_launch": _coerce_bool,
        "max_screenshots_per_summary": _coerce_int,
        "max_text_segments_per_summary": _coerce_int,
        "max_parallel_summary_jobs": _coerce_int,
        "request_timeout_seconds": _coerce_int,
    }

    for field_name, converter in converters.items():
        if field_name not in data:
            needs_save = True
            continue
        values[field_name] = converter(data[field_name], field_name, source=source)

    values["config_version"] = CONFIG_VERSION
    config = AppConfig(**values)
    before = config.to_dict()
    config.normalize()
    after = config.to_dict()
    if before != after:
        needs_save = True
    return config, needs_save



def default_app_data_dir() -> Path:
    override = os.environ.get("WORKLOG_DIARY_APP_DATA_DIR")
    if override:
        return Path(override).expanduser()
    if is_frozen_executable():
        return Path(sys.executable).resolve().parent / "data"
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA")
        if base:
            return Path(base) / "WorkLogDiary"
    return Path.home() / ".worklog_diary"


def is_frozen_executable() -> bool:
    return bool(getattr(sys, "frozen", False))


def native_hooks_disabled() -> bool:
    if os.environ.get("WORKLOG_DIARY_DISABLE_NATIVE_HOOKS") == "1":
        return True
    return "PYTEST_CURRENT_TEST" in os.environ


def app_data_dir_source() -> str:
    if os.environ.get("WORKLOG_DIARY_APP_DATA_DIR"):
        return "WORKLOG_DIARY_APP_DATA_DIR"
    if is_frozen_executable():
        return "frozen-executable"
    return "local-appdata"



def default_config_path() -> Path:
    return default_app_data_dir() / "config.json"



def config_path_from_env_or_default(explicit_path: str | Path | None = None) -> Path:
    if explicit_path:
        return Path(explicit_path)
    env_path = os.environ.get("WORKLOG_DIARY_CONFIG")
    if env_path:
        return Path(env_path)
    return default_config_path()



def load_config(config_path: str | Path | None = None) -> AppConfig:
    path = config_path_from_env_or_default(config_path)
    if not path.exists():
        config = AppConfig()
        config.config_version = CONFIG_VERSION
        config.config_path = str(path)
        config.ensure_paths()
        save_config(config, path)
        return config

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in config file {path}: {exc.msg}") from exc

    config, needs_save = _build_config_from_mapping(data, source=str(path))
    config.config_path = str(path)
    config.ensure_paths()
    if needs_save:
        save_config(config, path)
    return config



def save_config(config: AppConfig, config_path: str | Path | None = None) -> Path:
    path = config_path_from_env_or_default(config_path or config.config_path)
    config.config_path = str(path)
    config.ensure_paths()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config.to_dict(), indent=2), encoding="utf-8")
    return path
