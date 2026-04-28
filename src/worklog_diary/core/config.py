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
CONFIG_VERSION = 5

_LOGGER = logging.getLogger(__name__)

SAFE_CONFIG_KEYS: tuple[str, ...] = (
    "app_data_dir",
    "screenshot_dir",
    "db_path",
    "lmstudio_base_url",
    "lmstudio_model",
    "screenshot_interval_seconds",
    "flush_interval_seconds",
    "blocked_processes",
    "process_backlog_only_while_locked",
    "max_parallel_summary_jobs",
    "request_timeout_seconds",
    "daily_request_timeout_seconds",
    "capture_mode",
    "semantic_coalescing_enabled",
    "semantic_embedding_base_url",
    "semantic_embedding_model",
    "semantic_max_candidate_gap_seconds",
    "semantic_max_neighbor_count",
    "semantic_min_cosine_similarity",
    "semantic_min_merge_score",
    "semantic_same_app_boost",
    "semantic_window_title_boost",
    "semantic_keyword_overlap_boost",
    "semantic_temporal_gap_penalty_weight",
    "semantic_app_switch_penalty",
    "semantic_lock_boundary_blocks_merge",
    "semantic_pause_boundary_blocks_merge",
    "semantic_transition_keywords",
    "semantic_store_merge_diagnostics",
    "semantic_recompute_missing_embeddings_on_startup",
)


@dataclass(slots=True)
class AppConfig:
    """Validated application settings persisted to `config.json`."""

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
    lmstudio_max_prompt_chars: int = 20000
    app_data_dir: str = ""
    screenshot_dir: str = ""
    log_dir: str = ""
    db_path: str = ""
    config_path: str = ""
    start_monitoring_on_launch: bool = False
    max_screenshots_per_summary: int = 3
    max_text_segments_per_summary: int = 400
    max_parallel_summary_jobs: int = 2
    process_backlog_only_while_locked: bool = True
    request_timeout_seconds: int = 600
    daily_request_timeout_seconds: int | None = None
    activity_segment_min_duration_seconds: float = 180.0
    activity_segment_max_duration_seconds: float = 900.0
    activity_segment_idle_gap_seconds: float = 20.0
    activity_segment_title_similarity_threshold: float = 0.72
    summary_similarity_suppress_threshold: float = 0.86
    summary_similarity_merge_threshold: float = 0.74
    summary_cooldown_seconds: int = 240
    recent_summary_compare_count: int = 5
    screenshot_dedup_exact_hash_enabled: bool = True
    screenshot_dedup_perceptual_hash_enabled: bool = True
    screenshot_dedup_phash_threshold: int = 6
    screenshot_dedup_ssim_enabled: bool = True
    screenshot_dedup_ssim_threshold: float = 0.985
    screenshot_dedup_resize_width: int = 32
    screenshot_dedup_compare_recent_count: int = 8
    screenshot_dedup_enabled: bool = True
    screenshot_min_keep_interval_seconds: int = 120

    semantic_coalescing_enabled: bool = False
    semantic_embedding_base_url: str = "http://127.0.0.1:1234/v1"
    semantic_embedding_model: str = "text-embedding-nomic-embed-text-v1.5"
    semantic_max_candidate_gap_seconds: int = 900
    semantic_max_neighbor_count: int = 2
    semantic_min_cosine_similarity: float = 0.90
    semantic_min_merge_score: float = 0.85
    semantic_same_app_boost: float = 0.20
    semantic_window_title_boost: float = 0.10
    semantic_keyword_overlap_boost: float = 0.10
    semantic_temporal_gap_penalty_weight: float = 0.12
    semantic_app_switch_penalty: float = 0.20
    semantic_lock_boundary_blocks_merge: bool = True
    semantic_pause_boundary_blocks_merge: bool = True
    semantic_transition_keywords: list[str] = field(default_factory=lambda: ["then", "afterward", "next", "switched", "meeting", "call", "pausa", "riunione", "poi", "successivamente"])
    semantic_store_merge_diagnostics: bool = True
    semantic_recompute_missing_embeddings_on_startup: bool = False

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
        self.process_backlog_only_while_locked = bool(self.process_backlog_only_while_locked)
        self.lmstudio_max_prompt_chars = max(2000, int(self.lmstudio_max_prompt_chars))
        if self.daily_request_timeout_seconds is not None:
            self.daily_request_timeout_seconds = max(1, int(self.daily_request_timeout_seconds))
        self.activity_segment_min_duration_seconds = max(0.0, float(self.activity_segment_min_duration_seconds))
        self.activity_segment_max_duration_seconds = max(
            self.activity_segment_min_duration_seconds,
            float(self.activity_segment_max_duration_seconds),
        )
        self.activity_segment_idle_gap_seconds = max(0.0, float(self.activity_segment_idle_gap_seconds))
        self.activity_segment_title_similarity_threshold = max(
            0.0, min(1.0, float(self.activity_segment_title_similarity_threshold))
        )
        self.summary_similarity_suppress_threshold = max(
            0.0, min(1.0, float(self.summary_similarity_suppress_threshold))
        )
        self.summary_similarity_merge_threshold = max(
            0.0, min(self.summary_similarity_suppress_threshold, float(self.summary_similarity_merge_threshold))
        )
        self.summary_cooldown_seconds = max(0, int(self.summary_cooldown_seconds))
        self.recent_summary_compare_count = max(1, int(self.recent_summary_compare_count))
        self.screenshot_dedup_exact_hash_enabled = bool(self.screenshot_dedup_exact_hash_enabled)
        self.screenshot_dedup_perceptual_hash_enabled = bool(self.screenshot_dedup_perceptual_hash_enabled)
        self.screenshot_dedup_phash_threshold = max(0, min(64, int(self.screenshot_dedup_phash_threshold)))
        self.screenshot_dedup_ssim_enabled = bool(self.screenshot_dedup_ssim_enabled)
        self.screenshot_dedup_ssim_threshold = max(0.0, min(1.0, float(self.screenshot_dedup_ssim_threshold)))
        self.screenshot_dedup_resize_width = max(8, int(self.screenshot_dedup_resize_width))
        self.screenshot_dedup_compare_recent_count = max(1, int(self.screenshot_dedup_compare_recent_count))
        self.screenshot_dedup_enabled = bool(self.screenshot_dedup_enabled)
        self.screenshot_min_keep_interval_seconds = max(0, int(self.screenshot_min_keep_interval_seconds))
        self.semantic_coalescing_enabled = bool(self.semantic_coalescing_enabled)
        self.semantic_embedding_base_url = str(self.semantic_embedding_base_url).strip() or "http://127.0.0.1:1234/v1"
        self.semantic_embedding_model = str(self.semantic_embedding_model).strip() or "text-embedding-nomic-embed-text-v1.5"
        self.semantic_max_candidate_gap_seconds = max(0, int(self.semantic_max_candidate_gap_seconds))
        self.semantic_max_neighbor_count = max(1, int(self.semantic_max_neighbor_count))
        self.semantic_min_cosine_similarity = max(0.0, min(1.0, float(self.semantic_min_cosine_similarity)))
        self.semantic_min_merge_score = max(0.0, min(1.0, float(self.semantic_min_merge_score)))
        self.semantic_same_app_boost = max(0.0, float(self.semantic_same_app_boost))
        self.semantic_window_title_boost = max(0.0, float(self.semantic_window_title_boost))
        self.semantic_keyword_overlap_boost = max(0.0, float(self.semantic_keyword_overlap_boost))
        self.semantic_temporal_gap_penalty_weight = max(0.0, float(self.semantic_temporal_gap_penalty_weight))
        self.semantic_app_switch_penalty = max(0.0, float(self.semantic_app_switch_penalty))
        self.semantic_lock_boundary_blocks_merge = bool(self.semantic_lock_boundary_blocks_merge)
        self.semantic_pause_boundary_blocks_merge = bool(self.semantic_pause_boundary_blocks_merge)
        self.semantic_transition_keywords = [str(item).strip().lower() for item in self.semantic_transition_keywords if str(item).strip()]
        self.semantic_store_merge_diagnostics = bool(self.semantic_store_merge_diagnostics)
        self.semantic_recompute_missing_embeddings_on_startup = bool(self.semantic_recompute_missing_embeddings_on_startup)
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


def safe_config_snapshot(config: AppConfig) -> dict[str, Any]:
    data = config.to_dict()
    return {key: data.get(key) for key in SAFE_CONFIG_KEYS}


def safe_config_diff(previous: AppConfig, current: AppConfig) -> dict[str, tuple[Any, Any]]:
    before = safe_config_snapshot(previous)
    after = safe_config_snapshot(current)
    changes: dict[str, tuple[Any, Any]] = {}
    for key in SAFE_CONFIG_KEYS:
        if before.get(key) != after.get(key):
            changes[key] = (before.get(key), after.get(key))
    return changes


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


def _coerce_optional_int(value: Any, field_name: str, *, source: str | None = None) -> int | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return _coerce_int(value, field_name, source=source)


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

def _coerce_str_list(value: Any, field_name: str, *, source: str | None = None) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(_format_config_error(source, field_name, "expected a list of strings", value))
    result: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError(_format_config_error(source, field_name, "expected a list of strings", value))
        if item.strip():
            result.append(item.strip())
    return result


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

    unknown_fields = sorted(name for name in data.keys() if name not in field_names and name != "screenshot_dedup_threshold")
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
        "lmstudio_max_prompt_chars": _coerce_int,
        "app_data_dir": _coerce_str,
        "screenshot_dir": _coerce_str,
        "log_dir": _coerce_str,
        "db_path": _coerce_str,
        "config_path": _coerce_str,
        "start_monitoring_on_launch": _coerce_bool,
        "max_screenshots_per_summary": _coerce_int,
        "max_text_segments_per_summary": _coerce_int,
        "max_parallel_summary_jobs": _coerce_int,
        "process_backlog_only_while_locked": _coerce_bool,
        "request_timeout_seconds": _coerce_int,
        "daily_request_timeout_seconds": _coerce_optional_int,
        "activity_segment_min_duration_seconds": _coerce_float,
        "activity_segment_max_duration_seconds": _coerce_float,
        "activity_segment_idle_gap_seconds": _coerce_float,
        "activity_segment_title_similarity_threshold": _coerce_float,
        "summary_similarity_suppress_threshold": _coerce_float,
        "summary_similarity_merge_threshold": _coerce_float,
        "summary_cooldown_seconds": _coerce_int,
        "recent_summary_compare_count": _coerce_int,
        "screenshot_dedup_exact_hash_enabled": _coerce_bool,
        "screenshot_dedup_perceptual_hash_enabled": _coerce_bool,
        "screenshot_dedup_phash_threshold": _coerce_int,
        "screenshot_dedup_ssim_enabled": _coerce_bool,
        "screenshot_dedup_ssim_threshold": _coerce_float,
        "screenshot_dedup_resize_width": _coerce_int,
        "screenshot_dedup_compare_recent_count": _coerce_int,
        "screenshot_dedup_enabled": _coerce_bool,
        "screenshot_min_keep_interval_seconds": _coerce_int,
        "semantic_coalescing_enabled": _coerce_bool,
        "semantic_embedding_base_url": _coerce_str,
        "semantic_embedding_model": _coerce_str,
        "semantic_max_candidate_gap_seconds": _coerce_int,
        "semantic_max_neighbor_count": _coerce_int,
        "semantic_min_cosine_similarity": _coerce_float,
        "semantic_min_merge_score": _coerce_float,
        "semantic_same_app_boost": _coerce_float,
        "semantic_window_title_boost": _coerce_float,
        "semantic_keyword_overlap_boost": _coerce_float,
        "semantic_temporal_gap_penalty_weight": _coerce_float,
        "semantic_app_switch_penalty": _coerce_float,
        "semantic_lock_boundary_blocks_merge": _coerce_bool,
        "semantic_pause_boundary_blocks_merge": _coerce_bool,
        "semantic_transition_keywords": _coerce_str_list,
        "semantic_store_merge_diagnostics": _coerce_bool,
        "semantic_recompute_missing_embeddings_on_startup": _coerce_bool,
    }

    for field_name, converter in converters.items():
        if field_name not in data:
            needs_save = True
            continue
        values[field_name] = converter(data[field_name], field_name, source=source)

    has_phash_threshold = "screenshot_dedup_phash_threshold" in data
    has_legacy_threshold = "screenshot_dedup_threshold" in data
    if has_legacy_threshold:
        legacy_threshold = _coerce_int(
            data["screenshot_dedup_threshold"],
            "screenshot_dedup_threshold",
            source=source,
        )
        if has_phash_threshold:
            canonical_threshold = values["screenshot_dedup_phash_threshold"]
            if legacy_threshold != canonical_threshold:
                _LOGGER.warning(
                    "event=config_legacy_field_conflict source=%s canonical_field=%s legacy_field=%s behavior=%s",
                    source or "config",
                    "screenshot_dedup_phash_threshold",
                    "screenshot_dedup_threshold",
                    "prefer_canonical",
                )
                needs_save = True
        else:
            values["screenshot_dedup_phash_threshold"] = legacy_threshold
            needs_save = True

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
