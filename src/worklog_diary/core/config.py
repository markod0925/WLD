from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

DEFAULT_BLOCKED_PROCESSES = ["chrome.exe", "msedge.exe", "webex.exe", "lm studio.exe"]


@dataclass(slots=True)
class AppConfig:
    blocked_processes: list[str] = field(default_factory=lambda: DEFAULT_BLOCKED_PROCESSES.copy())
    screenshot_interval_seconds: int = 30
    foreground_poll_interval_seconds: float = 1.0
    text_inactivity_gap_seconds: float = 8.0
    reconstruction_poll_interval_seconds: float = 2.0
    flush_interval_seconds: int = 300
    lmstudio_base_url: str = "http://127.0.0.1:1234/v1"
    lmstudio_model: str = "local-model"
    app_data_dir: str = ""
    screenshot_dir: str = ""
    db_path: str = ""
    config_path: str = ""
    start_monitoring_on_launch: bool = False
    max_screenshots_per_summary: int = 3
    max_text_segments_per_summary: int = 200
    request_timeout_seconds: int = 60

    def normalize(self) -> None:
        if not self.app_data_dir:
            self.app_data_dir = str(default_app_data_dir())
        app_data_path = Path(self.app_data_dir)

        if not self.screenshot_dir:
            self.screenshot_dir = str(app_data_path / "screenshots")
        if not self.db_path:
            self.db_path = str(app_data_path / "worklog_diary.db")
        if not self.config_path:
            self.config_path = str(default_config_path())

        self.blocked_processes = [p.strip().lower() for p in self.blocked_processes if p.strip()]

    def ensure_paths(self) -> None:
        self.normalize()
        Path(self.app_data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.screenshot_dir).mkdir(parents=True, exist_ok=True)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> dict:
        return asdict(self)



def default_app_data_dir() -> Path:
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA")
        if base:
            return Path(base) / "WorkLogDiary"
    return Path.home() / ".worklog_diary"



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
        config.config_path = str(path)
        config.ensure_paths()
        save_config(config, path)
        return config

    data = json.loads(path.read_text(encoding="utf-8"))
    config = AppConfig(**data)
    config.config_path = str(path)
    config.ensure_paths()
    return config



def save_config(config: AppConfig, config_path: str | Path | None = None) -> Path:
    path = config_path_from_env_or_default(config_path or config.config_path)
    config.config_path = str(path)
    config.ensure_paths()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config.to_dict(), indent=2), encoding="utf-8")
    return path
