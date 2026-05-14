from __future__ import annotations

from enum import StrEnum
from pathlib import PureWindowsPath


class ActivityPathKind(StrEnum):
    USER_FILE = "user_file"
    INTERNAL_ARTIFACT = "internal_artifact"
    SYSTEM_LOG = "system_log"
    DATABASE = "database"
    UNKNOWN = "unknown"


_INTERNAL_DIRS = {"screenshots", "captures", "summaries", "logs", "audit_exports", "cache"}
_RUNTIME_NAMES = {"db_key.bin", "session_state.json", "crash_faulthandler.log"}


def classify_activity_path(path: str, *, app_data_dir: str | None = None) -> ActivityPathKind:
    raw = str(path or "").strip()
    if not raw:
        return ActivityPathKind.UNKNOWN
    p = PureWindowsPath(raw)
    parts = [part.lower() for part in p.parts]
    name = p.name.lower()
    if name in _RUNTIME_NAMES:
        return ActivityPathKind.INTERNAL_ARTIFACT
    if p.suffix.lower() in {".sqlite", ".db"} and _is_under_app_data(raw, app_data_dir):
        return ActivityPathKind.DATABASE
    if _is_under_app_data(raw, app_data_dir):
        if any(segment in _INTERNAL_DIRS for segment in parts):
            return ActivityPathKind.INTERNAL_ARTIFACT
    if "wld" in parts and "data" in parts and any(segment in _INTERNAL_DIRS for segment in parts):
        return ActivityPathKind.INTERNAL_ARTIFACT
    return ActivityPathKind.USER_FILE


def is_internal_artifact_path(path: str, *, app_data_dir: str | None = None) -> bool:
    return classify_activity_path(path, app_data_dir=app_data_dir) in {
        ActivityPathKind.INTERNAL_ARTIFACT,
        ActivityPathKind.DATABASE,
    }


def _is_under_app_data(path: str, app_data_dir: str | None) -> bool:
    if not app_data_dir:
        return False
    base = PureWindowsPath(str(app_data_dir).strip())
    target = PureWindowsPath(str(path).strip())
    base_parts = [part.lower() for part in base.parts]
    target_parts = [part.lower() for part in target.parts]
    return len(target_parts) >= len(base_parts) and target_parts[: len(base_parts)] == base_parts
