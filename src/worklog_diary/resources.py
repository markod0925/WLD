from __future__ import annotations

import sys
from pathlib import Path


def project_root() -> Path:
    frozen_root = getattr(sys, "_MEIPASS", None)
    if frozen_root:
        return Path(frozen_root)
    return Path(__file__).resolve().parents[2]


def asset_path(*parts: str) -> Path:
    return project_root() / "assets" / Path(*parts)


def app_logo_path() -> Path:
    return asset_path("WLD_Logo.png")
