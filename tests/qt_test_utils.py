from __future__ import annotations

import os

import pytest


def require_qt() -> None:
    """Skip current test module when Qt/PySide6 cannot initialize."""
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    try:
        from PySide6.QtWidgets import QApplication  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        pytest.skip(
            f"Qt/PySide6 unavailable in this environment: {exc}",
            allow_module_level=True,
        )
