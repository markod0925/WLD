from __future__ import annotations

import logging
from pathlib import Path


def configure_logging(log_dir: str, level: int = logging.INFO) -> None:
    path = Path(log_dir)
    path.mkdir(parents=True, exist_ok=True)
    log_path = path / "worklog_diary.log"

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(level)

    if not any(isinstance(handler, logging.StreamHandler) for handler in root.handlers):
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        root.addHandler(console)

    if not any(isinstance(handler, logging.FileHandler) and Path(handler.baseFilename) == log_path for handler in root.handlers):
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
