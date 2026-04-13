from __future__ import annotations

import sys
from pathlib import Path


def _ensure_src_on_path() -> None:
    if getattr(sys, "frozen", False):
        return

    repo_root = Path(__file__).resolve().parent
    src_dir = repo_root / "src"
    if src_dir.exists() and str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


def main() -> int:
    _ensure_src_on_path()
    from worklog_diary.main import main as app_main

    return app_main()


if __name__ == "__main__":
    raise SystemExit(main())
