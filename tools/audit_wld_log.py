from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap_src() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "src"
    sys.path.insert(0, str(src_dir))


def main() -> int:
    _bootstrap_src()
    from worklog_diary.tools.log_audit import main as audit_main

    return audit_main()


if __name__ == "__main__":
    raise SystemExit(main())

