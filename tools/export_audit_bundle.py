from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path


def _bootstrap_src() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "src"
    sys.path.insert(0, str(src_dir))


def _parse_day(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def main() -> int:
    _bootstrap_src()

    from worklog_diary.core.audit_export import AuditExportError, AuditExportOptions, export_audit_bundle
    from worklog_diary.core.config import load_config
    from worklog_diary.core.storage import SQLiteStorage

    parser = argparse.ArgumentParser(
        description="Export WorkLog Diary summary/coalescing audit bundle"
    )
    parser.add_argument("--config", dest="config_path", default=None, help="Path to config.json")
    parser.add_argument("--db", dest="db_path", default=None, help="Path to encrypted SQLite DB")
    parser.add_argument("--output", required=True, help="Destination parent folder")
    parser.add_argument("--from", dest="start_day", type=_parse_day, default=None, help="Start day YYYY-MM-DD")
    parser.add_argument("--to", dest="end_day", type=_parse_day, default=None, help="End day YYYY-MM-DD (inclusive)")
    parser.add_argument("--redact-window-titles", action="store_true", help="Redact window titles")
    parser.add_argument("--redact-process-names", action="store_true", help="Redact process names")
    args = parser.parse_args()

    if args.start_day and args.end_day and args.start_day > args.end_day:
        print("Error: --from must be earlier than or equal to --to.", file=sys.stderr)
        return 2

    config = load_config(Path(args.config_path) if args.config_path else None)
    db_path = str(Path(args.db_path)) if args.db_path else config.db_path

    storage = SQLiteStorage(db_path)
    try:
        options = AuditExportOptions(
            start_day=args.start_day,
            end_day=args.end_day,
            redact_window_titles=bool(args.redact_window_titles),
            redact_process_names=bool(args.redact_process_names),
        )
        result = export_audit_bundle(
            storage=storage,
            output_dir=Path(args.output),
            options=options,
            config=config,
        )
    except AuditExportError as exc:
        print(f"Audit export failed: {exc}", file=sys.stderr)
        return 1
    finally:
        storage.close()

    print(f"Exported audit bundle to: {result.output_dir}")
    for name, count in sorted(result.counts.items()):
        print(f"- {name}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
