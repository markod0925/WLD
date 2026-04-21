from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from ..core.config import AppConfig, load_config
from ..core.storage import SQLiteStorage


def main() -> int:
    parser = argparse.ArgumentParser(description="WorkLog Diary diagnostics and validation tools")
    subparsers = parser.add_subparsers(dest="command", required=True)

    pending_parser = subparsers.add_parser("pending", help="Print DB table counts and pending ranges")
    pending_parser.add_argument("--config", dest="config_path", default=None, help="Path to config.json")
    pending_parser.add_argument("--db", dest="db_path", default=None, help="Path to sqlite DB")

    flush_parser = subparsers.add_parser(
        "flush-buffered",
        help="Drain buffered data by running summary batches until completion, failure, or cancel",
    )
    flush_parser.add_argument("--config", dest="config_path", default=None, help="Path to config.json")
    flush_parser.add_argument("--reason", default="debug-cli", help="Summary job reason label")

    semantic_parser = subparsers.add_parser(
        "semantic-diag",
        help="Print semantic merge diagnostics rows for a specific day",
    )
    semantic_parser.add_argument("--config", dest="config_path", default=None, help="Path to config.json")
    semantic_parser.add_argument("--db", dest="db_path", default=None, help="Path to sqlite DB")
    semantic_parser.add_argument("--day", required=True, help="Day in YYYY-MM-DD format")
    semantic_parser.add_argument("--limit", type=int, default=100, help="Maximum rows to print")

    args = parser.parse_args()
    if args.command == "pending":
        return _run_pending(config_path=args.config_path, db_path=args.db_path)
    if args.command == "flush-buffered":
        return _run_flush(config_path=args.config_path, reason=args.reason)
    if args.command == "semantic-diag":
        return _run_semantic_diag(
            config_path=args.config_path,
            db_path=args.db_path,
            day=args.day,
            limit=args.limit,
        )
    raise ValueError(f"Unsupported command: {args.command}")


def _run_pending(config_path: str | None, db_path: str | None) -> int:
    resolved_db_path = _resolve_db_path(config_path=config_path, db_path=db_path)
    config = _load_config_for_reporting(config_path=config_path, db_path=db_path)
    storage = SQLiteStorage(resolved_db_path)
    try:
        diagnostics = storage.get_diagnostics_snapshot()
    finally:
        storage.close()

    table_counts: dict[str, int] = diagnostics["table_counts"]
    pending_counts: dict[str, int] = diagnostics["pending_counts"]
    pending_ranges: dict[str, dict | None] = diagnostics["pending_ranges"]
    summary_jobs: dict[str, int] = diagnostics["summary_jobs"]

    if config is not None:
        print(f"Config: {config.config_path}")
        print(f"Data: {config.app_data_dir}")
        print(f"Logs: {config.log_dir}")
        print(f"Screenshots: {config.screenshot_dir}")
    else:
        print("Config: (not loaded; explicit --db override in use)")
    print(f"DB: {resolved_db_path}")
    print("")
    print("Table counts:")
    for name in (
        "active_intervals",
        "blocked_intervals",
        "key_events",
        "text_segments",
        "screenshots",
        "summary_jobs",
        "summaries",
        "daily_summaries",
    ):
        print(f"  - {name}: {table_counts.get(name, 0)}")

    print("")
    print("Pending counts:")
    print(f"  - active_intervals: {pending_counts.get('intervals', 0)}")
    print(f"  - key_events_unprocessed: {pending_counts.get('key_events', 0)}")
    print(f"  - key_events_processed: {pending_counts.get('processed_key_events', 0)}")
    print(f"  - text_segments: {pending_counts.get('text_segments', 0)}")
    print(f"  - screenshots: {pending_counts.get('screenshots', 0)}")

    print("")
    print("Pending ranges:")
    _print_range("active_intervals_unsummarized", pending_ranges.get("active_intervals_unsummarized"))
    _print_range("blocked_intervals_unsummarized", pending_ranges.get("blocked_intervals_unsummarized"))
    _print_range("key_events_unprocessed", pending_ranges.get("key_events_unprocessed"))
    _print_range("text_segments_pending", pending_ranges.get("text_segments_pending"))
    _print_range("screenshots_pending", pending_ranges.get("screenshots_pending"))

    print("")
    print("Summary jobs:")
    print(f"  - running: {summary_jobs.get('running', 0)}")
    print(f"  - failed: {summary_jobs.get('failed', 0)}")
    print(f"  - succeeded: {summary_jobs.get('succeeded', 0)}")
    return 0


def _run_flush(config_path: str | None, reason: str) -> int:
    from ..app import create_services

    services = create_services(config_path=config_path)
    try:
        result = services.flush_now(reason=reason)
    finally:
        services.shutdown()

    if result is None:
        print("Drain already running; no new flush operation started.")
    else:
        print(
            "Drain finished: "
            f"stop={result.stop_reason} created={result.summaries_created} "
            f"failed={result.failed_jobs} cancelled={result.cancelled_jobs} "
            f"pending_summary_jobs={result.pending_summary_jobs}"
        )
    return 0


def _run_semantic_diag(config_path: str | None, db_path: str | None, day: str, limit: int) -> int:
    resolved_db_path = _resolve_db_path(config_path=config_path, db_path=db_path)
    target_day = date.fromisoformat(day)
    storage = SQLiteStorage(resolved_db_path)
    try:
        rows = storage.list_semantic_merge_diagnostics(target_day, limit=limit)
    finally:
        storage.close()
    print(f"DB: {resolved_db_path}")
    print(f"Day: {target_day.isoformat()}")
    print(f"Rows: {len(rows)}")
    for row in rows:
        blockers = ",".join(row.blockers_json)
        print(
            f"- pair={row.left_summary_id}->{row.right_summary_id} decision={row.decision} "
            f"score={row.final_merge_score:.3f} cosine={row.embedding_cosine_similarity:.3f} "
            f"gap={row.temporal_gap_seconds:.1f} blockers=[{blockers}]"
        )
    return 0


def _resolve_db_path(config_path: str | None, db_path: str | None) -> str:
    if db_path:
        return str(Path(db_path))
    config = load_config(Path(config_path) if config_path else None)
    return config.db_path


def _load_config_for_reporting(config_path: str | None, db_path: str | None) -> AppConfig | None:
    if db_path and not config_path:
        return None
    return load_config(Path(config_path) if config_path else None)


def _print_range(name: str, value: dict | None) -> None:
    if not value:
        print(f"  - {name}: none")
        return
    print(
        f"  - {name}: count={value.get('count', 0)} "
        f"start_ts={value.get('start_ts', 0.0):.3f} end_ts={value.get('end_ts', 0.0):.3f}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
