from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, datetime, time
from pathlib import Path

from worklog_diary.core.audit_export import AuditExportError, AuditExportOptions, export_audit_bundle
from worklog_diary.core.config import AppConfig
from worklog_diary.core.storage import SQLiteStorage


def _ts(day: date, hour: int, minute: int) -> float:
    return datetime.combine(day, time(hour=hour, minute=minute)).timestamp()


def _insert_summary(storage: SQLiteStorage, *, start_ts: float, end_ts: float, text: str) -> int:
    job_id = storage.create_summary_job(start_ts=start_ts, end_ts=end_ts, status="succeeded")
    return storage.insert_summary(
        job_id=job_id,
        start_ts=start_ts,
        end_ts=end_ts,
        summary_text=text,
        summary_json={
            "summary_text": text,
            "source_context": {
                "process_name": "code.exe",
                "window_title": "editor",
            },
            "metadata": {"schema": "worklog.lmstudio.prompt.v1"},
        },
    )


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_export_creates_jsonl_files_and_manifest_counts(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "wld.db"))
    day = date(2026, 4, 20)
    try:
        sid1 = _insert_summary(storage, start_ts=_ts(day, 9, 0), end_ts=_ts(day, 9, 15), text="alpha")
        _insert_summary(storage, start_ts=_ts(day, 10, 0), end_ts=_ts(day, 10, 15), text="beta")
        storage.create_daily_summary(day, "daily", {"metadata": {"schema": "worklog.daily"}}, 2)
        storage.replace_coalesced_summaries_for_day(
            day,
            [
                type(
                    "Plan",
                    (),
                    {
                        "start_ts": _ts(day, 9, 0),
                        "end_ts": _ts(day, 10, 15),
                        "summary_text": "alpha+beta",
                        "summary_json": {"confidence_bucket": "High"},
                        "source_summary_ids": [sid1],
                    },
                )
            ],
        )
        storage.replace_coalescing_diagnostics_for_day(
            day,
            [
                type(
                    "Diag",
                    (),
                    {
                        "left_summary_id": sid1,
                        "right_summary_id": sid1,
                        "semantic_similarity": 0.95,
                        "app_similarity": 1.0,
                        "window_similarity": 1.0,
                        "keyword_overlap": 0.9,
                        "gap_seconds": 12.0,
                        "blockers": [],
                        "final_score": 0.96,
                        "decision": "merge",
                        "reasons": ["ok"],
                    },
                )
            ],
        )

        export_root = tmp_path / "exports"
        config = AppConfig(db_path=str(tmp_path / "wld.db"))
        result = export_audit_bundle(storage, export_root, AuditExportOptions(), config=config)

        assert (result.output_dir / "summaries.jsonl").exists()
        assert (result.output_dir / "daily_summaries.jsonl").exists()
        assert (result.output_dir / "coalesced_summaries.jsonl").exists()
        assert (result.output_dir / "merge_diagnostics.jsonl").exists()
        assert (result.output_dir / "config_snapshot.json").exists()
        assert (result.output_dir / "manifest.json").exists()
        assert (result.output_dir / "audit_readme.md").exists()

        summaries = _read_jsonl(result.output_dir / "summaries.jsonl")
        daily = _read_jsonl(result.output_dir / "daily_summaries.jsonl")
        coalesced = _read_jsonl(result.output_dir / "coalesced_summaries.jsonl")
        diagnostics = _read_jsonl(result.output_dir / "merge_diagnostics.jsonl")
        manifest = json.loads((result.output_dir / "manifest.json").read_text(encoding="utf-8"))

        assert manifest["counts"]["summaries.jsonl"] == len(summaries)
        assert manifest["counts"]["daily_summaries.jsonl"] == len(daily)
        assert manifest["counts"]["coalesced_summaries.jsonl"] == len(coalesced)
        assert manifest["counts"]["merge_diagnostics.jsonl"] == len(diagnostics)
        assert manifest["contains_raw_activity_data"] is False
        assert manifest["export_scope"] == "summaries_and_coalescing_diagnostics"
    finally:
        storage.close()


def test_export_empty_database_works(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "wld.db"))
    try:
        result = export_audit_bundle(storage, tmp_path / "exports", AuditExportOptions(), config=AppConfig())
        assert _read_jsonl(result.output_dir / "summaries.jsonl") == []
        assert _read_jsonl(result.output_dir / "daily_summaries.jsonl") == []
    finally:
        storage.close()


def test_export_date_range_filtering(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "wld.db"))
    try:
        _insert_summary(storage, start_ts=_ts(date(2026, 4, 10), 9, 0), end_ts=_ts(date(2026, 4, 10), 9, 30), text="old")
        _insert_summary(storage, start_ts=_ts(date(2026, 4, 20), 9, 0), end_ts=_ts(date(2026, 4, 20), 9, 30), text="new")

        options = AuditExportOptions(start_day=date(2026, 4, 15), end_day=date(2026, 4, 21))
        result = export_audit_bundle(storage, tmp_path / "exports", options, config=AppConfig())
        summaries = _read_jsonl(result.output_dir / "summaries.jsonl")
        assert len(summaries) == 1
        assert summaries[0]["summary_text"] == "new"
    finally:
        storage.close()


def test_coalesced_members_query_scales_with_filtered_date_range(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "wld.db"))
    try:
        old_day = date(2026, 4, 10)
        in_range_day = date(2026, 4, 20)
        old_sid = _insert_summary(
            storage,
            start_ts=_ts(old_day, 9, 0),
            end_ts=_ts(old_day, 9, 15),
            text="old",
        )
        in_range_sid = _insert_summary(
            storage,
            start_ts=_ts(in_range_day, 9, 0),
            end_ts=_ts(in_range_day, 9, 15),
            text="in-range",
        )
        storage.replace_coalesced_summaries_for_day(
            old_day,
            [
                type(
                    "Plan",
                    (),
                    {
                        "start_ts": _ts(old_day, 9, 0),
                        "end_ts": _ts(old_day, 9, 15),
                        "summary_text": "old",
                        "summary_json": {"confidence_bucket": "High"},
                        "source_summary_ids": [old_sid],
                    },
                )
            ],
        )
        storage.replace_coalesced_summaries_for_day(
            in_range_day,
            [
                type(
                    "Plan",
                    (),
                    {
                        "start_ts": _ts(in_range_day, 9, 0),
                        "end_ts": _ts(in_range_day, 9, 15),
                        "summary_text": "in-range",
                        "summary_json": {"confidence_bucket": "High"},
                        "source_summary_ids": [in_range_sid],
                    },
                )
            ],
        )

        statements: list[str] = []
        storage._conn.set_trace_callback(statements.append)
        rows = storage.list_audit_coalesced_summaries(
            start_day=date(2026, 4, 15),
            end_day_exclusive=date(2026, 4, 21),
        )
        storage._conn.set_trace_callback(None)

        assert len(rows) == 1
        assert rows[0]["member_summary_ids"] == [in_range_sid]

        members_query = next(sql for sql in statements if "FROM coalesced_summary_members" in sql)
        assert "WHERE coalesced_summary_id IN" in members_query
    finally:
        storage.close()


def test_export_redaction_and_no_raw_activity_fields(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "wld.db"))
    day = date(2026, 4, 20)
    try:
        _insert_summary(storage, start_ts=_ts(day, 9, 0), end_ts=_ts(day, 9, 10), text="alpha")
        default_result = export_audit_bundle(storage, tmp_path / "exports", AuditExportOptions(), config=AppConfig())
        default_rows = _read_jsonl(default_result.output_dir / "summaries.jsonl")
        row = default_rows[0]
        for forbidden in (
            "screenshot_path",
            "screenshot_paths",
            "screenshot_files",
            "raw_text",
            "keypresses",
            "key_logs",
        ):
            assert forbidden not in row

        redacted = export_audit_bundle(
            storage,
            tmp_path / "exports2",
            AuditExportOptions(redact_process_names=True, redact_window_titles=True),
            config=AppConfig(),
        )
        redacted_rows = _read_jsonl(redacted.output_dir / "summaries.jsonl")
        assert redacted_rows[0]["process_name"] == "[REDACTED]"
        assert redacted_rows[0]["window_title"] == "[REDACTED]"
    finally:
        storage.close()


def test_readme_states_no_raw_activity_data(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "wld.db"))
    try:
        result = export_audit_bundle(storage, tmp_path / "exports", AuditExportOptions(), config=AppConfig())
        readme = (result.output_dir / "audit_readme.md").read_text(encoding="utf-8")
        assert "It does not contain screenshots, raw key logs, raw captured text" in readme
        assert "Analyze this WLD summary audit bundle. Focus on:" in readme
    finally:
        storage.close()


def test_merge_diagnostics_export_includes_expected_fields(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "wld.db"))
    day = date(2026, 4, 20)
    try:
        sid = _insert_summary(storage, start_ts=_ts(day, 9, 0), end_ts=_ts(day, 9, 10), text="alpha")
        storage.replace_coalescing_diagnostics_for_day(
            day,
            [
                type(
                    "Diag",
                    (),
                    {
                        "left_summary_id": sid,
                        "right_summary_id": sid,
                        "semantic_similarity": 0.95,
                        "app_similarity": 1.0,
                        "window_similarity": 0.0,
                        "keyword_overlap": 0.5,
                        "gap_seconds": 30.0,
                        "blockers": ["none"],
                        "final_score": 0.8,
                        "decision": "no_merge",
                        "reasons": ["threshold"],
                    },
                )
            ],
        )
        result = export_audit_bundle(storage, tmp_path / "exports", AuditExportOptions(), config=AppConfig())
        rows = _read_jsonl(result.output_dir / "merge_diagnostics.jsonl")
        row = rows[0]
        assert "left_summary_id" in row
        assert "right_summary_id" in row
        assert "decision" in row
        assert "merge_score" in row
        assert "embedding_similarity" in row
        assert "temporal_gap_seconds" in row
        assert "semantic_parameters" in row
    finally:
        storage.close()


def test_invalid_output_directory_returns_clear_error(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "wld.db"))
    try:
        blocked = tmp_path / "blocked"
        blocked.write_text("x", encoding="utf-8")
        try:
            export_audit_bundle(storage, blocked / "out", AuditExportOptions(), config=AppConfig())
            assert False, "Expected export to fail"
        except AuditExportError as exc:
            assert "Could not create export directory" in str(exc)
    finally:
        storage.close()


def test_cli_no_longer_recognizes_removed_screenshot_flags(tmp_path: Path) -> None:
    cmd = [
        sys.executable,
        "tools/export_audit_bundle.py",
        "--output",
        str(tmp_path / "exports"),
        "--include-screenshot-paths",
    ]
    result = subprocess.run(cmd, cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True)
    assert result.returncode != 0
    assert "unrecognized arguments" in result.stderr
