from __future__ import annotations

from pathlib import Path

from worklog_diary.tools.log_audit import (
    LogAuditRunner,
    ParsedEvent,
    normalize_signature,
    parse_fields,
)


def _write_log(tmp_path: Path, name: str, lines: list[str]) -> Path:
    path = tmp_path / name
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_multiline_traceback_and_timestamp_parsing(tmp_path: Path) -> None:
    log_path = _write_log(
        tmp_path,
        "traceback.log",
        [
            "2026-04-24 10:00:00 [ERROR] worklog_diary.core.session_monitor: startup failed reason=boom",
            "Traceback (most recent call last):",
            '  File "C:\\repo\\src\\worklog_diary\\core\\session_monitor.py", line 12, in start',
            "    raise ValueError('bad value')",
            "ValueError: bad value",
        ],
    )
    out_dir = tmp_path / "out"

    runner = LogAuditRunner([log_path], out_dir)
    outputs = runner.run()

    assert runner.stats.total_lines == 5
    assert runner.stats.parsed_entries == 1
    assert runner.stats.first_timestamp is not None
    assert runner.stats.first_timestamp.strftime("%Y-%m-%d %H:%M:%S") == "2026-04-24 10:00:00"
    assert runner.stats.last_timestamp == runner.stats.first_timestamp
    assert outputs["error_taxonomy"][0]["has_traceback"] is True
    assert outputs["error_taxonomy"][0]["error_class"] == "ValueError"
    parsed_lines = (out_dir / "parsed_events.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(parsed_lines) == 1
    assert "ValueError: bad value" in parsed_lines[0]


def test_job_lifecycle_and_lmstudio_audit(tmp_path: Path) -> None:
    log_path = _write_log(
        tmp_path,
        "summary.log",
        [
            "2026-04-24 11:00:00 [INFO] worklog_diary.core.summarizer: [LLM] stage=job_created job_id=summary-1 job_type=event_summary target_day=2026-04-24",
            "2026-04-24 11:00:01 [INFO] worklog_diary.core.summarizer: [LLM] stage=request_submit job_id=summary-1 endpoint=http://127.0.0.1:1234/v1/chat/completions model=llama elapsed_s=0.0 timeout_s=30",
            "2026-04-24 11:00:02 [INFO] worklog_diary.core.summarizer: [LLM] stage=request_success job_id=summary-1 endpoint=http://127.0.0.1:1234/v1/chat/completions model=llama elapsed_s=1.25 http_status=200",
            "2026-04-24 11:00:03 [INFO] worklog_diary.core.summarizer: [LLM] stage=response_parse job_id=summary-1 status=ok",
            "2026-04-24 11:00:04 [INFO] worklog_diary.core.summarizer: [LLM] stage=summary_store job_id=summary-1 status=ok db_path=C:\\db\\wld.db",
            "2026-04-24 11:00:05 [INFO] worklog_diary.core.summarizer: [LLM] stage=summary_job_completed job_id=summary-1",
        ],
    )
    out_dir = tmp_path / "out"

    runner = LogAuditRunner([log_path], out_dir)
    outputs = runner.run()

    summary_queue = outputs["summary_queue"]
    lmstudio = outputs["lmstudio"]
    assert summary_queue["job_count"] == 1
    assert summary_queue["terminal_jobs"] == 1
    assert summary_queue["incomplete_jobs"] == []
    assert summary_queue["long_running_jobs"] == []
    assert lmstudio["total_requests"] == 1
    assert lmstudio["success_count"] == 1
    assert lmstudio["failure_count"] == 0
    assert lmstudio["models"] == ["llama"]
    assert lmstudio["base_urls"] == ["http://127.0.0.1:1234/v1/chat/completions"]
    assert lmstudio["latency"]["count"] == 1
    assert lmstudio["latency"]["max"] == 1.25


def test_normalize_signature_masks_volatile_values() -> None:
    event = ParsedEvent(
        source_file="/tmp/worklog.log",
        line_start=1,
        line_end=1,
        timestamp="2026-04-24 12:00:00",
        level="ERROR",
        logger="worklog_diary.core.storage",
        message="db open failed for C:\\Users\\alice\\AppData\\Local\\WorkLog\\wld.db job_id=42 request_id=123e4567-e89b-12d3-a456-426614174000 elapsed_s=3.25",
        traceback="Traceback (most recent call last):\n  File \"C:\\Users\\alice\\repo\\src\\worklog_diary\\core\\storage.py\", line 10, in open_db\n    raise RuntimeError('boom')\nRuntimeError: boom",
        event_name="db_open",
        category="storage",
        subsystem="storage",
        fields=parse_fields(
            "job_id=42 request_id=123e4567-e89b-12d3-a456-426614174000 elapsed_s=3.25 db_path=C:\\Users\\alice\\AppData\\Local\\WorkLog\\wld.db"
        ),
        error_class="RuntimeError",
    )

    signature = normalize_signature(event)

    assert "<uuid>" in signature
    assert "<path>" in signature
    assert "42" not in signature
    assert "3.25" not in signature


def test_log_audit_flags_shutdown_and_crash_regressions(tmp_path: Path) -> None:
    log_path = _write_log(
        tmp_path,
        "regression.log",
        [
            "2026-04-24 12:00:00 [INFO] svc: event=shutdown_start",
            "2026-04-24 12:00:01 [INFO] svc: event=storage_closed",
            "2026-04-24 12:00:02 [INFO] svc: event=summary_flush_triggered reason=scheduled",
            "2026-04-24 12:00:03 [INFO] svc: event=shutdown_complete",
        ],
    )
    out_dir = tmp_path / "out"
    outputs = LogAuditRunner([log_path], out_dir).run()
    anomaly_types = {item["type"] for item in outputs["anomalies"]}
    assert "shutdown_storage_ordering_violation" in anomaly_types
    assert "missing_summary_workers_joined" in anomaly_types
    assert "missing_crash_finalization_marker" in anomaly_types


def test_log_audit_skips_session_monitor_missing_marker_when_inactive(tmp_path: Path) -> None:
    log_path = _write_log(
        tmp_path,
        "no-monitor.log",
        [
            "2026-04-24 09:00:00 [INFO] svc: event=runtime_paths mode=dev",
            "2026-04-24 09:00:01 [INFO] svc: event=shutdown_complete",
        ],
    )
    out_dir = tmp_path / "out-inactive"
    outputs = LogAuditRunner([log_path], out_dir).run()
    anomaly_types = {item["type"] for item in outputs["anomalies"]}
    assert "session_monitor_no_start_evidence" not in anomaly_types
