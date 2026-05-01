from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "tools" / "analyze_audit_export.py"


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    text = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    if rows:
        text += "\n"
    path.write_text(text, encoding="utf-8")


def _make_bundle(path: Path, *, include_optional: bool = True) -> None:
    path.mkdir(parents=True, exist_ok=True)

    summaries = [
        {"summary_id": 1, "day": "2026-04-20", "summary_text": "alpha"},
        {"summary_id": 2, "day": "2026-04-20", "summary_text": "beta"},
        {"summary_id": 3, "day": "2026-04-21", "summary_text": "gamma"},
        {"summary_id": 4, "day": "2026-04-21", "summary_text": "delta"},
        {"summary_id": 5, "day": "2026-04-21", "summary_text": "epsilon"},
        {"summary_id": 6, "day": "2026-04-22", "summary_text": "zeta"},
    ]
    daily = [
        {"day": "2026-04-20", "daily_summary_id": 10},
        {"day": "2026-04-22", "daily_summary_id": 11},
    ]
    activity_entities = [
        {"entity_type": "program", "entity_value": "code.exe", "entity_normalized": "code.exe", "source_kind": "process_name", "source_ref": "code.exe", "confidence": 1.0},
        {"entity_type": "program", "entity_value": "code.exe", "entity_normalized": "code.exe", "source_kind": "process_name", "source_ref": "code.exe", "confidence": 1.0},
        {"entity_type": "program", "entity_value": "code.exe", "entity_normalized": "code.exe", "source_kind": "process_name", "source_ref": "code.exe", "confidence": 1.0},
        {"entity_type": "file_path", "entity_value": "U:\\PROJ1\\XXX\\data\\Input_cases.m", "entity_normalized": "u:\\proj1\\xxx\\data\\input_cases.m", "source_kind": "window_title", "source_ref": "Editor", "confidence": 0.95},
        {"entity_type": "file_path", "entity_value": "U:\\PROJ1\\XXX\\data\\Input_cases.m", "entity_normalized": "u:\\proj1\\xxx\\data\\input_cases.m", "source_kind": "window_title", "source_ref": "Editor", "confidence": 0.95},
        {"entity_type": "file_path", "entity_value": "U:\\PROJ1\\XXX\\data\\Input_cases.m", "entity_normalized": "u:\\proj1\\xxx\\data\\input_cases.m", "source_kind": "window_title", "source_ref": "Editor", "confidence": 0.95},
        {"entity_type": "file_name", "entity_value": "Input_cases.m", "entity_normalized": "input_cases.m", "source_kind": "window_title", "source_ref": "Editor", "confidence": 0.95},
        {"entity_type": "folder_path", "entity_value": "U:\\PROJ1\\XXX\\data", "entity_normalized": "u:\\proj1\\xxx\\data", "source_kind": "window_title", "source_ref": "Editor", "confidence": 0.95},
        {"entity_type": "task_candidate", "entity_value": "ABC-1234", "entity_normalized": "abc-1234", "source_kind": "window_title", "source_ref": "Editor", "confidence": 0.77},
        {"entity_type": "conversation_subject", "entity_value": "Project Phoenix", "entity_normalized": "project phoenix", "source_kind": "window_title", "source_ref": "Mail", "confidence": 0.92},
        {"entity_type": "conversation_subject", "entity_value": "Project Phoenix", "entity_normalized": "project phoenix", "source_kind": "window_title", "source_ref": "Mail", "confidence": 0.92},
        {"entity_type": "mail_subject", "entity_value": "RE: Project Phoenix", "entity_normalized": "re: project phoenix", "source_kind": "window_title", "source_ref": "Mail", "confidence": 0.88},
        {"entity_type": "web_page_title", "entity_value": "Daily build notes", "entity_normalized": "daily build notes", "source_kind": "window_title", "source_ref": "Browser", "confidence": 0.91},
        {"entity_type": "web_page_title", "entity_value": "Daily build notes", "entity_normalized": "daily build notes", "source_kind": "window_title", "source_ref": "Browser", "confidence": 0.91},
    ]
    parser_coverage = [
        {
            "process_name": "FooTool.exe",
            "normalized_process_name": "footool.exe",
            "window_title": "FooTool - Analysis for ABC-1234 - run_042.log",
            "normalized_window_title": "footool - analysis for abc-1234 - run_042.log",
            "matched_parser_names": ["generic_window"],
            "used_generic_parser": True,
            "used_specialized_parser": False,
            "extracted_entity_count": 1,
            "unclassified_evidence_count": 1,
            "parser_confidence": 0.2,
            "unknown_app": True,
        },
        {
            "process_name": "FooTool.exe",
            "normalized_process_name": "footool.exe",
            "window_title": "FooTool - Analysis for ABC-1234 - run_042.log",
            "normalized_window_title": "footool - analysis for abc-1234 - run_042.log",
            "matched_parser_names": ["generic_window"],
            "used_generic_parser": True,
            "used_specialized_parser": False,
            "extracted_entity_count": 1,
            "unclassified_evidence_count": 1,
            "parser_confidence": 0.2,
            "unknown_app": True,
        },
        {
            "process_name": "BarTool.exe",
            "normalized_process_name": "bartool.exe",
            "window_title": "BarTool - Scratchpad",
            "normalized_window_title": "bartool - scratchpad",
            "matched_parser_names": ["generic_window"],
            "used_generic_parser": True,
            "used_specialized_parser": False,
            "extracted_entity_count": 0,
            "unclassified_evidence_count": 1,
            "parser_confidence": 0.15,
            "unknown_app": True,
        },
    ]
    unknown_apps = [
        {
            "process_name": "FooTool.exe",
            "normalized_process_name": "footool.exe",
            "occurrence_count": 2,
            "first_seen_ts": 1.0,
            "last_seen_ts": 2.0,
            "sample_window_title": "FooTool - Analysis for ABC-1234 - run_042.log",
            "normalized_title_sample": "footool - analysis for abc-1234 - run_042.log",
            "extracted_entity_count": 1,
            "unclassified_evidence_count": 2,
            "matched_parser_names": ["generic_window"],
            "parser_confidence": 0.2,
            "unknown_app": True,
        },
        {
            "process_name": "BarTool.exe",
            "normalized_process_name": "bartool.exe",
            "occurrence_count": 1,
            "first_seen_ts": 3.0,
            "last_seen_ts": 4.0,
            "sample_window_title": "BarTool - Scratchpad",
            "normalized_title_sample": "bartool - scratchpad",
            "extracted_entity_count": 0,
            "unclassified_evidence_count": 1,
            "matched_parser_names": ["generic_window"],
            "parser_confidence": 0.15,
            "unknown_app": True,
        },
    ]
    unknown_window_patterns = [
        {
            "process_name": "FooTool.exe",
            "normalized_process_name": "footool.exe",
            "sample_window_title": "FooTool - Analysis for ABC-1234 - run_042.log",
            "normalized_title_sample": "footool - analysis for abc-1234 - run_042.log",
            "occurrence_count": 2,
            "first_seen_ts": 1.0,
            "last_seen_ts": 2.0,
            "extracted_entity_count": 1,
            "candidate_patterns": ["generic_window"],
            "suggested_parser_reason": "No specialized parser matched; generic parsers extracted generic_window.",
            "unknown_app": True,
        },
        {
            "process_name": "BarTool.exe",
            "normalized_process_name": "bartool.exe",
            "sample_window_title": "BarTool - Scratchpad",
            "normalized_title_sample": "bartool - scratchpad",
            "occurrence_count": 1,
            "first_seen_ts": 3.0,
            "last_seen_ts": 4.0,
            "extracted_entity_count": 0,
            "candidate_patterns": ["generic_window"],
            "suggested_parser_reason": "No specialized parser matched; generic parsers extracted generic_window.",
            "unknown_app": True,
        },
    ]
    low_confidence = [
        {"entity_type": "task_candidate", "entity_value": "ABC-1234", "entity_normalized": "abc-1234", "source_kind": "window_title", "source_ref": "Editor", "confidence": 0.77},
        {"entity_type": "mail_subject", "entity_value": "RE: Project Phoenix", "entity_normalized": "re: project phoenix", "source_kind": "window_title", "source_ref": "Mail", "confidence": 0.78},
    ]
    evidence_quality = [
        {
            "summary_id": 1,
            "day": "2026-04-20",
            "start_ts": 1.0,
            "end_ts": 2.0,
            "score": 0.9,
            "bucket": "excellent",
            "strengths": ["file evidence present"],
            "weaknesses": [],
            "entity_counts_by_type": {"program": 1, "file_path": 1, "task_candidate": 1, "conversation_subject": 1},
            "unknown_app": False,
            "degraded_payload": False,
            "has_file_evidence": True,
            "has_task_evidence": True,
            "has_conversation_evidence": True,
            "has_text_evidence": True,
            "has_screenshot_evidence": False,
            "blocked_or_privacy_heavy": False,
            "summary_kind": "event",
            "source_summary_count": None,
        },
        {
            "summary_id": 2,
            "day": "2026-04-20",
            "start_ts": 3.0,
            "end_ts": 4.0,
            "score": 0.82,
            "bucket": "good",
            "strengths": ["file evidence present"],
            "weaknesses": [],
            "entity_counts_by_type": {"program": 1, "file_path": 1},
            "unknown_app": False,
            "degraded_payload": False,
            "has_file_evidence": True,
            "has_task_evidence": False,
            "has_conversation_evidence": False,
            "has_text_evidence": True,
            "has_screenshot_evidence": False,
            "blocked_or_privacy_heavy": False,
            "summary_kind": "event",
            "source_summary_count": None,
        },
        {
            "summary_id": 3,
            "day": "2026-04-21",
            "start_ts": 5.0,
            "end_ts": 6.0,
            "score": 0.28,
            "bucket": "poor",
            "strengths": [],
            "weaknesses": ["structured payload degraded", "unknown app with no derived entities"],
            "entity_counts_by_type": {"program": 1, "window_title": 1},
            "unknown_app": True,
            "degraded_payload": True,
            "has_file_evidence": False,
            "has_task_evidence": False,
            "has_conversation_evidence": False,
            "has_text_evidence": False,
            "has_screenshot_evidence": False,
            "blocked_or_privacy_heavy": False,
            "summary_kind": "event",
            "source_summary_count": None,
        },
        {
            "summary_id": 4,
            "day": "2026-04-21",
            "start_ts": 7.0,
            "end_ts": 8.0,
            "score": 0.34,
            "bucket": "poor",
            "strengths": [],
            "weaknesses": ["structured payload degraded", "only raw process/window evidence"],
            "entity_counts_by_type": {"program": 1, "window_title": 1},
            "unknown_app": True,
            "degraded_payload": True,
            "has_file_evidence": False,
            "has_task_evidence": False,
            "has_conversation_evidence": False,
            "has_text_evidence": False,
            "has_screenshot_evidence": False,
            "blocked_or_privacy_heavy": False,
            "summary_kind": "event",
            "source_summary_count": None,
        },
        {
            "summary_id": 5,
            "day": "2026-04-21",
            "start_ts": 9.0,
            "end_ts": 10.0,
            "score": 0.4,
            "bucket": "weak",
            "strengths": ["generic summary text without concrete entities"],
            "weaknesses": [],
            "entity_counts_by_type": {"program": 1, "window_title": 1},
            "unknown_app": True,
            "degraded_payload": False,
            "has_file_evidence": False,
            "has_task_evidence": False,
            "has_conversation_evidence": False,
            "has_text_evidence": True,
            "has_screenshot_evidence": False,
            "blocked_or_privacy_heavy": False,
            "summary_kind": "event",
            "source_summary_count": None,
        },
        {
            "summary_id": 6,
            "day": "2026-04-22",
            "start_ts": 11.0,
            "end_ts": 12.0,
            "score": 0.88,
            "bucket": "excellent",
            "strengths": ["file evidence present"],
            "weaknesses": [],
            "entity_counts_by_type": {"program": 1, "file_path": 1, "mail_subject": 1, "web_page_title": 1},
            "unknown_app": False,
            "degraded_payload": False,
            "has_file_evidence": True,
            "has_task_evidence": False,
            "has_conversation_evidence": True,
            "has_text_evidence": True,
            "has_screenshot_evidence": False,
            "blocked_or_privacy_heavy": False,
            "summary_kind": "event",
            "source_summary_count": None,
        },
        {
            "summary_id": 101,
            "day": "2026-04-20",
            "start_ts": 13.0,
            "end_ts": 14.0,
            "score": 0.74,
            "bucket": "good",
            "strengths": ["daily recap is reasonably grounded"],
            "weaknesses": [],
            "entity_counts_by_type": {"program": 1, "file_path": 1, "task_candidate": 1},
            "unknown_app": False,
            "degraded_payload": False,
            "has_file_evidence": True,
            "has_task_evidence": True,
            "has_conversation_evidence": False,
            "has_text_evidence": True,
            "has_screenshot_evidence": False,
            "blocked_or_privacy_heavy": False,
            "summary_kind": "daily",
            "source_summary_count": 2,
        },
        {
            "summary_id": 102,
            "day": "2026-04-22",
            "start_ts": 15.0,
            "end_ts": 16.0,
            "score": 0.77,
            "bucket": "good",
            "strengths": ["daily recap is reasonably grounded"],
            "weaknesses": [],
            "entity_counts_by_type": {"program": 1, "file_path": 1, "conversation_subject": 1},
            "unknown_app": False,
            "degraded_payload": False,
            "has_file_evidence": True,
            "has_task_evidence": False,
            "has_conversation_evidence": True,
            "has_text_evidence": True,
            "has_screenshot_evidence": False,
            "blocked_or_privacy_heavy": False,
            "summary_kind": "daily",
            "source_summary_count": 1,
        },
    ]
    evidence_summary = {
        "summary_count": len(evidence_quality),
        "event_summary_count": 6,
        "daily_summary_count": 2,
        "bucket_counts": {"excellent": 2, "good": 3, "weak": 1, "poor": 2},
        "average_score": 0.639,
        "poor_or_weak_summary_count": 3,
        "summaries_without_file_or_task_entities": 3,
        "summaries_with_only_unclassified_evidence": 0,
        "degraded_payload_count": 2,
        "unknown_app_count": 3,
        "top_unknown_processes": [{"process_name": "FooTool.exe", "occurrence_count": 2}],
        "top_low_confidence_entity_types": [{"entity_type": "task_candidate", "count": 1}],
        "parser_coverage_by_process": [{"process_name": "FooTool.exe", "occurrence_count": 2}],
    }
    manifest = {
        "audit_export_format_version": 1,
        "exported_at_utc": "2026-04-22T10:00:00Z",
        "app_version": "test",
        "export_scope": "summaries_and_coalescing_diagnostics_and_activity_entities_and_parser_coverage_and_evidence_quality",
        "contains_raw_activity_data": False,
        "counts": {
            "summaries.jsonl": len(summaries),
            "daily_summaries.jsonl": len(daily),
            "activity_entities.jsonl": len(activity_entities),
            "parser_coverage.jsonl": len(parser_coverage),
            "unknown_apps.jsonl": len(unknown_apps),
            "unknown_window_patterns.jsonl": len(unknown_window_patterns),
            "low_confidence_entities.jsonl": len(low_confidence),
            "evidence_quality.jsonl": len(evidence_quality),
            "evidence_quality_summary.json": 1,
        },
        "evidence_quality_count": len(evidence_quality),
        "evidence_quality_bucket_counts": evidence_summary["bucket_counts"],
        "average_evidence_quality_score": evidence_summary["average_score"],
        "poor_or_weak_summary_count": evidence_summary["poor_or_weak_summary_count"],
    }

    _write_jsonl(path / "summaries.jsonl", summaries)
    if include_optional:
        _write_jsonl(path / "daily_summaries.jsonl", daily)
        _write_jsonl(path / "activity_entities.jsonl", activity_entities)
        _write_jsonl(path / "parser_coverage.jsonl", parser_coverage)
        _write_jsonl(path / "unknown_apps.jsonl", unknown_apps)
        _write_jsonl(path / "unknown_window_patterns.jsonl", unknown_window_patterns)
        _write_jsonl(path / "low_confidence_entities.jsonl", low_confidence)
        _write_jsonl(path / "evidence_quality.jsonl", evidence_quality)
        _write_json(path / "evidence_quality_summary.json", evidence_summary)
        _write_json(path / "manifest.json", manifest)
    else:
        _write_jsonl(path / "activity_entities.jsonl", activity_entities)


def _run(bundle: Path, *extra_args: str) -> subprocess.CompletedProcess[str]:
    cmd = [sys.executable, str(SCRIPT), str(bundle), *extra_args]
    return subprocess.run(cmd, cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True)


def test_basic_report_generation(tmp_path: Path) -> None:
    bundle = tmp_path / "current"
    _make_bundle(bundle)

    result = _run(bundle)

    assert result.returncode == 0, result.stderr
    stdout = result.stdout
    assert "# WLD Audit Export Analysis" in stdout
    assert "Date range covered: `2026-04-20` to `2026-04-22`" in stdout
    assert "Event summary count: `6`" in stdout
    assert "Evidence quality bucket counts: excellent 2, good 3, weak 1, poor 2" in stdout
    assert "Top entity types: `program` (3)" in stdout
    assert "Top processes: `code.exe` (3)" in stdout
    assert "Top files:" in stdout
    assert "Top task/ticket candidates:" in stdout
    assert "Top conversation/mail/web subjects:" in stdout


def test_missing_optional_files(tmp_path: Path) -> None:
    bundle = tmp_path / "legacy"
    _make_bundle(bundle, include_optional=False)

    result = _run(bundle)

    assert result.returncode == 0, result.stderr
    stdout = result.stdout
    assert "Date range covered:" in stdout
    assert "Daily summary count: `0`" in stdout
    assert "Evidence quality bucket counts: excellent 0, good 0, weak 0, poor 0" in stdout
    assert "Top unknown processes: none" in stdout


def test_compare_mode(tmp_path: Path) -> None:
    current = tmp_path / "current"
    baseline = tmp_path / "baseline"
    _make_bundle(current)
    _make_bundle(baseline, include_optional=False)
    _write_jsonl(
        baseline / "summaries.jsonl",
        [
            {"summary_id": 1, "day": "2026-04-20", "summary_text": "alpha"},
            {"summary_id": 2, "day": "2026-04-21", "summary_text": "beta"},
        ],
    )
    _write_jsonl(
        baseline / "activity_entities.jsonl",
        [
            {"entity_type": "program", "entity_value": "code.exe", "entity_normalized": "code.exe", "source_kind": "process_name", "source_ref": "code.exe", "confidence": 1.0},
            {"entity_type": "file_path", "entity_value": "U:\\PROJ1\\XXX\\data\\Input_cases.m", "entity_normalized": "u:\\proj1\\xxx\\data\\input_cases.m", "source_kind": "window_title", "source_ref": "Editor", "confidence": 0.95},
        ],
    )
    _write_jsonl(
        baseline / "evidence_quality.jsonl",
        [
            {
                "summary_id": 1,
                "day": "2026-04-20",
                "start_ts": 1.0,
                "end_ts": 2.0,
                "score": 0.24,
                "bucket": "poor",
                "strengths": [],
                "weaknesses": [],
                "entity_counts_by_type": {"program": 1},
                "unknown_app": True,
                "degraded_payload": True,
                "has_file_evidence": False,
                "has_task_evidence": False,
                "has_conversation_evidence": False,
                "has_text_evidence": False,
                "has_screenshot_evidence": False,
                "blocked_or_privacy_heavy": False,
                "summary_kind": "event",
                "source_summary_count": None,
            },
            {
                "summary_id": 2,
                "day": "2026-04-21",
                "start_ts": 3.0,
                "end_ts": 4.0,
                "score": 0.72,
                "bucket": "good",
                "strengths": [],
                "weaknesses": [],
                "entity_counts_by_type": {"program": 1, "file_path": 1},
                "unknown_app": False,
                "degraded_payload": False,
                "has_file_evidence": True,
                "has_task_evidence": False,
                "has_conversation_evidence": False,
                "has_text_evidence": True,
                "has_screenshot_evidence": False,
                "blocked_or_privacy_heavy": False,
                "summary_kind": "event",
                "source_summary_count": None,
            },
        ],
    )
    _write_jsonl(
        baseline / "unknown_apps.jsonl",
        [
            {
                "process_name": "FooTool.exe",
                "normalized_process_name": "footool.exe",
                "occurrence_count": 1,
                "first_seen_ts": 1.0,
                "last_seen_ts": 2.0,
                "sample_window_title": "FooTool - Analysis",
                "normalized_title_sample": "footool - analysis",
                "extracted_entity_count": 1,
                "unclassified_evidence_count": 1,
                "matched_parser_names": ["generic_window"],
                "parser_confidence": 0.2,
                "unknown_app": True,
            }
        ],
    )
    _write_jsonl(
        baseline / "unknown_window_patterns.jsonl",
        [
            {
                "process_name": "FooTool.exe",
                "normalized_process_name": "footool.exe",
                "sample_window_title": "FooTool - Analysis",
                "normalized_title_sample": "footool - analysis",
                "occurrence_count": 1,
                "first_seen_ts": 1.0,
                "last_seen_ts": 2.0,
                "extracted_entity_count": 1,
                "candidate_patterns": ["generic_window"],
                "suggested_parser_reason": "No specialized parser matched.",
                "unknown_app": True,
            }
        ],
    )
    _write_jsonl(
        baseline / "parser_coverage.jsonl",
        [
            {
                "process_name": "FooTool.exe",
                "normalized_process_name": "footool.exe",
                "window_title": "FooTool - Analysis",
                "normalized_window_title": "footool - analysis",
                "matched_parser_names": ["generic_window"],
                "used_generic_parser": True,
                "used_specialized_parser": False,
                "extracted_entity_count": 1,
                "unclassified_evidence_count": 1,
                "parser_confidence": 0.2,
                "unknown_app": True,
            }
        ],
    )
    _write_jsonl(
        baseline / "low_confidence_entities.jsonl",
        [
            {"entity_type": "program", "entity_value": "code.exe", "entity_normalized": "code.exe", "source_kind": "process_name", "source_ref": "code.exe", "confidence": 0.77}
        ],
    )

    result = _run(current, "--compare", str(baseline))

    assert result.returncode == 0, result.stderr
    stdout = result.stdout
    assert "## Delta vs Baseline" in stdout
    assert "Summary count delta: `+4`" in stdout
    assert "Entity count delta: `+12`" in stdout
    assert "Entities per summary delta:" in stdout
    assert "Evidence quality bucket deltas:" in stdout
    assert "Average score delta:" in stdout
    assert "Degraded payload delta:" in stdout
    assert "Unknown app delta:" in stdout
    assert "File/task/conversation entity deltas:" in stdout
    assert "Missing daily recap delta:" in stdout


def test_missing_daily_recap_detection(tmp_path: Path) -> None:
    bundle = tmp_path / "current"
    _make_bundle(bundle)

    result = _run(bundle)

    assert result.returncode == 0, result.stderr
    assert "Days with event summaries but missing daily recap: `2026-04-21`" in result.stdout


def test_weak_poor_evidence_recommendation(tmp_path: Path) -> None:
    bundle = tmp_path / "current"
    _make_bundle(bundle)

    result = _run(bundle)

    assert result.returncode == 0, result.stderr
    stdout = result.stdout
    assert "Many event summaries still rely on process/window evidence only." in stdout
    assert "Daily recaps are missing for active days." in stdout
    assert "Structured payload degradation is frequent enough" in stdout
    assert "File evidence is strong but task candidates are thin." in stdout


def test_top_entity_aggregation(tmp_path: Path) -> None:
    bundle = tmp_path / "current"
    _make_bundle(bundle)

    result = _run(bundle)

    assert result.returncode == 0, result.stderr
    stdout = result.stdout
    assert "Top entity types: `program` (3);" in stdout
    assert "`file_path` (3)" in stdout
    assert "Top files: `U:\\PROJ1\\XXX\\data\\Input_cases.m` (3) [file_path]; `Input_cases.m` (1) [file_name]; `U:\\PROJ1\\XXX\\data` (1) [folder_path]" in stdout
    assert "Top task/ticket candidates: `ABC-1234` (1) [task_candidate]" in stdout
    assert "Top conversation/mail/web subjects:" in stdout
    assert "`Project Phoenix` (2) [conversation_subject]" in stdout
    assert "`RE: Project Phoenix` (1) [mail_subject]" in stdout
    assert "`Daily build notes` (2) [web_page_title]" in stdout
