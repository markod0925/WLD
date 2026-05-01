from __future__ import annotations

from dataclasses import asdict
from datetime import date

from worklog_diary.core.activity_extraction import extract_activity_entities
from worklog_diary.core.evidence_quality import score_event_evidence_quality


def _high_quality_payload() -> dict:
    return {
        "summary_text": "Edited Input_cases.m in MATLAB while reviewing ABC-1234.",
        "primary_activity": [{"text": "editing a MATLAB file", "confidence": 0.96}],
        "programs_used": [{"name": "MATLAB", "confidence": 0.99}],
        "files": [
            {
                "entity_type": "file_path",
                "entity_value": "U:\\PROJ1\\XXX\\data\\Input_cases.m",
                "entity_normalized": "u:/proj1/xxx/data/input_cases.m",
                "evidence_kind": "observed",
            }
        ],
        "conversations": [],
        "task_candidates": [{"text": "review ABC-1234", "confidence": 0.88}],
        "outcomes": [{"text": "updated MATLAB input handling", "confidence": 0.91}],
        "follow_ups": [{"text": "verify downstream test coverage", "confidence": 0.74}],
        "blocked_activity": [],
        "unknowns": [],
        "metadata": {"parse_status": "validated", "included_counts": {"text_segments": 1, "screenshots": 1}},
    }


def test_high_quality_event_scores_good_or_better() -> None:
    start_ts = 100.0
    end_ts = 160.0
    entities = extract_activity_entities(
        start_ts=start_ts,
        end_ts=end_ts,
        process_name="MATLAB.exe",
        window_title="Editor - U:\\PROJ1\\XXX\\data\\Input_cases.m *",
        text_segments=["Working on ABC-1234"],
    )
    report = score_event_evidence_quality(
        summary_id=1,
        day=date(2026, 4, 20),
        start_ts=start_ts,
        end_ts=end_ts,
        summary_json=_high_quality_payload(),
        activity_entities=[asdict(item) for item in entities],
        parser_coverage=[
            {
                "process_name": "MATLAB.exe",
                "parser_confidence": 0.96,
                "unknown_app": False,
                "used_generic_parser": True,
                "used_specialized_parser": True,
            }
        ],
        source_batch={"text_segment_count": 1, "screenshot_count": 1, "blocked_interval_count": 0},
        source_context={"process_name": "MATLAB.exe", "window_title": "Editor - U:\\PROJ1\\XXX\\data\\Input_cases.m *"},
    )

    assert report.score >= 0.85
    assert report.bucket == "excellent"
    assert report.has_file_evidence is True
    assert report.has_task_evidence is True
    assert report.degraded_payload is False


def test_raw_process_and_window_only_scores_poor() -> None:
    report = score_event_evidence_quality(
        summary_id=2,
        day=date(2026, 4, 20),
        start_ts=10.0,
        end_ts=20.0,
        summary_json={
            "summary_text": "Worked on something.",
            "metadata": {"parse_status": "validated"},
            "source_context": {"process_name": "FooTool", "window_title": "FooTool"},
        },
        activity_entities=[],
        parser_coverage=[
            {
                "process_name": "FooTool",
                "parser_confidence": 0.1,
                "unknown_app": True,
                "used_generic_parser": True,
                "used_specialized_parser": False,
            }
        ],
        source_batch={"text_segment_count": 0, "screenshot_count": 0, "blocked_interval_count": 0},
        source_context={"process_name": "FooTool", "window_title": "FooTool"},
    )

    assert report.score <= 0.35
    assert report.bucket in {"weak", "poor"}
    assert report.has_file_evidence is False
    assert report.has_task_evidence is False


def test_unknown_app_generic_file_extraction_beats_raw_unknown_app() -> None:
    start_ts = 200.0
    end_ts = 260.0
    base_payload = {
        "summary_text": "Unknown app session.",
        "metadata": {"parse_status": "validated"},
        "source_context": {"process_name": "FooTool", "window_title": "FooTool - Analysis"},
    }
    no_entities_report = score_event_evidence_quality(
        summary_id=3,
        day=date(2026, 4, 20),
        start_ts=start_ts,
        end_ts=end_ts,
        summary_json=base_payload,
        activity_entities=[],
        parser_coverage=[
            {
                "process_name": "FooTool",
                "parser_confidence": 0.05,
                "unknown_app": True,
                "used_generic_parser": True,
                "used_specialized_parser": False,
            }
        ],
        source_batch={"text_segment_count": 0, "screenshot_count": 0, "blocked_interval_count": 0},
        source_context={"process_name": "FooTool", "window_title": "FooTool - Analysis"},
    )

    extracted_entities = extract_activity_entities(
        start_ts=start_ts,
        end_ts=end_ts,
        process_name="FooTool",
        window_title="FooTool - U:\\PROJ1\\XXX\\data\\Input_cases.m *",
    )
    extracted_report = score_event_evidence_quality(
        summary_id=4,
        day=date(2026, 4, 20),
        start_ts=start_ts,
        end_ts=end_ts,
        summary_json={
            **base_payload,
            "source_context": {"process_name": "FooTool", "window_title": "FooTool - U:\\PROJ1\\XXX\\data\\Input_cases.m *"},
        },
        activity_entities=[asdict(item) for item in extracted_entities],
        parser_coverage=[
            {
                "process_name": "FooTool",
                "parser_confidence": 0.2,
                "unknown_app": True,
                "used_generic_parser": True,
                "used_specialized_parser": False,
            }
        ],
        source_batch={"text_segment_count": 0, "screenshot_count": 0, "blocked_interval_count": 0},
        source_context={"process_name": "FooTool", "window_title": "FooTool - U:\\PROJ1\\XXX\\data\\Input_cases.m *"},
    )

    assert extracted_report.score > no_entities_report.score
    assert extracted_report.has_file_evidence is True
    assert extracted_report.unknown_app is True


def test_degraded_payload_penalizes_score_and_scoring_is_deterministic() -> None:
    payload = _high_quality_payload()
    degraded_payload = dict(payload)
    degraded_payload["metadata"] = {"parse_status": "degraded"}

    first = score_event_evidence_quality(
        summary_id=5,
        day=date(2026, 4, 20),
        start_ts=300.0,
        end_ts=360.0,
        summary_json=payload,
        activity_entities=[
            asdict(item)
            for item in extract_activity_entities(
                start_ts=300.0,
                end_ts=360.0,
                process_name="MATLAB.exe",
                window_title="Editor - U:\\PROJ1\\XXX\\data\\Input_cases.m *",
                text_segments=["Working on ABC-1234"],
            )
        ],
        parser_coverage=[
            {
                "process_name": "MATLAB.exe",
                "parser_confidence": 0.96,
                "unknown_app": False,
                "used_generic_parser": True,
                "used_specialized_parser": True,
            }
        ],
        source_batch={"text_segment_count": 1, "screenshot_count": 1, "blocked_interval_count": 0},
        source_context={"process_name": "MATLAB.exe", "window_title": "Editor - U:\\PROJ1\\XXX\\data\\Input_cases.m *"},
    )
    second = score_event_evidence_quality(
        summary_id=5,
        day=date(2026, 4, 20),
        start_ts=300.0,
        end_ts=360.0,
        summary_json=payload,
        activity_entities=[
            asdict(item)
            for item in extract_activity_entities(
                start_ts=300.0,
                end_ts=360.0,
                process_name="MATLAB.exe",
                window_title="Editor - U:\\PROJ1\\XXX\\data\\Input_cases.m *",
                text_segments=["Working on ABC-1234"],
            )
        ],
        parser_coverage=[
            {
                "process_name": "MATLAB.exe",
                "parser_confidence": 0.96,
                "unknown_app": False,
                "used_generic_parser": True,
                "used_specialized_parser": True,
            }
        ],
        source_batch={"text_segment_count": 1, "screenshot_count": 1, "blocked_interval_count": 0},
        source_context={"process_name": "MATLAB.exe", "window_title": "Editor - U:\\PROJ1\\XXX\\data\\Input_cases.m *"},
    )
    degraded = score_event_evidence_quality(
        summary_id=5,
        day=date(2026, 4, 20),
        start_ts=300.0,
        end_ts=360.0,
        summary_json=degraded_payload,
        activity_entities=[
            asdict(item)
            for item in extract_activity_entities(
                start_ts=300.0,
                end_ts=360.0,
                process_name="MATLAB.exe",
                window_title="Editor - U:\\PROJ1\\XXX\\data\\Input_cases.m *",
                text_segments=["Working on ABC-1234"],
            )
        ],
        parser_coverage=[
            {
                "process_name": "MATLAB.exe",
                "parser_confidence": 0.96,
                "unknown_app": False,
                "used_generic_parser": True,
                "used_specialized_parser": True,
            }
        ],
        source_batch={"text_segment_count": 1, "screenshot_count": 1, "blocked_interval_count": 0},
        source_context={"process_name": "MATLAB.exe", "window_title": "Editor - U:\\PROJ1\\XXX\\data\\Input_cases.m *"},
    )

    assert first.to_dict() == second.to_dict()
    assert degraded.score < first.score
    assert degraded.degraded_payload is True
