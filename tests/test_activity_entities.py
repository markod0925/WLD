from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path

from worklog_diary.core.activity_extraction import extract_activity_entities, extract_activity_entities_with_coverage
from worklog_diary.core.storage import SQLiteStorage


def _ts(day: date, hour: int, minute: int = 0) -> float:
    return datetime.combine(day, time(hour=hour, minute=minute)).astimezone().timestamp()


def _by_type(rows):
    grouped: dict[str, list[object]] = {}
    for row in rows:
        grouped.setdefault(row.entity_type, []).append(row)
    return grouped


def test_windows_path_extraction_emits_file_entities_and_dirty_marker() -> None:
    drafts = extract_activity_entities(
        start_ts=1.0,
        end_ts=2.0,
        process_name="MATLAB.exe",
        window_title="Editor - U:\\PROJ1\\XXX\\data\\Input_cases.m *",
    )
    grouped = _by_type(drafts)

    file_path = grouped["file_path"][0]
    file_name = grouped["file_name"][0]
    folder_path = grouped["folder_path"][0]
    project_candidate = grouped["project_candidate"][0]
    window_title = grouped["window_title"][0]

    assert file_path.entity_value == "U:\\PROJ1\\XXX\\data\\Input_cases.m"
    assert file_path.evidence_kind == "observed"
    assert file_path.attributes["dirty_marker"] is True
    assert file_path.attributes["likely_edited"] is True
    assert file_name.entity_value == "Input_cases.m"
    assert folder_path.entity_value == "U:\\PROJ1\\XXX\\data"
    assert project_candidate.entity_value == "PROJ1"
    assert project_candidate.evidence_kind == "likely"
    assert window_title.attributes["dirty_marker"] is True


def test_outlook_subject_extraction() -> None:
    drafts = extract_activity_entities(
        start_ts=1.0,
        end_ts=2.0,
        process_name="OUTLOOK.EXE",
        window_title="RE: Project Phoenix - Outlook",
    )
    grouped = _by_type(drafts)

    assert grouped["mail_subject"][0].entity_value == "RE: Project Phoenix"
    assert grouped["mail_subject"][0].evidence_kind == "observed"


def test_browser_title_extraction() -> None:
    drafts = extract_activity_entities(
        start_ts=1.0,
        end_ts=2.0,
        process_name="chrome.exe",
        window_title="Daily build notes - Google Chrome",
    )
    grouped = _by_type(drafts)

    assert grouped["web_page_title"][0].entity_value == "Daily build notes"
    assert grouped["web_page_title"][0].source_kind == "window_title"


def test_ticket_label_extraction() -> None:
    drafts = extract_activity_entities(
        start_ts=1.0,
        end_ts=2.0,
        process_name="code.exe",
        window_title="Implement ABC-1234 support",
        text_segments=["Working on ABC-1234 and DEF-4321"],
    )
    tickets = sorted(item.entity_value for item in drafts if item.entity_type == "task_candidate")

    assert tickets == ["ABC-1234", "DEF-4321"]


def test_unknown_app_title_preserves_raw_evidence_and_generic_tokens() -> None:
    drafts, coverage = extract_activity_entities_with_coverage(
        start_ts=1.0,
        end_ts=2.0,
        process_name="FooTool",
        window_title="FooTool - U:\\PROJ1\\XXX\\data\\Input_cases.m *",
    )
    grouped = _by_type(drafts)

    assert grouped["program"][0].entity_value == "FooTool"
    assert grouped["program"][0].confidence == 1.0
    assert grouped["window_title"][0].entity_value == "FooTool - U:\\PROJ1\\XXX\\data\\Input_cases.m *"
    assert grouped["unclassified_window_title"][0].entity_value == "FooTool - U:\\PROJ1\\XXX\\data\\Input_cases.m *"
    assert grouped["file_path"][0].entity_value == "U:\\PROJ1\\XXX\\data\\Input_cases.m"
    assert grouped["file_name"][0].entity_value == "Input_cases.m"
    assert grouped["project_candidate"][0].entity_value == "PROJ1"
    assert coverage["unknown_app"] is True
    assert coverage["used_generic_parser"] is True
    assert coverage["used_specialized_parser"] is False
    assert coverage["unclassified_evidence_count"] == 1


def test_unknown_app_ticket_and_file_token_extraction() -> None:
    drafts, coverage = extract_activity_entities_with_coverage(
        start_ts=1.0,
        end_ts=2.0,
        process_name="FooTool",
        window_title="FooTool - Analysis for ABC-1234 - run_042.log",
    )
    grouped = _by_type(drafts)

    assert grouped["program"][0].entity_value == "FooTool"
    assert grouped["unclassified_window_title"][0].entity_value == "FooTool - Analysis for ABC-1234 - run_042.log"
    assert grouped["task_candidate"][0].entity_value == "ABC-1234"
    assert grouped["file_name"][0].entity_value == "run_042.log"
    assert coverage["unknown_app"] is True
    assert "ticket_label" in coverage["matched_parser_names"]
    assert "file_token" in coverage["matched_parser_names"]


def test_storage_roundtrip_and_search_normalizes_paths(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    day = date(2026, 4, 20)
    start_ts = _ts(day, 9, 0)
    end_ts = _ts(day, 9, 15)
    try:
        job_id = storage.create_summary_job(start_ts=start_ts, end_ts=end_ts, status="succeeded")
        summary_id = storage.insert_summary(
            job_id=job_id,
            start_ts=start_ts,
            end_ts=end_ts,
            summary_text="Batch summary",
            summary_json={"summary_text": "Batch summary"},
        )
        drafts = extract_activity_entities(
            start_ts=start_ts,
            end_ts=end_ts,
            process_name="MATLAB.exe",
            window_title="Editor - U:\\PROJ1\\XXX\\data\\Input_cases.m *",
            text_segments=["Working on ABC-1234"],
        )
        storage.add_activity_entities(
            day=day,
            start_ts=start_ts,
            end_ts=end_ts,
            summary_id=summary_id,
            entities=drafts,
        )

        rows = storage.list_activity_entities_for_day(day)
        assert any(row.entity_type == "file_path" and row.entity_value == "U:\\PROJ1\\XXX\\data\\Input_cases.m" for row in rows)
        assert any(row.entity_type == "file_name" and row.entity_value == "Input_cases.m" for row in rows)
        assert any(row.entity_type == "project_candidate" and row.entity_value == "PROJ1" for row in rows)

        path_matches = storage.search_activity_entities(
            entity_type="file_path",
            query="u:/proj1/xxx/data/input_cases.m",
            day_from=day,
            day_to=day,
            min_confidence=0.9,
        )
        file_name_matches = storage.search_activity_entities(
            entity_type="file_name",
            query="input_cases.m",
            day_from=day,
            day_to=day,
        )

        assert len(path_matches) == 1
        assert path_matches[0].entity_value == "U:\\PROJ1\\XXX\\data\\Input_cases.m"
        assert len(file_name_matches) == 1
        assert file_name_matches[0].entity_value == "Input_cases.m"
    finally:
        storage.close()
