from __future__ import annotations

import json
from datetime import date, datetime, time
from pathlib import Path

from worklog_diary.core.models import SummaryRecord
from worklog_diary.core.storage import SQLiteStorage
from worklog_diary.core.task_evidence_extraction import extract_task_evidence


def _summary(process: str, window: str, text: str, blocked: bool = False) -> SummaryRecord:
    return SummaryRecord(
        id=1,
        job_id=1,
        start_ts=1.0,
        end_ts=10.0,
        summary_text=text,
        summary_json={"source_context": {"process_name": process, "window_title": window, "blocked": blocked}},
        created_ts=1.0,
    )


def test_matlab_extraction() -> None:
    ev = extract_task_evidence(_summary("matlab.exe", "Genetic Algorithm", "Pareto Front Fitness Average Spread"))
    assert ev.primary_task_label == "MATLAB genetic optimization"
    assert ev.primary_activity_type in {"optimization_run", "result_analysis"}
    assert any(e.entity_type == "app" and e.entity_value == "matlab.exe" for e in ev.entities)


def test_matlab_artifact_support_notepad() -> None:
    ev = extract_task_evidence(_summary("notepad.exe", "OptimHistory_110kts.txt - Notepad", "review"))
    assert ev.primary_task_label == "MATLAB genetic optimization"
    assert ev.primary_activity_type == "file_review"
    assert any("OptimHistory_110kts.txt" in e.entity_value for e in ev.entities if e.entity_type == "file")


def test_matlab_code_editing() -> None:
    ev = extract_task_evidence(_summary("matlab.exe", r"Editor - U:\TOOLS\SMASH\LFM_SMASH_plotAll.m", "editing"))
    assert ev.primary_activity_type == "code_editing"
    assert any("lfm_smash_plotall.m" in e.entity_normalized.lower() for e in ev.entities if e.entity_type == "file")


def test_wld_extraction() -> None:
    ev = extract_task_evidence(_summary("chrome.exe", "WorkLog Diary Summaries", "search merge defect investigation with ChatGPT"))
    assert ev.primary_task_label == "WLD summary/search review"
    assert ev.primary_activity_type in {"software_debugging", "analysis"}


def test_email_extraction() -> None:
    ev = extract_task_evidence(_summary("outlook.exe", "Posta in arrivo - Outlook", "email correspondence"))
    assert ev.primary_task_label == "Email handling"
    assert ev.primary_activity_type == "communication"


def test_noise_unknown_short() -> None:
    s = _summary("unknown.exe", "Unknown", "manual flush")
    s.end_ts = 5.0
    ev = extract_task_evidence(s)
    assert ev.is_low_value is True and ev.noise_reason == "unknown_process_short_duration"
    assert ev.primary_task_label is None


def test_blocked_suppression() -> None:
    ev = extract_task_evidence(_summary("chrome.exe", "blocked activity", "content unavailable", blocked=True))
    assert ev.is_blocked is True and ev.is_low_value is True
    assert ev.noise_reason == "blocked_content"
    assert ev.primary_task_label is None


def test_blocked_but_semantic() -> None:
    ev = extract_task_evidence(_summary("chrome.exe", "ChatGPT - WLD", "blocked activity plus WLD summary search defect", blocked=True))
    assert ev.primary_task_label == "WLD summary/search review"
    assert ev.is_low_value is False


def test_storage_integration_roundtrip(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    try:
        day = date(2026, 5, 1)
        ts = datetime.combine(day, time(9)).timestamp()
        job_id = storage.create_summary_job(start_ts=ts, end_ts=ts + 60, status="succeeded")
        sid = storage.insert_summary(job_id=job_id, start_ts=ts, end_ts=ts + 60, summary_text="test", summary_json={"source_context": {"process_name": "matlab.exe", "window_title": "Genetic Algorithm", "blocked": False}})
        ev = extract_task_evidence(_summary("matlab.exe", "Genetic Algorithm", "Pareto"))
        storage.update_event_summary_structured_fields(sid, structured_payload_json=ev.payload, primary_task_label=ev.primary_task_label, primary_activity_type=ev.primary_activity_type, is_blocked=ev.is_blocked, is_low_value=ev.is_low_value, noise_reason=ev.noise_reason, confidence=ev.confidence)
        storage.replace_activity_entities_for_summary(day=day, start_ts=ts, end_ts=ts + 60, summary_id=sid, entities=ev.entities)
        storage.replace_activity_entities_for_summary(day=day, start_ts=ts, end_ts=ts + 60, summary_id=sid, entities=ev.entities)
        rows = storage.list_activity_entities_for_summary(sid)
        assert len(rows) == len(ev.entities)
        db_row = storage._conn.execute("SELECT structured_payload_json, primary_task_label FROM summaries WHERE id=?", (sid,)).fetchone()
        assert db_row is not None and json.loads(str(db_row["structured_payload_json"]))["schema_version"] == 1
    finally:
        storage.close()
