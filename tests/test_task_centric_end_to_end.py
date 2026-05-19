from __future__ import annotations

import json
from datetime import date, datetime, time
from pathlib import Path

from worklog_diary.core.audit_export import AuditExportOptions, export_audit_bundle
from worklog_diary.core.config import AppConfig
from worklog_diary.core.storage import SQLiteStorage
from worklog_diary.core.summary_search import SummarySearchParams, SummarySearchScope, SummarySearchService
from worklog_diary.core.task_clustering import cluster_tasks_for_day
from worklog_diary.core.task_daily_summary import build_daily_summary_from_task_clusters


def _ts(day: date, h: int, m: int = 0) -> float:
    return datetime.combine(day, time(h, m)).timestamp()


def _insert(storage: SQLiteStorage, day: date, i: int, text: str, task: str | None, activity: str | None, low=False, reason=None):
    st = _ts(day, 9, i)
    en = st + 40
    job = storage.create_summary_job(start_ts=st, end_ts=en, status='succeeded')
    sid = storage.insert_summary(job_id=job, start_ts=st, end_ts=en, summary_text=text, summary_json={"is_low_value": low, "noise_reason": reason, "primary_task_label": task, "primary_activity_type": activity})
    storage.update_event_summary_structured_fields(sid, structured_payload_json={"schema_version": 1}, primary_task_label=task, primary_activity_type=activity, is_blocked=(reason=='blocked_content'), is_low_value=low, noise_reason=reason, confidence=0.8)
    return sid


def test_task_centric_end_to_end(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / 'w.db'))
    day = date(2026, 5, 3)
    try:
        _insert(storage, day, 1, 'matlab.exe Genetic Algorithm Pareto Fitness Average Spread', 'MATLAB genetic optimization', 'optimization_run')
        _insert(storage, day, 2, 'matlabwindow.exe Bode and Eigenvalues', 'MATLAB genetic optimization', 'plot_review')
        _insert(storage, day, 3, 'matlabwindow.exe Optimal Solution Selection Final Results Plots', 'MATLAB genetic optimization', 'result_analysis')
        _insert(storage, day, 4, r'matlab.exe Editor - U:\TOOLS\SMASH\LFM_SMASH_plotAll.m', 'MATLAB genetic optimization', 'code_editing')
        _insert(storage, day, 5, 'notepad.exe OptimHistory_110kts.txt', 'MATLAB genetic optimization', 'file_review')
        _insert(storage, day, 6, 'notepad.exe OptimHistory_90kts.txt', 'MATLAB genetic optimization', 'file_review')
        _insert(storage, day, 7, 'excel.exe Varie.xlsx optimization', 'MATLAB genetic optimization', 'file_review')
        _insert(storage, day, 8, 'wld.exe WorkLog Diary Summaries', 'WLD summary/search review', 'software_debugging')
        _insert(storage, day, 9, 'chrome.exe ChatGPT WLD summary search merge defect', 'WLD summary/search review', 'analysis')
        _insert(storage, day, 10, 'outlook.exe Posta in arrivo email correspondence', 'Email handling', 'communication')
        _insert(storage, day, 11, 'chrome.exe blocked content unavailable', None, 'blocked_noise', low=True, reason='blocked_content')
        _insert(storage, day, 12, 'unknown.exe manual flush', None, 'generic_app_switching_noise', low=True, reason='unknown_process_short_duration')
        _insert(storage, day, 13, 'searchhost.exe', None, 'system_noise', low=True, reason='searchhost')

        cluster_tasks_for_day(day, storage=storage)
        clusters = storage.list_task_clusters_for_day(day)
        assert len([c for c in clusters if c['title']=='MATLAB genetic optimization']) == 1
        assert len([c for c in clusters if c['title']=='WLD summary/search review']) == 1
        assert len([c for c in clusters if c['title']=='Email handling']) == 1

        daily = build_daily_summary_from_task_clusters(day, clusters, storage.list_summaries_for_day(day), ignored_noise=storage.count_low_value_noise_reasons_for_day(day))
        assert any(t['title']=='MATLAB genetic optimization' for t in daily.payload['main_tasks'])
        assert any(t['title']=='WLD summary/search review' for t in daily.payload['main_tasks'])
        assert any(t['title']=='Email handling' for t in daily.payload['minor_tasks'])
        noise = {x['reason'] for x in daily.payload['ignored_noise']}
        assert {'blocked_content', 'unknown_process_short_duration', 'searchhost'} <= noise

        storage.create_daily_summary(day=day, recap_text=daily.summary_text, recap_json=daily.payload, source_batch_count=13, structured_payload_json=daily.payload, generated_from_task_clusters=True)

        search = SummarySearchService(storage)
        assert any('MATLAB genetic optimization' in r.text for r in search.search(SummarySearchParams(query='Pareto', scope=SummarySearchScope.ALL, anchor_day=day)))
        assert any('WLD summary/search review' in r.text for r in search.search(SummarySearchParams(query='coalescing', scope=SummarySearchScope.ALL, anchor_day=day)))
        assert any('Email handling' in r.text for r in search.search(SummarySearchParams(query='email', scope=SummarySearchScope.ALL, anchor_day=day)))

        exported = export_audit_bundle(storage, tmp_path / 'exports', AuditExportOptions(start_day=day, end_day=day), config=AppConfig(db_path=str(tmp_path / 'w.db')))
        trows = [json.loads(line) for line in (exported.output_dir / 'task_clusters.jsonl').read_text().splitlines() if line.strip()]
        lrows = [json.loads(line) for line in (exported.output_dir / 'summary_task_links.jsonl').read_text().splitlines() if line.strip()]
        drows = [json.loads(line) for line in (exported.output_dir / 'daily_summaries.jsonl').read_text().splitlines() if line.strip()]
        assert len(trows) == 3
        assert len(lrows) >= 3
        assert drows[0]['generated_from_task_clusters'] is True
        assert isinstance(drows[0]['structured_payload_json'], dict)
        by_title = {row["title"]: row for row in trows}
        email_ev = by_title["Email handling"]["evidence_json"]
        wld_ev = by_title["WLD summary/search review"]["evidence_json"]
        matlab_ev = by_title["MATLAB genetic optimization"]["evidence_json"]
        assert "LFM_SMASH_plotAll.m" not in email_ev.get("files", [])
        assert "OptimHistory_110kts.txt" not in email_ev.get("files", [])
        assert "Genetic Algorithm" not in email_ev.get("concepts", [])
        assert "LFM_SMASH_plotAll.m" not in wld_ev.get("files", [])
        assert "OptimHistory_110kts.txt" not in wld_ev.get("files", [])
        assert "Final Results Plots" not in wld_ev.get("windows", [])
        assert "outlook.exe" not in matlab_ev.get("apps", [])
    finally:
        storage.close()
