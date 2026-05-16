from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path

from worklog_diary.core.storage import SQLiteStorage
from worklog_diary.core.task_clustering import cluster_tasks_for_day


def _ts(day: date, h: int, m: int = 0) -> float:
    return datetime.combine(day, time(h, m)).timestamp()


def _insert(storage: SQLiteStorage, day: date, idx: int, process: str, window: str, text: str, *, task: str | None, activity: str | None, low=False, noise=None) -> int:
    st = _ts(day, 9, idx)
    en = st + 30
    job = storage.create_summary_job(start_ts=st, end_ts=en, status="succeeded")
    sid = storage.insert_summary(job_id=job, start_ts=st, end_ts=en, summary_text=text, summary_json={"source_context": {"process_name": process, "window_title": window}, "primary_task_label": task, "primary_activity_type": activity, "is_low_value": low, "noise_reason": noise})
    storage.update_event_summary_structured_fields(sid, structured_payload_json={"schema_version": 1}, primary_task_label=task, primary_activity_type=activity, is_blocked=False, is_low_value=low, noise_reason=noise, confidence=0.8)
    ents = []
    for token in [process, window, text]:
        ents.append(type('E', (), dict(entity_type='concept', entity_value=token, entity_normalized=token.lower(), source_kind='window_title', source_ref=window, evidence_kind='observed', confidence=0.8, attributes={})))
    # use real draft class via extractor path would be heavier; minimal compatible object
    storage.replace_activity_entities_for_summary(day=day, start_ts=st, end_ts=en, summary_id=sid, entities=ents)  # type: ignore[arg-type]
    return sid


def test_task_clustering_end_to_end(tmp_path: Path) -> None:
    storage = SQLiteStorage(str(tmp_path / 'w.db'))
    day = date(2026, 5, 2)
    try:
        _insert(storage, day, 1, 'matlab.exe', 'Genetic Algorithm', 'Pareto Fitness', task='MATLAB genetic optimization', activity='optimization_run')
        _insert(storage, day, 2, 'matlabwindow.exe', 'Bode and Eigenvalues', 'plot', task='MATLAB genetic optimization', activity='plot_review')
        _insert(storage, day, 3, 'notepad.exe', 'OptimHistory_110kts.txt - Notepad', 'review', task='MATLAB genetic optimization', activity='file_review')
        _insert(storage, day, 4, 'excel.exe', 'Varie.xlsx', 'optimization context', task='MATLAB genetic optimization', activity='file_review')
        _insert(storage, day, 5, 'wld.exe', 'WorkLog Diary Summaries', 'summary merge search defect', task='WLD summary/search review', activity='software_debugging')
        _insert(storage, day, 6, 'chrome.exe', 'ChatGPT', 'WLD summary search merge', task='WLD summary/search review', activity='analysis')
        _insert(storage, day, 7, 'outlook.exe', 'Posta in arrivo - Outlook', 'email correspondence', task='Email handling', activity='communication')
        _insert(storage, day, 8, 'chrome.exe', 'blocked activity', 'content unavailable', task=None, activity='blocked_noise', low=True, noise='blocked_content')

        res = cluster_tasks_for_day(day, storage=storage)
        assert res.cluster_count == 3
        clusters = storage.list_task_clusters_for_day(day)
        titles = {c['title'] for c in clusters}
        assert {'MATLAB genetic optimization', 'WLD summary/search review', 'Email handling'} <= titles

        matlab = [c for c in clusters if c['title'] == 'MATLAB genetic optimization'][0]
        assert matlab['task_type'] == 'engineering_analysis'
        assert 'worked on matlab genetic optimization' in matlab['summary_text'].lower()
        assert matlab['evidence_json']['linked_summary_ids']

        links = storage.list_summary_task_links_for_day(day)
        assert links

        # idempotent
        res2 = cluster_tasks_for_day(day, storage=storage)
        clusters2 = storage.list_task_clusters_for_day(day)
        links2 = storage.list_summary_task_links_for_day(day)
        assert res2.cluster_count == len(clusters2) == len(clusters)
        assert len(links2) == len(links)
    finally:
        storage.close()
