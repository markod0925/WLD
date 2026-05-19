from __future__ import annotations

from datetime import date

from worklog_diary.core.models import SummaryRecord
from worklog_diary.core.task_daily_summary import build_daily_summary_from_task_clusters


def _s(low=False, reason=None):
    return SummaryRecord(id=1, job_id=1, start_ts=1.0, end_ts=2.0, summary_text='x', summary_json={'is_low_value': low, 'noise_reason': reason}, created_ts=1.0)


def test_build_payload_and_render() -> None:
    day = date(2026,5,16)
    clusters = [
        {'id':1,'title':'MATLAB genetic optimization','task_type':'engineering_analysis','status':'active','start_ts':1.0,'end_ts':100.0,'confidence':0.9,'evidence_json':{'files':['OptimHistory_110kts.txt','LFM_SMASH_plotAll.m'],'windows':['Genetic Algorithm'],'apps':['matlab.exe'],'concepts':['Pareto Front'],'activity_types':['result_analysis'],'linked_summary_ids':[1,2]}},
        {'id':2,'title':'WLD summary/search review','task_type':'software_debugging','status':'active','start_ts':110.0,'end_ts':160.0,'confidence':0.8,'evidence_json':{'files':[],'windows':['WorkLog Diary Summaries'],'apps':['wld.exe','chrome.exe'],'concepts':['summary','search','merge'],'activity_types':['analysis'],'linked_summary_ids':[3]}},
        {'id':3,'title':'Email handling','task_type':'communication','status':'minor','start_ts':170.0,'end_ts':180.0,'confidence':0.7,'evidence_json':{'apps':['outlook.exe'],'concepts':['email'],'linked_summary_ids':[4]}},
    ]
    out = build_daily_summary_from_task_clusters(day, clusters, [_s(True,'blocked_content'), _s(True,'searchhost')])
    assert out.payload['generated_from_task_clusters'] is True
    assert len(out.payload['main_tasks']) == 2
    assert len(out.payload['minor_tasks']) == 1
    text = out.summary_text
    assert 'Main tasks' in text and 'Minor tasks' in text
    assert 'MATLAB genetic optimization' in text and 'WLD summary/search review' in text and 'Email handling' in text
    assert 'Ignored / low-value evidence' in text


def test_fallback_no_clusters() -> None:
    out = build_daily_summary_from_task_clusters(date(2026,5,16), [], [_s(True,'unknown_process_short_duration')])
    assert out.generated_from_task_clusters is False
    assert out.payload['fallback_reason'] == 'no_task_clusters'


def test_noise_counts_db_fields_precedence_and_blocked_fallback() -> None:
    class S:
        def __init__(self, *, is_low_value=None, noise_reason=None, is_blocked=None, summary_json=None):
            self.id = 1
            self.job_id = 1
            self.start_ts = 1.0
            self.end_ts = 2.0
            self.summary_text = "x"
            self.summary_json = summary_json or {}
            self.created_ts = 1.0
            self.is_low_value = is_low_value
            self.noise_reason = noise_reason
            self.is_blocked = is_blocked

    day = date(2026, 5, 16)
    out = build_daily_summary_from_task_clusters(
        day,
        [],
        [
            S(is_low_value=1, noise_reason="blocked_content", is_blocked=0, summary_json={}),
            S(is_low_value=None, noise_reason=None, is_blocked=None, summary_json={"is_low_value": True, "noise_reason": "searchhost"}),
            S(is_low_value=1, noise_reason="unknown_process_short_duration", is_blocked=1, summary_json={"noise_reason": "blocked_content"}),
            S(is_low_value=None, noise_reason=None, is_blocked=1, summary_json={}),
        ],
    )
    counts = {i["reason"]: i["count"] for i in out.payload["ignored_noise"]}
    assert counts["blocked_content"] == 2
    assert counts["searchhost"] == 1
    assert counts["unknown_process_short_duration"] == 1


def test_daily_summary_email_minor_task_does_not_list_matlab_evidence() -> None:
    day = date(2026, 5, 16)
    clusters = [
        {'id': 1, 'title': 'MATLAB genetic optimization', 'task_type': 'engineering_analysis', 'status': 'active', 'start_ts': 1.0, 'end_ts': 100.0, 'confidence': 0.9, 'evidence_json': {'files': ['LFM_SMASH_plotAll.m'], 'windows': ['Final Results Plots'], 'apps': ['matlab.exe'], 'concepts': ['Genetic Algorithm'], 'activity_types': ['plot_review'], 'linked_summary_ids': [1]}},
        {'id': 3, 'title': 'Email handling', 'task_type': 'communication', 'status': 'minor', 'start_ts': 170.0, 'end_ts': 180.0, 'confidence': 0.7, 'evidence_json': {'apps': ['outlook.exe'], 'concepts': ['email correspondence'], 'windows': ['Posta in arrivo - Outlook'], 'linked_summary_ids': [2]}},
    ]
    out = build_daily_summary_from_task_clusters(day, clusters, [])
    email_task = out.payload["minor_tasks"][0]
    assert email_task["title"] == "Email handling"
    assert "outlook.exe" in email_task["evidence"]["apps"]
    assert "LFM_SMASH_plotAll.m" not in email_task["evidence"].get("files", [])
    assert "Final Results Plots" not in email_task["evidence"].get("windows", [])
