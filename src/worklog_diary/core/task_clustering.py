from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import Any

from .models import ActivityEntityRecord, SummaryRecord
from .storage import SQLiteStorage
from .task_evidence_extraction import TASK_EMAIL, TASK_MATLAB, TASK_WLD

_LOGGER = logging.getLogger(__name__)

@dataclass(slots=True)
class TaskClusteringResult:
    cluster_count: int
    link_count: int


MATLAB_WINDOW_PATTERNS = [
    "genetic algorithm",
    "bode and eigenvalues",
    "start: bode and eigenvalues",
    "optimal solution selection",
    "final results plots",
    "boxplot of pareto solutions",
    "boxplot of (actual) pareto solutions",
    "desired optimal solution",
    "matlab r2024b",
]
MATLAB_CONCEPT_PATTERNS = [
    "genetic algorithm", "pareto", "pareto front", "pareto solutions", "fitness", "average spread",
    "average distance", "objective", "bode", "eigenvalues", "body roll rate", "body pitch rate",
    "optimal solution", "numerical optimization", "smash",
]
WLD_CONCEPT_PATTERNS = [
    "worklog diary", "wld", "summary", "summaries", "daily summary", "search", "search defect", "merge",
    "coalescing", "task representation", "task cluster", "audit export", "activity_entities",
]
EMAIL_CONCEPT_PATTERNS = ["email", "mail", "inbox", "posta in arrivo", "correspondence", "message", "reply", "sent mail"]


def cluster_tasks_for_day(day: date, *, storage: SQLiteStorage) -> TaskClusteringResult:
    summaries = storage.list_summaries_for_day(day, limit=2000)
    entities = storage.list_activity_entities_for_day(day)
    clusters, links = build_task_clusters_for_day(summaries, entities)
    c_count, l_count = storage.replace_task_clusters_for_day(day=day, clusters=clusters, links=links)
    _LOGGER.info(
        "event=task_clustering day=%s source_summary_count=%s cluster_count=%s link_count=%s cluster_titles=%s",
        day.isoformat(),
        len(summaries),
        c_count,
        l_count,
        [str(c.get("title")) for c in clusters],
    )
    return TaskClusteringResult(c_count, l_count)


def build_task_clusters_for_day(
    summaries: list[SummaryRecord],
    entities: list[ActivityEntityRecord],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    by_summary: dict[int, list[ActivityEntityRecord]] = {}
    for e in entities:
        if e.summary_id is not None:
            by_summary.setdefault(int(e.summary_id), []).append(e)

    clusters: dict[str, dict[str, Any]] = {}
    links: list[dict[str, object]] = []
    for s in summaries:
        task_scores = _score_summary_tasks(s, by_summary.get(int(s.id or 0), []))
        if not task_scores:
            continue
        ranked = sorted(task_scores.items(), key=lambda kv: kv[1], reverse=True)
        primary_task, primary_score = ranked[0]
        if primary_score < 0.5:
            continue
        _ensure_cluster(clusters, primary_task)
        _add_to_cluster(clusters[primary_task], s, by_summary.get(int(s.id or 0), []), task_label=primary_task, primary=True, score=primary_score)
        links.append({"summary_id": int(s.id or 0), "cluster_normalized_title": _norm(primary_task), "relation_type": "primary", "weight": 0.75, "confidence": min(1.0, primary_score)})

        if len(ranked) > 1 and ranked[1][1] >= 0.35:
            sec_task, sec_score = ranked[1]
            _ensure_cluster(clusters, sec_task)
            _add_to_cluster(clusters[sec_task], s, by_summary.get(int(s.id or 0), []), task_label=sec_task, primary=False, score=sec_score)
            links.append({"summary_id": int(s.id or 0), "cluster_normalized_title": _norm(sec_task), "relation_type": "secondary", "weight": 0.25, "confidence": min(1.0, sec_score)})

    output_clusters: list[dict[str, object]] = []
    for c in sorted(clusters.values(), key=lambda x: x["title"]):
        evidence = c["evidence"]
        evidence["files"].sort()
        evidence["windows"].sort()
        evidence["apps"].sort()
        evidence["concepts"].sort()
        evidence["activity_types"].sort()
        evidence["linked_summary_ids"].sort()
        evidence["primary_summary_ids"].sort()
        evidence["secondary_summary_ids"].sort()
        evidence["noise_summary_ids"].sort()
        output_clusters.append(
            {
                "title": c["title"],
                "normalized_title": c["normalized_title"],
                "task_type": c["task_type"],
                "status": c["status"],
                "start_ts": c["start_ts"],
                "end_ts": c["end_ts"],
                "summary_text": _cluster_text(c["title"], evidence),
                "evidence_json": evidence,
                "confidence": round(min(1.0, c["score_total"] / max(1, c["count"])), 3),
            }
        )
    return output_clusters, links


def _score_summary_tasks(summary: SummaryRecord, entities: list[ActivityEntityRecord]) -> dict[str, float]:
    raw = summary.summary_json if isinstance(summary.summary_json, dict) else {}
    text = (summary.summary_text + " " + json.dumps(raw)).lower()
    label = str(getattr(summary, "primary_task_label", None) or raw.get("primary_task_label") or "").strip()
    if not label:
        label = ""
    scores = {TASK_MATLAB: 0.0, TASK_WLD: 0.0, TASK_EMAIL: 0.0}
    if label in scores:
        scores[label] += 0.9
    if _is_low_value(raw):
        if "blocked_content" in text or "unknown_process_short_duration" in text or "searchhost" in text:
            return {}
    for e in entities:
        v = (e.entity_value or "").lower()
        if e.entity_type in {"file", "window", "concept", "app", "program"}:
            if any(k in v for k in ["optimhistory_", "lfm_smash_plotall.m", "main.m", "varie.xlsx", "genetic algorithm", "pareto", "fitness", "bode", "eigenvalues", "optimal solution selection", "final results plots"]):
                scores[TASK_MATLAB] += 0.2
            if any(k in v for k in ["worklog diary", "wld", "summary", "search", "merge", "coalescing", "chatgpt", "lm studio"]):
                scores[TASK_WLD] += 0.2
            if any(k in v for k in ["outlook", "inbox", "posta in arrivo", "email", "mail", "correspondence"]):
                scores[TASK_EMAIL] += 0.25
    if "outlook.exe" in text:
        scores[TASK_EMAIL] += 0.6
    return scores


def _is_low_value(raw: dict[str, Any]) -> bool:
    return bool(raw.get("is_low_value")) or bool(raw.get("low_value"))


def _ensure_cluster(clusters: dict[str, dict[str, Any]], title: str) -> None:
    if title in clusters:
        return
    task_type = "engineering_analysis" if title == TASK_MATLAB else "software_debugging" if title == TASK_WLD else "communication"
    status = "minor" if title == TASK_EMAIL else "active"
    clusters[title] = {
        "title": title,
        "normalized_title": _norm(title),
        "task_type": task_type,
        "status": status,
        "start_ts": None,
        "end_ts": None,
        "score_total": 0.0,
        "count": 0,
        "evidence": {"schema_version": 1, "linked_summary_ids": [], "primary_summary_ids": [], "secondary_summary_ids": [], "noise_summary_ids": [], "files": [], "windows": [], "apps": [], "concepts": [], "activity_types": [], "time_range": {"start_ts": None, "end_ts": None}, "confidence": 0.0},
    }


def _add_to_cluster(cluster: dict[str, Any], s: SummaryRecord, entities: list[ActivityEntityRecord], *, task_label: str, primary: bool, score: float) -> None:
    sid = int(s.id or 0)
    cluster["score_total"] += score
    cluster["count"] += 1
    cluster["start_ts"] = s.start_ts if cluster["start_ts"] is None else min(cluster["start_ts"], s.start_ts)
    cluster["end_ts"] = s.end_ts if cluster["end_ts"] is None else max(cluster["end_ts"], s.end_ts)
    ev = cluster["evidence"]
    ev["linked_summary_ids"].append(sid)
    (ev["primary_summary_ids"] if primary else ev["secondary_summary_ids"]).append(sid)
    filtered = _filter_evidence_for_task(task_label, s, entities)
    ev["files"].extend(filtered["files"])
    ev["windows"].extend(filtered["windows"])
    ev["apps"].extend(filtered["apps"])
    ev["concepts"].extend(filtered["concepts"])
    raw = s.summary_json if isinstance(s.summary_json, dict) else {}
    activity = getattr(s, "primary_activity_type", None) or raw.get("primary_activity_type")
    if isinstance(activity, str) and activity and activity in filtered["activity_types"]:
        ev["activity_types"].append(activity)
    ev["time_range"]["start_ts"] = cluster["start_ts"]
    ev["time_range"]["end_ts"] = cluster["end_ts"]
    ev["confidence"] = round(min(1.0, cluster["score_total"] / max(1, cluster["count"])), 3)
    for k in ("files", "windows", "apps", "concepts", "activity_types"):
        ev[k] = sorted(set(ev[k]))


def _cluster_text(title: str, evidence: dict[str, Any]) -> str:
    if title == TASK_MATLAB:
        return "Worked on MATLAB genetic optimization, including GA/Pareto analysis, plot review, and related artifact work."
    if title == TASK_WLD:
        return "Reviewed WorkLog Diary summaries/search behavior and investigated merge/task-representation issues."
    return "Handled email correspondence in Outlook."


def _norm(v: str) -> str:
    return v.strip().lower()


def _filter_evidence_for_task(task_label: str, summary: SummaryRecord, entities: list[ActivityEntityRecord]) -> dict[str, list[str]]:
    raw = summary.summary_json if isinstance(summary.summary_json, dict) else {}
    summary_text = f"{summary.summary_text} {json.dumps(raw)}".lower()
    values = {"files": [], "windows": [], "apps": [], "concepts": [], "activity_types": []}
    has_varie_file = any((e.entity_type == "file" and (e.entity_value or "").strip().lower().endswith("varie.xlsx")) for e in entities)
    has_optimhistory_file = any((e.entity_type == "file" and (e.entity_value or "").strip().lower().startswith("optimhistory_")) for e in entities)
    matlab_context = any(k in summary_text for k in ["matlab", "smash", "optimization", "pareto", "genetic algorithm", "bode"])
    wld_context = any(k in summary_text for k in ["worklog diary", "wld", "summary", "search", "merge", "coalescing", "task cluster", "chatgpt"])
    email_context = any(k in summary_text for k in EMAIL_CONCEPT_PATTERNS + ["outlook"])
    for e in entities:
        val = (e.entity_value or "").strip()
        low = val.lower()
        if not val:
            continue
        if task_label == TASK_MATLAB:
            if e.entity_type == "file":
                if low.endswith(".m") and matlab_context:
                    values["files"].append(val)
                elif ("lfm_smash" in low and low.endswith(".m")) or low.startswith("main.m"):
                    values["files"].append(val)
                elif low.startswith("optimhistory_") and low.endswith(".txt"):
                    values["files"].append(val)
                elif low.endswith("varie.xlsx"):
                    values["files"].append(val)
            elif e.entity_type == "window" and any(k in low for k in MATLAB_WINDOW_PATTERNS):
                values["windows"].append(val)
            elif e.entity_type in {"app", "program"}:
                if low in {"matlab.exe", "matlabwindow.exe"}:
                    values["apps"].append(low)
                elif low == "notepad.exe" and (has_optimhistory_file or any(f.lower().startswith("optimhistory_") for f in values["files"])):
                    values["apps"].append(low)
                elif low == "excel.exe" and (has_varie_file or any(f.lower().endswith("varie.xlsx") for f in values["files"])):
                    values["apps"].append(low)
                elif low == "explorer.exe" and values["files"]:
                    values["apps"].append(low)
            elif e.entity_type == "concept" and any(k in low for k in MATLAB_CONCEPT_PATTERNS):
                values["concepts"].append(val)
        elif task_label == TASK_WLD:
            if e.entity_type == "file" and ("wld" in low or "worklog" in low):
                values["files"].append(val)
            elif e.entity_type == "window":
                if any(k in low for k in ["worklog diary", "worklog diary summaries", "wld"]):
                    values["windows"].append(val)
                elif "chatgpt" in low and wld_context:
                    values["windows"].append(val)
                elif "lm studio" in low and wld_context:
                    values["windows"].append(val)
            elif e.entity_type in {"app", "program"}:
                if low == "wld.exe":
                    values["apps"].append(low)
                elif low in {"chrome.exe", "msedge.exe"} and wld_context:
                    values["apps"].append(low)
                elif low == "lm studio.exe" and wld_context:
                    values["apps"].append(low)
            elif e.entity_type == "concept" and any(k in low for k in WLD_CONCEPT_PATTERNS):
                values["concepts"].append(val)
        elif task_label == TASK_EMAIL:
            if e.entity_type == "window" and any(k in low for k in ["outlook", "posta in arrivo", "inbox", "email"]):
                values["windows"].append(val)
            elif e.entity_type in {"app", "program"} and low == "outlook.exe":
                values["apps"].append(low)
            elif e.entity_type == "concept" and any(k in low for k in EMAIL_CONCEPT_PATTERNS):
                values["concepts"].append(val)
    activity = str(getattr(summary, "primary_activity_type", None) or raw.get("primary_activity_type") or "")
    if task_label == TASK_MATLAB and activity in {"optimization_run", "result_analysis", "plot_review", "code_editing", "file_review"}:
        if activity != "file_review" or values["files"]:
            values["activity_types"].append(activity)
    elif task_label == TASK_WLD and activity in {"software_debugging", "analysis", "file_review"}:
        if activity != "file_review" or values["files"]:
            values["activity_types"].append(activity)
    elif task_label == TASK_EMAIL and activity == "communication":
        values["activity_types"].append(activity)
    for k in values:
        values[k] = sorted(set(values[k]))
    return values
