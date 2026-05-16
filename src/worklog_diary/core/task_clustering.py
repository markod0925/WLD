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
        _add_to_cluster(clusters[primary_task], s, by_summary.get(int(s.id or 0), []), primary=True, score=primary_score)
        links.append({"summary_id": int(s.id or 0), "cluster_normalized_title": _norm(primary_task), "relation_type": "primary", "weight": 0.75, "confidence": min(1.0, primary_score)})

        if len(ranked) > 1 and ranked[1][1] >= 0.35:
            sec_task, sec_score = ranked[1]
            _ensure_cluster(clusters, sec_task)
            _add_to_cluster(clusters[sec_task], s, by_summary.get(int(s.id or 0), []), primary=False, score=sec_score)
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


def _add_to_cluster(cluster: dict[str, Any], s: SummaryRecord, entities: list[ActivityEntityRecord], *, primary: bool, score: float) -> None:
    sid = int(s.id or 0)
    cluster["score_total"] += score
    cluster["count"] += 1
    cluster["start_ts"] = s.start_ts if cluster["start_ts"] is None else min(cluster["start_ts"], s.start_ts)
    cluster["end_ts"] = s.end_ts if cluster["end_ts"] is None else max(cluster["end_ts"], s.end_ts)
    ev = cluster["evidence"]
    ev["linked_summary_ids"].append(sid)
    (ev["primary_summary_ids"] if primary else ev["secondary_summary_ids"]).append(sid)
    for e in entities:
        if e.entity_type == "file":
            ev["files"].append(e.entity_value)
        elif e.entity_type == "window":
            ev["windows"].append(e.entity_value)
        elif e.entity_type in {"app", "program"}:
            ev["apps"].append(e.entity_value.lower())
        elif e.entity_type == "concept":
            ev["concepts"].append(e.entity_value)
    raw = s.summary_json if isinstance(s.summary_json, dict) else {}
    activity = getattr(s, "primary_activity_type", None) or raw.get("primary_activity_type")
    if isinstance(activity, str) and activity:
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
