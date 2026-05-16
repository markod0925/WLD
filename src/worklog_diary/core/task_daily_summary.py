from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from .models import SummaryRecord


@dataclass(slots=True)
class DailyTaskSummary:
    summary_text: str
    payload: dict[str, Any]
    generated_from_task_clusters: bool


def build_daily_summary_from_task_clusters(
    day: date,
    task_clusters: list[dict[str, object]],
    source_summaries: list[SummaryRecord],
    *,
    ignored_noise: list[dict[str, object]] | None = None,
) -> DailyTaskSummary:
    if not task_clusters:
        payload = {
            "schema_version": 1,
            "generated_from_task_clusters": False,
            "fallback_reason": "no_task_clusters",
            "day": day.isoformat(),
            "main_tasks": [],
            "minor_tasks": [],
            "ignored_noise": ignored_noise if ignored_noise is not None else _noise_counts(source_summaries),
        }
        return DailyTaskSummary(
            summary_text=f"Daily Summary — {day.isoformat()}\n\nNo meaningful task clusters were detected.",
            payload=payload,
            generated_from_task_clusters=False,
        )

    items = [_normalize_cluster(item) for item in task_clusters]
    main = [x for x in items if x["status"] == "active"]
    minor = [x for x in items if x["status"] == "minor"]
    main.sort(key=lambda x: (-_duration(x), x["time_range"]["start_ts"] or 0.0, x["title"]))
    minor.sort(key=lambda x: (-_duration(x), x["time_range"]["start_ts"] or 0.0, x["title"]))

    payload = {
        "schema_version": 1,
        "generated_from_task_clusters": True,
        "day": day.isoformat(),
        "main_tasks": main,
        "minor_tasks": minor,
        "ignored_noise": ignored_noise if ignored_noise is not None else _noise_counts(source_summaries),
    }
    return DailyTaskSummary(
        summary_text=render_task_centric_daily_summary(payload),
        payload=payload,
        generated_from_task_clusters=True,
    )


def render_task_centric_daily_summary(payload: dict[str, Any]) -> str:
    day = payload.get("day", "")
    lines = [f"Daily Summary — {day}", "", "Main tasks", ""]
    for idx, task in enumerate(payload.get("main_tasks", []), start=1):
        lines.extend([f"{idx}. {task['title']}", "   Outcome:"])
        for bullet in task.get("outcome", []):
            lines.append(f"   - {bullet}")
        ev = task.get("evidence", {})
        lines.append("   Evidence:")
        if ev.get("files"):
            lines.append(f"   - Files: {', '.join(ev['files'])}.")
        if ev.get("windows"):
            lines.append(f"   - Windows: {', '.join(ev['windows'])}.")
        if ev.get("apps"):
            lines.append(f"   - Apps: {', '.join(ev['apps'])}.")
        if ev.get("concepts"):
            lines.append(f"   - Concepts: {', '.join(ev['concepts'])}.")
        lines.append("")
    if payload.get("minor_tasks"):
        lines.extend(["Minor tasks", ""])
        for task in payload["minor_tasks"]:
            first = task.get("outcome", ["supporting activity"])[0]
            lines.append(f"- {task['title']}: {first}")
        lines.append("")
    if payload.get("ignored_noise"):
        lines.extend(["Ignored / low-value evidence", ""])
        for item in payload["ignored_noise"]:
            lines.append(f"- {item['reason']}: {item['count']} summaries.")
    return "\n".join(lines).strip()


def _normalize_cluster(row: dict[str, object]) -> dict[str, Any]:
    evidence = row.get("evidence_json") if isinstance(row.get("evidence_json"), dict) else {}
    cleaned = {k: sorted(set(v for v in evidence.get(k, []) if isinstance(v, str) and v.lower() not in {"unknown.exe", "searchhost.exe", "blocked activity"})) for k in ("files", "windows", "apps", "concepts", "activity_types")}
    title = str(row.get("title", "Task"))
    return {
        "task_cluster_id": int(row.get("id", 0)),
        "title": title,
        "task_type": row.get("task_type"),
        "status": row.get("status") or ("minor" if title == "Email handling" else "active"),
        "time_range": {"start_ts": row.get("start_ts"), "end_ts": row.get("end_ts")},
        "outcome": _outcomes(title, cleaned),
        "evidence": cleaned,
        "linked_summary_ids": sorted(set(evidence.get("linked_summary_ids", []))) if isinstance(evidence.get("linked_summary_ids", []), list) else [],
        "confidence": float(row.get("confidence", 0.0) or 0.0),
    }


def _outcomes(title: str, evidence: dict[str, list[str]]) -> list[str]:
    if title == "MATLAB genetic optimization":
        out = ["Worked on MATLAB genetic optimization and result analysis.", "Reviewed GA/Pareto/Fitness/Bode/Eigenvalue outputs."]
        if evidence.get("files"):
            out.append(f"Reviewed or edited related files such as {', '.join(evidence['files'][:2])}.")
        return out
    if title == "WLD summary/search review":
        return ["Reviewed WorkLog Diary summaries and task representation.", "Investigated search, merge, or coalescing behavior."]
    if title == "Email handling":
        return ["Handled email correspondence in Outlook."]
    return ["Advanced task work based on linked evidence."]


def _noise_counts(summaries: list[SummaryRecord]) -> list[dict[str, object]]:
    counts: dict[str, int] = {}
    for s in summaries:
        raw = s.summary_json if isinstance(s.summary_json, dict) else {}
        db_low = getattr(s, "is_low_value", None)
        db_blocked = getattr(s, "is_blocked", None)
        db_reason = getattr(s, "noise_reason", None)
        json_low = bool(raw.get("is_low_value")) if isinstance(raw, dict) else False
        json_blocked = bool(raw.get("is_blocked")) if isinstance(raw, dict) else False
        json_reason = raw.get("noise_reason") if isinstance(raw, dict) else None
        is_low = bool(db_low) if db_low is not None else json_low
        is_blocked = bool(db_blocked) if db_blocked is not None else json_blocked
        reason = str(db_reason or json_reason or ("blocked_content" if is_blocked else "low_value"))
        if is_low or is_blocked:
            counts[reason] = counts.get(reason, 0) + 1
    return [{"reason": k, "count": counts[k]} for k in sorted(counts)]


def _duration(item: dict[str, Any]) -> float:
    t = item.get("time_range", {})
    st, en = t.get("start_ts"), t.get("end_ts")
    if isinstance(st, (int, float)) and isinstance(en, (int, float)):
        return max(0.0, float(en) - float(st))
    return 0.0
