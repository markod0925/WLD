from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .activity_extraction import ActivityEntityDraft
from .models import SummaryRecord

TASK_MATLAB = "MATLAB genetic optimization"
TASK_WLD = "WLD summary/search review"
TASK_EMAIL = "Email handling"

_FILE_RE = re.compile(r"([A-Za-z]:[\\/][^\n\r]+?\.(?:m|txt|xlsx)|[^\s\\/]+\.(?:m|txt|xlsx))", re.IGNORECASE)


@dataclass(slots=True)
class StructuredTaskEvidence:
    payload: dict[str, Any]
    primary_task_label: str | None
    primary_activity_type: str | None
    is_blocked: bool
    is_low_value: bool
    noise_reason: str | None
    confidence: float
    entities: list[ActivityEntityDraft]


def extract_task_evidence(summary: SummaryRecord) -> StructuredTaskEvidence:
    source = summary.summary_json if isinstance(summary.summary_json, dict) else {}
    ctx = source.get("source_context") if isinstance(source.get("source_context"), dict) else {}
    process = str(ctx.get("process_name", "")).strip().lower()
    window = str(ctx.get("window_title", "")).strip()
    text = f"{summary.summary_text}\n{window}\n{source}".lower()
    blocked = bool(ctx.get("blocked")) or ("blocked activity" in text or "content unavailable" in text)

    files = _extract_files(f"{window}\n{summary.summary_text}")
    concepts: list[str] = []
    windows: list[str] = []
    noise: list[str] = []
    task = None
    activity = None
    confidence = 0.2
    noise_reason = None
    low_value = False

    matlab_hits = [k for k in ["genetic algorithm", "pareto", "fitness", "average spread", "average distance", "bode and eigenvalues", "optimal solution selection", "final results plots", "boxplot of pareto solutions"] if k in text]
    wld_hits = [k for k in ["worklog diary", "wld", "summary", "summaries", "search", "merge", "coalescing", "task representation", "daily summary", "chatgpt"] if k in text]
    email_hits = [k for k in ["outlook", "inbox", "posta in arrivo", "email", "mail", "correspondence"] if k in text]

    if "unknown.exe" in process and ("manual flush" in text or summary.end_ts - summary.start_ts < 15):
        low_value, noise_reason, activity = True, "unknown_process_short_duration", "generic_app_switching_noise"
    elif "searchhost.exe" in process:
        low_value, noise_reason, activity = True, "searchhost", "system_noise"
    elif blocked and not (wld_hits or matlab_hits or email_hits):
        low_value, noise_reason, activity = True, "blocked_content", "blocked_noise"

    if not low_value:
        if "outlook.exe" in process or email_hits:
            task, activity, confidence = TASK_EMAIL, "communication", 0.9
        elif "matlab.exe" in process or "matlabwindow.exe" in process or matlab_hits or any("optimhistory_" in f.lower() or "varie.xlsx" in f.lower() for f in files):
            task, confidence = TASK_MATLAB, 0.92
            if "genetic algorithm" in text:
                activity = "optimization_run"
            elif any(x in text for x in ["pareto", "fitness", "average spread", "average distance", "objective"]):
                activity = "result_analysis"
            elif any(x in text for x in ["bode and eigenvalues", "final results plots", "boxplot of pareto solutions"]):
                activity = "plot_review"
            elif any(f.lower().endswith(".m") for f in files):
                activity = "code_editing"
            elif ("notepad.exe" in process or "excel.exe" in process) and files:
                activity = "file_review"
            else:
                activity = "analysis"
        elif ("wld.exe" in process or "chrome.exe" in process or "msedge.exe" in process or "lm studio" in text) and wld_hits:
            task, activity, confidence = TASK_WLD, "software_debugging", 0.88

    concepts.extend(sorted(set(_concepts_from_text(text))))
    if "genetic algorithm" in text:
        windows.append("Genetic Algorithm")
    if "bode and eigenvalues" in text:
        windows.append("Bode and Eigenvalues")
    if "optimal solution selection" in text:
        windows.append("Optimal Solution Selection")
    if "final results plots" in text:
        windows.append("Final Results Plots")
    if "worklog diary summaries" in text:
        windows.append("WorkLog Diary Summaries")
    if low_value and noise_reason:
        noise.append(noise_reason)

    entities = [ActivityEntityDraft("app", process, process, "process_name", process, "observed", 1.0, {})] if process else []
    for f in files:
        entities.append(ActivityEntityDraft("file", f, _canon_file(f), "window_title", window, "observed", 0.9, {}))
    for c in concepts:
        entities.append(ActivityEntityDraft("concept", c, c.lower(), "window_title", window, "observed", 0.85, {}))
    for w in windows:
        entities.append(ActivityEntityDraft("window", w, w.lower(), "window_title", window, "observed", 0.9, {}))

    payload = {
        "schema_version": 1,
        "task_candidates": [{"label": task, "confidence": confidence, "evidence": sorted(set(matlab_hits + wld_hits + email_hits + files))}] if task else [],
        "primary_activity_type": activity,
        "artifacts": files,
        "windows": windows,
        "apps": [process] if process else [],
        "concepts": concepts,
        "supporting_activities": [],
        "noise": noise,
        "blocked": blocked,
        "low_value": low_value,
        "confidence": confidence,
    }
    return StructuredTaskEvidence(payload, task, activity, blocked, low_value, noise_reason, confidence, entities)


def _extract_files(text: str) -> list[str]:
    return sorted(set(m.group(1).strip(" -*") for m in _FILE_RE.finditer(text)))


def _canon_file(value: str) -> str:
    return value.strip().lstrip("*").replace("\\", "/")


def _concepts_from_text(text: str) -> list[str]:
    pairs = {
        "pareto front": "Pareto Front", "genetic algorithm": "Genetic Algorithm", "fitness": "Fitness",
        "average spread": "Average Spread", "average distance": "Average Distance", "bode": "Bode",
        "eigenvalues": "Eigenvalues", "search": "summary search", "merge": "merge", "blocked activity": "blocked activity",
    }
    return [label for key, label in pairs.items() if key in text]
