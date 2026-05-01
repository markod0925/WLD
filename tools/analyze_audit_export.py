#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any, Callable


BUCKET_ORDER = ("excellent", "good", "weak", "poor")
ENTITY_FILE_TYPES = {"file_path", "file_name", "folder_path"}
TASK_ENTITY_HINTS = ("task", "ticket")
CONVERSATION_ENTITY_TYPES = {"conversation_subject", "mail_subject", "web_page_title"}


class AuditExportAnalysisError(RuntimeError):
    """Raised when an audit export cannot be analyzed."""


@dataclass(slots=True)
class BundleData:
    path: Path
    manifest: dict[str, Any]
    event_summaries: list[dict[str, Any]]
    daily_summaries: list[dict[str, Any]]
    activity_entities: list[dict[str, Any]]
    parser_coverage: list[dict[str, Any]]
    unknown_apps: list[dict[str, Any]]
    unknown_window_patterns: list[dict[str, Any]]
    low_confidence_entities: list[dict[str, Any]]
    evidence_quality_rows: list[dict[str, Any]]
    evidence_quality_summary: dict[str, Any]


@dataclass(slots=True)
class BundleMetrics:
    path: Path
    manifest: dict[str, Any]
    event_summary_count: int
    daily_summary_count: int
    activity_entity_count: int
    entities_per_summary: float
    evidence_bucket_counts: dict[str, int]
    average_evidence_quality_score: float
    degraded_payload_count: int
    unknown_app_count: int
    unclassified_window_title_count: int
    low_confidence_entity_count: int
    top_entity_types: list[tuple[str, int]]
    top_processes: list[tuple[str, int]]
    top_files: list[tuple[str, int, str]]
    top_task_candidates: list[tuple[str, int, str]]
    top_conversation_subjects: list[tuple[str, int, str]]
    top_unknown_processes: list[tuple[str, int]]
    top_unknown_window_patterns: list[tuple[str, int]]
    missing_daily_recap_days: list[str]
    weak_or_poor_days: list[str]
    raw_only_event_summary_count: int
    file_entity_count: int
    task_candidate_count: int
    conversation_entity_count: int
    date_range_start: str | None
    date_range_end: str | None


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - defensive
        raise AuditExportAnalysisError(f"Failed to read JSON file '{path}': {exc}") from exc
    if isinstance(data, dict):
        return data
    return {}


def _read_jsonl_file(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            stripped = line.strip()
            if not stripped:
                continue
            item = json.loads(stripped)
            if isinstance(item, dict):
                rows.append(item)
    except Exception as exc:  # pragma: no cover - defensive
        raise AuditExportAnalysisError(f"Failed to read JSONL file '{path}': {exc}") from exc
    return rows


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _bundle_days(rows: list[dict[str, Any]]) -> set[str]:
    days: set[str] = set()
    for row in rows:
        day = _safe_str(row.get("day"))
        if day:
            days.add(day)
            continue
        start_ts = row.get("start_ts")
        if isinstance(start_ts, (int, float)):
            from datetime import datetime

            days.add(datetime.fromtimestamp(float(start_ts)).date().isoformat())
    return days


def _format_number(value: int) -> str:
    return f"{value:,}"


def _format_signed_int(value: int) -> str:
    return f"{value:+d}"


def _format_signed_float(value: float) -> str:
    return f"{value:+.3f}"


def _format_bucket_counts(counts: dict[str, int]) -> str:
    parts = [f"{bucket} {_format_number(counts.get(bucket, 0))}" for bucket in BUCKET_ORDER]
    return ", ".join(parts)


def _format_ranked_items(items: list[tuple[str, int]], *, limit: int = 5) -> str:
    if not items:
        return "none"
    selected = items[:limit]
    return "; ".join(f"`{label}` ({_format_number(count)})" for label, count in selected)


def _format_ranked_items_with_type(items: list[tuple[str, int, str]], *, limit: int = 5) -> str:
    if not items:
        return "none"
    selected = items[:limit]
    return "; ".join(
        f"`{label}` ({_format_number(count)}) [{item_type}]"
        for label, count, item_type in selected
    )


def _rank_rows(
    rows: list[dict[str, Any]],
    *,
    predicate: Callable[[dict[str, Any]], bool],
    label_for_row: Callable[[dict[str, Any]], str],
    weight_for_row: Callable[[dict[str, Any]], int] | None = None,
    limit: int = 5,
) -> list[tuple[str, int]]:
    weight_for_row = weight_for_row or (lambda row: 1)
    counts: Counter[str] = Counter()
    for row in rows:
        if not predicate(row):
            continue
        label = _safe_str(label_for_row(row))
        if not label:
            continue
        counts[label] += max(1, _safe_int(weight_for_row(row), default=1))
    return counts.most_common(limit)


def _rank_rows_with_type(
    rows: list[dict[str, Any]],
    *,
    predicate: Callable[[dict[str, Any]], bool],
    label_for_row: Callable[[dict[str, Any]], str],
    type_for_row: Callable[[dict[str, Any]], str],
    weight_for_row: Callable[[dict[str, Any]], int] | None = None,
    limit: int = 5,
) -> list[tuple[str, int, str]]:
    weight_for_row = weight_for_row or (lambda row: 1)
    counts: Counter[tuple[str, str]] = Counter()
    for row in rows:
        if not predicate(row):
            continue
        label = _safe_str(label_for_row(row))
        if not label:
            continue
        item_type = _safe_str(type_for_row(row))
        counts[(label, item_type)] += max(1, _safe_int(weight_for_row(row), default=1))
    ranked = counts.most_common(limit)
    return [(label, count, item_type) for (label, item_type), count in ranked]


def _load_bundle(path: Path) -> BundleData:
    if not path.exists():
        raise AuditExportAnalysisError(f"Audit export folder not found: {path}")
    if not path.is_dir():
        raise AuditExportAnalysisError(f"Audit export path is not a directory: {path}")
    return BundleData(
        path=path,
        manifest=_read_json_file(path / "manifest.json"),
        event_summaries=_read_jsonl_file(path / "summaries.jsonl"),
        daily_summaries=_read_jsonl_file(path / "daily_summaries.jsonl"),
        activity_entities=_read_jsonl_file(path / "activity_entities.jsonl"),
        parser_coverage=_read_jsonl_file(path / "parser_coverage.jsonl"),
        unknown_apps=_read_jsonl_file(path / "unknown_apps.jsonl"),
        unknown_window_patterns=_read_jsonl_file(path / "unknown_window_patterns.jsonl"),
        low_confidence_entities=_read_jsonl_file(path / "low_confidence_entities.jsonl"),
        evidence_quality_rows=_read_jsonl_file(path / "evidence_quality.jsonl"),
        evidence_quality_summary=_read_json_file(path / "evidence_quality_summary.json"),
    )


def _sum_occurrences(rows: list[dict[str, Any]]) -> int:
    total = 0
    for row in rows:
        total += max(1, _safe_int(row.get("occurrence_count"), default=1))
    return total


def _count_by_evidence_flag(rows: list[dict[str, Any]], flag: str) -> int:
    return sum(1 for row in rows if bool(row.get(flag)))


def _build_top_unknown_processes(data: BundleData) -> list[tuple[str, int]]:
    if data.unknown_apps:
        return _rank_rows(
            data.unknown_apps,
            predicate=lambda row: True,
            label_for_row=lambda row: _safe_str(row.get("process_name") or row.get("normalized_process_name")),
            weight_for_row=lambda row: _safe_int(row.get("occurrence_count"), default=1),
            limit=5,
        )
    return _rank_rows(
        data.parser_coverage,
        predicate=lambda row: bool(row.get("unknown_app")),
        label_for_row=lambda row: _safe_str(row.get("process_name") or row.get("normalized_process_name")),
        limit=5,
    )


def _build_top_unknown_window_patterns(data: BundleData) -> list[tuple[str, int]]:
    if data.unknown_window_patterns:
        return _rank_rows(
            data.unknown_window_patterns,
            predicate=lambda row: True,
            label_for_row=lambda row: f"{_safe_str(row.get('process_name') or row.get('normalized_process_name'))}: {_safe_str(row.get('sample_window_title') or row.get('normalized_title_sample'))}",
            weight_for_row=lambda row: _safe_int(row.get("occurrence_count"), default=1),
            limit=5,
        )
    return _rank_rows(
        data.parser_coverage,
        predicate=lambda row: bool(row.get("unknown_app")),
        label_for_row=lambda row: f"{_safe_str(row.get('process_name') or row.get('normalized_process_name'))}: {_safe_str(row.get('window_title') or row.get('normalized_window_title'))}",
        limit=5,
    )


def analyze_bundle(data: BundleData) -> BundleMetrics:
    summary_days = _bundle_days(data.event_summaries)
    daily_days = _bundle_days(data.daily_summaries)
    evidence_days = _bundle_days(data.evidence_quality_rows)
    all_days = sorted(summary_days | daily_days | evidence_days)
    date_range_start = all_days[0] if all_days else None
    date_range_end = all_days[-1] if all_days else None

    event_summary_count = len(data.event_summaries)
    daily_summary_count = len(data.daily_summaries)
    activity_entity_count = len(data.activity_entities)
    entities_per_summary = round(activity_entity_count / event_summary_count, 2) if event_summary_count else 0.0

    evidence_quality_summary = data.evidence_quality_summary if isinstance(data.evidence_quality_summary, dict) else {}
    if data.evidence_quality_rows:
        bucket_counts = Counter(
            _safe_str(row.get("bucket") or "unknown")
            for row in data.evidence_quality_rows
        )
        average_score = round(
            mean(_safe_float(row.get("score"), default=0.0) for row in data.evidence_quality_rows),
            3,
        )
    else:
        bucket_counts = Counter(
            {
                _safe_str(bucket): _safe_int(count)
                for bucket, count in (evidence_quality_summary.get("bucket_counts") or {}).items()
            }
        )
        average_score = round(_safe_float(evidence_quality_summary.get("average_score"), default=0.0), 3)
    for bucket in BUCKET_ORDER:
        bucket_counts.setdefault(bucket, 0)

    degraded_payload_count = _count_by_evidence_flag(data.evidence_quality_rows, "degraded_payload")
    low_confidence_entity_count = len(data.low_confidence_entities)
    unknown_app_count = _sum_occurrences(data.unknown_apps) if data.unknown_apps else sum(
        1 for row in data.parser_coverage if bool(row.get("unknown_app"))
    )
    unclassified_window_title_count = _sum_occurrences(data.unknown_window_patterns) if data.unknown_window_patterns else sum(
        1 for row in data.parser_coverage if bool(row.get("unknown_app"))
    )

    top_entity_types = _rank_rows(
        data.activity_entities,
        predicate=lambda row: bool(_safe_str(row.get("entity_type"))),
        label_for_row=lambda row: _safe_str(row.get("entity_type")),
        limit=5,
    )
    top_processes = _rank_rows(
        data.activity_entities,
        predicate=lambda row: _safe_str(row.get("entity_type")) == "program",
        label_for_row=lambda row: _safe_str(row.get("entity_value") or row.get("entity_normalized")),
        limit=5,
    )
    top_files = _rank_rows_with_type(
        data.activity_entities,
        predicate=lambda row: _safe_str(row.get("entity_type")) in ENTITY_FILE_TYPES,
        label_for_row=lambda row: _safe_str(row.get("entity_value") or row.get("entity_normalized")),
        type_for_row=lambda row: _safe_str(row.get("entity_type")),
        limit=5,
    )
    top_task_candidates = _rank_rows_with_type(
        data.activity_entities,
        predicate=lambda row: any(hint in _safe_str(row.get("entity_type")).lower() for hint in TASK_ENTITY_HINTS),
        label_for_row=lambda row: _safe_str(row.get("entity_value") or row.get("entity_normalized")),
        type_for_row=lambda row: _safe_str(row.get("entity_type")),
        limit=5,
    )
    top_conversation_subjects = _rank_rows_with_type(
        data.activity_entities,
        predicate=lambda row: _safe_str(row.get("entity_type")) in CONVERSATION_ENTITY_TYPES,
        label_for_row=lambda row: _safe_str(row.get("entity_value") or row.get("entity_normalized")),
        type_for_row=lambda row: _safe_str(row.get("entity_type")),
        limit=5,
    )

    file_entity_count = sum(1 for row in data.activity_entities if _safe_str(row.get("entity_type")) in ENTITY_FILE_TYPES)
    task_candidate_count = sum(
        1 for row in data.activity_entities if any(hint in _safe_str(row.get("entity_type")).lower() for hint in TASK_ENTITY_HINTS)
    )
    conversation_entity_count = sum(1 for row in data.activity_entities if _safe_str(row.get("entity_type")) in CONVERSATION_ENTITY_TYPES)

    missing_daily_recap_days = sorted(summary_days - daily_days)
    raw_only_event_summary_count = sum(
        1
        for row in data.evidence_quality_rows
        if _safe_str(row.get("summary_kind") or "event") != "daily"
        and not bool(row.get("has_file_evidence"))
        and not bool(row.get("has_task_evidence"))
        and not bool(row.get("has_conversation_evidence"))
    )

    weak_or_poor_by_day: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "weak_or_poor": 0})
    for row in data.evidence_quality_rows:
        if _safe_str(row.get("summary_kind") or "event") == "daily":
            continue
        day = _safe_str(row.get("day"))
        if not day:
            continue
        bucket = _safe_str(row.get("bucket"))
        day_bucket = weak_or_poor_by_day[day]
        day_bucket["total"] += 1
        if bucket in {"weak", "poor"}:
            day_bucket["weak_or_poor"] += 1

    weak_or_poor_days: list[str] = []
    for day in sorted(weak_or_poor_by_day):
        total = weak_or_poor_by_day[day]["total"]
        weak_or_poor = weak_or_poor_by_day[day]["weak_or_poor"]
        if total and weak_or_poor / total >= 0.5:
            weak_or_poor_days.append(f"{day} ({weak_or_poor}/{total} weak/poor)")

    return BundleMetrics(
        path=data.path,
        manifest=data.manifest,
        event_summary_count=event_summary_count,
        daily_summary_count=daily_summary_count,
        activity_entity_count=activity_entity_count,
        entities_per_summary=entities_per_summary,
        evidence_bucket_counts=dict(bucket_counts),
        average_evidence_quality_score=average_score,
        degraded_payload_count=degraded_payload_count,
        unknown_app_count=unknown_app_count,
        unclassified_window_title_count=unclassified_window_title_count,
        low_confidence_entity_count=low_confidence_entity_count,
        top_entity_types=top_entity_types,
        top_processes=top_processes,
        top_files=top_files,
        top_task_candidates=top_task_candidates,
        top_conversation_subjects=top_conversation_subjects,
        top_unknown_processes=_build_top_unknown_processes(data),
        top_unknown_window_patterns=_build_top_unknown_window_patterns(data),
        missing_daily_recap_days=missing_daily_recap_days,
        weak_or_poor_days=weak_or_poor_days,
        raw_only_event_summary_count=raw_only_event_summary_count,
        file_entity_count=file_entity_count,
        task_candidate_count=task_candidate_count,
        conversation_entity_count=conversation_entity_count,
        date_range_start=date_range_start,
        date_range_end=date_range_end,
    )


def _recommendations(metrics: BundleMetrics) -> list[str]:
    recommendations: list[str] = []
    total_summaries = max(1, metrics.event_summary_count)
    total_quality_rows = max(1, metrics.event_summary_count + metrics.daily_summary_count)
    good_or_excellent = metrics.evidence_bucket_counts.get("good", 0) + metrics.evidence_bucket_counts.get("excellent", 0)
    weak_or_poor = metrics.evidence_bucket_counts.get("weak", 0) + metrics.evidence_bucket_counts.get("poor", 0)

    if metrics.raw_only_event_summary_count and metrics.raw_only_event_summary_count / total_summaries >= 0.4:
        recommendations.append(
            "Many event summaries still rely on process/window evidence only. Improve parser coverage and entity extraction for file, task, and conversation sources."
        )

    if metrics.top_unknown_processes and metrics.unknown_app_count:
        top_process, top_count = metrics.top_unknown_processes[0]
        if top_count / max(1, metrics.unknown_app_count) >= 0.5:
            recommendations.append(
                f"Unknown windows are concentrated in `{top_process}`. Add a specialized parser for that process or widen its title patterns."
            )

    if metrics.missing_daily_recap_days:
        recommendations.append(
            "Daily recaps are missing for active days. Check the lifecycle/reconciliation path so event days roll into daily summaries."
        )

    if metrics.degraded_payload_count and metrics.degraded_payload_count / total_quality_rows >= 0.15:
        recommendations.append(
            "Structured payload degradation is frequent enough to justify prompt/schema hardening or LM response repair work."
        )

    if metrics.file_entity_count >= 5 and metrics.task_candidate_count <= max(1, metrics.file_entity_count // 3):
        recommendations.append(
            "File evidence is strong but task candidates are thin. Improve task mapping and ticket candidate extraction."
        )

    if total_quality_rows and (good_or_excellent / total_quality_rows) >= 0.7 and weak_or_poor == 0:
        recommendations.append(
            "Evidence quality is mostly good or excellent. Phase 3 entity search looks ready to proceed."
        )

    if not recommendations:
        recommendations.append("No obvious blockers surfaced from this export.")

    return recommendations


def _render_metric_block(metrics: BundleMetrics) -> list[str]:
    lines = [
        f"- Date range covered: {_format_date_range(metrics)}",
        f"- Event summary count: `{_format_number(metrics.event_summary_count)}`",
        f"- Daily summary count: `{_format_number(metrics.daily_summary_count)}`",
        f"- Activity entity count: `{_format_number(metrics.activity_entity_count)}`",
        f"- Entities per summary: `{metrics.entities_per_summary:.2f}`",
        f"- Evidence quality bucket counts: {_format_bucket_counts(metrics.evidence_bucket_counts)}",
        f"- Average evidence quality score: `{metrics.average_evidence_quality_score:.3f}`",
        f"- Degraded structured payload count: `{_format_number(metrics.degraded_payload_count)}`",
        f"- Unknown app count: `{_format_number(metrics.unknown_app_count)}`",
        f"- Unclassified window title count: `{_format_number(metrics.unclassified_window_title_count)}`",
        f"- Low-confidence entity count: `{_format_number(metrics.low_confidence_entity_count)}`",
    ]
    scope = _safe_str(metrics.manifest.get("export_scope"))
    if scope:
        lines.append(f"- Export scope: `{scope}`")
    version = metrics.manifest.get("audit_export_format_version")
    if version is not None:
        lines.append(f"- Export format version: `{version}`")
    return lines


def _format_date_range(metrics: BundleMetrics) -> str:
    if not metrics.date_range_start or not metrics.date_range_end:
        return "n/a"
    if metrics.date_range_start == metrics.date_range_end:
        return f"`{metrics.date_range_start}`"
    return f"`{metrics.date_range_start}` to `{metrics.date_range_end}`"


def _render_current_report(metrics: BundleMetrics) -> str:
    sections = [
        "# WLD Audit Export Analysis",
        "",
        "## Overview",
        * _render_metric_block(metrics),
        "",
        "## Top Signals",
        f"- Top entity types: {_format_ranked_items(metrics.top_entity_types)}",
        f"- Top processes: {_format_ranked_items(metrics.top_processes)}",
        f"- Top files: {_format_ranked_items_with_type(metrics.top_files)}",
        f"- Top task/ticket candidates: {_format_ranked_items_with_type(metrics.top_task_candidates)}",
        f"- Top conversation/mail/web subjects: {_format_ranked_items_with_type(metrics.top_conversation_subjects)}",
        f"- Top unknown processes: {_format_ranked_items(metrics.top_unknown_processes)}",
        f"- Top unknown window patterns: {_format_ranked_items(metrics.top_unknown_window_patterns)}",
        "",
        "## Gaps",
        f"- Days with event summaries but missing daily recap: {', '.join(f'`{day}`' for day in metrics.missing_daily_recap_days) if metrics.missing_daily_recap_days else 'none'}",
        f"- Days dominated by weak/poor evidence: {', '.join(f'`{day}`' for day in metrics.weak_or_poor_days) if metrics.weak_or_poor_days else 'none'}",
        "",
        "## Recommendations",
        *[f"- {item}" for item in _recommendations(metrics)],
    ]
    return "\n".join(sections)


def _render_compare_report(current: BundleMetrics, baseline: BundleMetrics) -> str:
    bucket_deltas = []
    for bucket in BUCKET_ORDER:
        delta = current.evidence_bucket_counts.get(bucket, 0) - baseline.evidence_bucket_counts.get(bucket, 0)
        bucket_deltas.append(f"{bucket} {_format_signed_int(delta)}")
    file_delta = current.file_entity_count - baseline.file_entity_count
    task_delta = current.task_candidate_count - baseline.task_candidate_count
    conversation_delta = current.conversation_entity_count - baseline.conversation_entity_count

    sections = [
        "# WLD Audit Export Analysis",
        "",
        f"Current export: `{current.path}`",
        f"Baseline export: `{baseline.path}`",
        "",
        "## Current Snapshot",
        * _render_metric_block(current),
        "",
        "## Delta vs Baseline",
        f"- Summary count delta: `{_format_signed_int(current.event_summary_count - baseline.event_summary_count)}`",
        f"- Entity count delta: `{_format_signed_int(current.activity_entity_count - baseline.activity_entity_count)}`",
        f"- Entities per summary delta: `{_format_signed_float(current.entities_per_summary - baseline.entities_per_summary)}`",
        f"- Evidence quality bucket deltas: {', '.join(bucket_deltas)}",
        f"- Average score delta: `{_format_signed_float(current.average_evidence_quality_score - baseline.average_evidence_quality_score)}`",
        f"- Degraded payload delta: `{_format_signed_int(current.degraded_payload_count - baseline.degraded_payload_count)}`",
        f"- Unknown app delta: `{_format_signed_int(current.unknown_app_count - baseline.unknown_app_count)}`",
        f"- File/task/conversation entity deltas: files `{_format_signed_int(file_delta)}`, task candidates `{_format_signed_int(task_delta)}`, conversation/mail/web subjects `{_format_signed_int(conversation_delta)}`",
        f"- Missing daily recap delta: `{_format_signed_int(len(current.missing_daily_recap_days) - len(baseline.missing_daily_recap_days))}`",
        "",
        "## Recommendations",
        *[f"- {item}" for item in _recommendations(current)],
    ]
    return "\n".join(sections)


def build_report(bundle_dir: Path, compare_dir: Path | None = None) -> str:
    current = analyze_bundle(_load_bundle(bundle_dir))
    if compare_dir is None:
        return _render_current_report(current)
    baseline = analyze_bundle(_load_bundle(compare_dir))
    return _render_compare_report(current, baseline)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Summarize a WLD audit export bundle")
    parser.add_argument("bundle_dir", help="Path to the audit export bundle directory")
    parser.add_argument("--compare", dest="compare_dir", default=None, help="Optional baseline audit export bundle")
    args = parser.parse_args(argv)

    try:
        report = build_report(Path(args.bundle_dir), Path(args.compare_dir) if args.compare_dir else None)
    except AuditExportAnalysisError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
