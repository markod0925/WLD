from __future__ import annotations

import json
import logging
import tempfile
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from collections import Counter
from pathlib import Path
from statistics import mean
from typing import Any

from worklog_diary import __version__

from .config import AppConfig
from .evidence_quality import score_daily_evidence_quality
from .evidence_quality import score_event_evidence_quality

_LOGGER = logging.getLogger(__name__)
_AUDIT_EXPORT_FORMAT_VERSION = 1
_LOW_CONFIDENCE_ENTITY_THRESHOLD = 0.8


@dataclass(slots=True)
class AuditExportOptions:
    start_day: date | None = None
    end_day: date | None = None
    redact_window_titles: bool = False
    redact_process_names: bool = False
    redact_database_path: bool = True


@dataclass(slots=True)
class AuditExportResult:
    output_dir: Path
    manifest_path: Path
    counts: dict[str, int]


class AuditExportError(RuntimeError):
    """Raised when exporting an audit bundle fails."""


def export_audit_bundle(
    storage: Any,
    output_dir: str | Path,
    options: AuditExportOptions,
    *,
    config: AppConfig,
) -> AuditExportResult:
    started_at = datetime.utcnow()
    end_day_exclusive = options.end_day + timedelta(days=1) if options.end_day is not None else None
    start_ts = _day_start_ts(options.start_day) if options.start_day is not None else None
    end_ts = _day_start_ts(end_day_exclusive) if end_day_exclusive is not None else None

    base_dir = Path(output_dir)
    bundle_dir = base_dir / _build_bundle_dir_name(started_at)
    _LOGGER.info(
        "event=audit_export_start output_dir=%s start_day=%s end_day=%s export_scope=%s",
        str(bundle_dir),
        options.start_day.isoformat() if options.start_day else "none",
        options.end_day.isoformat() if options.end_day else "none",
        "summaries_and_coalescing_diagnostics_and_activity_entities_and_parser_coverage_and_evidence_quality",
    )

    try:
        bundle_dir.mkdir(parents=True, exist_ok=False)
    except Exception as exc:
        raise AuditExportError(f"Could not create export directory '{bundle_dir}': {exc}") from exc

    counts: dict[str, int] = {}
    try:
        summaries = storage.list_audit_summaries(start_ts=start_ts, end_ts=end_ts)
        summary_rows = [
            _build_summary_row(row=item, options=options)
            for item in summaries
        ]
        counts["summaries.jsonl"] = _write_jsonl_atomic(bundle_dir / "summaries.jsonl", summary_rows)

        daily_summaries = storage.list_audit_daily_summaries(
            start_day=options.start_day,
            end_day_exclusive=end_day_exclusive,
        )
        daily_rows = [_build_daily_summary_row(item) for item in daily_summaries]
        counts["daily_summaries.jsonl"] = _write_jsonl_atomic(bundle_dir / "daily_summaries.jsonl", daily_rows)

        coalesced = storage.list_audit_coalesced_summaries(
            start_day=options.start_day,
            end_day_exclusive=end_day_exclusive,
        )
        coalesced_rows = [_build_coalesced_row(item) for item in coalesced]
        counts["coalesced_summaries.jsonl"] = _write_jsonl_atomic(
            bundle_dir / "coalesced_summaries.jsonl",
            coalesced_rows,
        )

        activity_entities = storage.list_audit_activity_entities(
            start_day=options.start_day,
            end_day_exclusive=end_day_exclusive,
        )
        activity_entity_rows = [_build_activity_entity_row(item) for item in activity_entities]
        counts["activity_entities.jsonl"] = _write_jsonl_atomic(
            bundle_dir / "activity_entities.jsonl",
            activity_entity_rows,
        )

        activity_entities_by_summary_id = _index_activity_entities_by_summary_id(activity_entity_rows)
        parser_coverage_rows = _build_parser_coverage_rows(summaries)
        counts["parser_coverage.jsonl"] = _write_jsonl_atomic(
            bundle_dir / "parser_coverage.jsonl",
            parser_coverage_rows,
        )

        unknown_app_rows = _build_unknown_app_rows(parser_coverage_rows)
        counts["unknown_apps.jsonl"] = _write_jsonl_atomic(
            bundle_dir / "unknown_apps.jsonl",
            unknown_app_rows,
        )

        unknown_pattern_rows = _build_unknown_window_pattern_rows(parser_coverage_rows)
        counts["unknown_window_patterns.jsonl"] = _write_jsonl_atomic(
            bundle_dir / "unknown_window_patterns.jsonl",
            unknown_pattern_rows,
        )

        low_confidence_rows = _build_low_confidence_entity_rows(activity_entity_rows)
        counts["low_confidence_entities.jsonl"] = _write_jsonl_atomic(
            bundle_dir / "low_confidence_entities.jsonl",
            low_confidence_rows,
        )

        evidence_quality_rows, evidence_quality_summary = _build_evidence_quality_bundle(
            summaries=summaries,
            daily_summaries=daily_summaries,
            activity_entities_by_summary_id=activity_entities_by_summary_id,
            parser_coverage_rows=parser_coverage_rows,
            unknown_app_rows=unknown_app_rows,
            low_confidence_rows=low_confidence_rows,
        )
        counts["evidence_quality.jsonl"] = _write_jsonl_atomic(
            bundle_dir / "evidence_quality.jsonl",
            evidence_quality_rows,
        )
        counts["evidence_quality_summary.json"] = 1
        _write_json_atomic(bundle_dir / "evidence_quality_summary.json", evidence_quality_summary)

        merge_diagnostics = storage.list_audit_merge_diagnostics(
            start_day=options.start_day,
            end_day_exclusive=end_day_exclusive,
        )
        diagnostics_rows = [_build_merge_diagnostic_row(item, config=config) for item in merge_diagnostics]
        counts["merge_diagnostics.jsonl"] = _write_jsonl_atomic(
            bundle_dir / "merge_diagnostics.jsonl",
            diagnostics_rows,
        )

        config_snapshot = _build_config_snapshot(config)
        _write_json_atomic(bundle_dir / "config_snapshot.json", config_snapshot)

        manifest = {
            "audit_export_format_version": _AUDIT_EXPORT_FORMAT_VERSION,
            "exported_at_utc": started_at.isoformat(timespec="seconds") + "Z",
            "app_version": __version__,
            "database": {
                "path": Path(storage.db_path).name if options.redact_database_path else str(storage.db_path),
                "redacted": bool(options.redact_database_path),
            },
            "options": _options_to_manifest(options),
            "contains_raw_activity_data": False,
            "export_scope": "summaries_and_coalescing_diagnostics_and_activity_entities_and_parser_coverage_and_evidence_quality",
            "counts": counts,
            "evidence_quality_count": len(evidence_quality_rows),
            "evidence_quality_bucket_counts": evidence_quality_summary["bucket_counts"],
            "average_evidence_quality_score": evidence_quality_summary["average_score"],
            "poor_or_weak_summary_count": evidence_quality_summary["poor_or_weak_summary_count"],
            "evidence_quality_summary": evidence_quality_summary,
        }
        _write_json_atomic(bundle_dir / "manifest.json", manifest)
        _write_text_atomic(bundle_dir / "audit_readme.md", _build_audit_readme())

        _LOGGER.info("event=audit_export_complete output_dir=%s counts=%s", str(bundle_dir), counts)
        return AuditExportResult(output_dir=bundle_dir, manifest_path=bundle_dir / "manifest.json", counts=counts)
    except Exception as exc:
        _LOGGER.exception("event=audit_export_failed output_dir=%s error=%s", str(bundle_dir), exc)
        raise AuditExportError(f"Audit export failed: {exc}") from exc


def _build_summary_row(*, row: dict[str, Any], options: AuditExportOptions) -> dict[str, Any]:
    summary_json = row.get("summary_json") if isinstance(row.get("summary_json"), dict) else {}
    source_context = summary_json.get("source_context") if isinstance(summary_json.get("source_context"), dict) else {}
    prompt_metadata = summary_json.get("metadata") if isinstance(summary_json.get("metadata"), dict) else {}
    persisted_counts = (
        prompt_metadata.get("included_counts")
        if isinstance(prompt_metadata.get("included_counts"), dict)
        else {}
    )

    process_name = str(source_context.get("process_name") or "")
    window_title = str(source_context.get("window_title") or "")
    if options.redact_process_names:
        process_name = "[REDACTED]"
    if options.redact_window_titles:
        window_title = "[REDACTED]"

    return {
        "summary_id": row["summary_id"],
        "job_id": row["job_id"],
        "day": datetime.fromtimestamp(float(row["start_ts"])).date().isoformat(),
        "start_ts": float(row["start_ts"]),
        "end_ts": float(row["end_ts"]),
        "process_name": process_name,
        "window_title": window_title,
        "summary_text": str(row["summary_text"]),
        "screenshot_count": persisted_counts.get("screenshots"),
        "text_segment_count": persisted_counts.get("text_segments"),
        "prompt_name": prompt_metadata.get("schema"),
        "prompt_version": prompt_metadata.get("schema"),
        "model_name": summary_json.get("model_name"),
        "created_at": float(row["created_ts"]),
    }


def _build_bundle_dir_name(started_at: datetime) -> str:
    return f"wld_audit_export_{started_at.strftime('%Y%m%d_%H%M%S_%f')}_{uuid.uuid4().hex[:8]}"


def _build_daily_summary_row(row: dict[str, Any]) -> dict[str, Any]:
    recap_json = row.get("recap_json") if isinstance(row.get("recap_json"), dict) else {}
    metadata = recap_json.get("metadata") if isinstance(recap_json.get("metadata"), dict) else {}
    return {
        "day": row["day"],
        "daily_summary_id": row["daily_summary_id"],
        "summary_id": row["daily_summary_id"],
        "job_id": recap_json.get("job_id"),
        "summary_text": row["recap_text"],
        "source_summary_count": int(row["source_batch_count"]),
        "prompt_name": metadata.get("schema"),
        "prompt_version": metadata.get("schema"),
        "model_name": recap_json.get("model_name"),
        "created_at": float(row["created_ts"]),
    }


def _build_coalesced_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("summary_json") if isinstance(row.get("summary_json"), dict) else {}
    score = payload.get("merge_score")
    return {
        "coalesced_id": row["coalesced_id"],
        "day": row["day"],
        "start_ts": float(row["start_ts"]),
        "end_ts": float(row["end_ts"]),
        "summary_text": row["summary_text"],
        "member_summary_ids": [int(item) for item in row.get("member_summary_ids", [])],
        "member_count": len(row.get("member_summary_ids", [])),
        "confidence_bucket": payload.get("confidence_bucket"),
        "merge_score_aggregate": float(score) if isinstance(score, (int, float)) else None,
        "created_at": float(row["created_ts"]),
    }


def _build_activity_entity_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "activity_entity_id": row["activity_entity_id"],
        "summary_id": row.get("summary_id"),
        "day": row["day"],
        "start_ts": float(row["start_ts"]),
        "end_ts": float(row["end_ts"]),
        "entity_type": row["entity_type"],
        "entity_value": row["entity_value"],
        "entity_normalized": row["entity_normalized"],
        "source_kind": row["source_kind"],
        "source_ref": row["source_ref"],
        "evidence_kind": row["evidence_kind"],
        "confidence": float(row["confidence"]),
        "attributes_json": row["attributes_json"],
        "created_at": float(row["created_at"]),
    }


def _build_parser_coverage_rows(summaries: list[dict[str, object]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for summary in summaries:
        summary_json = summary.get("summary_json") if isinstance(summary.get("summary_json"), dict) else {}
        coverage_items = summary_json.get("parser_coverage") if isinstance(summary_json.get("parser_coverage"), list) else []
        for item in coverage_items:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "summary_id": summary["summary_id"],
                    "job_id": summary["job_id"],
                    "day": datetime.fromtimestamp(float(summary["start_ts"])).date().isoformat(),
                    "start_ts": float(item.get("start_ts", summary["start_ts"])),
                    "end_ts": float(item.get("end_ts", summary["end_ts"])),
                    "process_name": str(item.get("process_name") or ""),
                    "normalized_process_name": str(item.get("normalized_process_name") or "").lower(),
                    "window_title": str(item.get("window_title") or ""),
                    "normalized_window_title": str(item.get("normalized_window_title") or "").lower(),
                    "matched_parser_names": list(item.get("matched_parser_names") or []),
                    "used_generic_parser": bool(item.get("used_generic_parser")),
                    "used_specialized_parser": bool(item.get("used_specialized_parser")),
                    "extracted_entity_count": int(item.get("extracted_entity_count", 0)),
                    "unclassified_evidence_count": int(item.get("unclassified_evidence_count", 0)),
                    "parser_confidence": float(item.get("parser_confidence", 0.0)),
                    "unknown_app": bool(item.get("unknown_app")),
                    "created_at": float(summary["created_ts"]),
                }
            )
    return rows


def _build_unknown_app_rows(parser_coverage_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in parser_coverage_rows:
        if not row.get("unknown_app"):
            continue
        key = str(row.get("normalized_process_name") or row.get("process_name") or "")
        bucket = grouped.setdefault(
            key,
            {
                "process_name": row.get("process_name") or "",
                "normalized_process_name": row.get("normalized_process_name") or "",
                "occurrence_count": 0,
                "first_seen_ts": float(row["start_ts"]),
                "last_seen_ts": float(row["end_ts"]),
                "sample_window_title": row.get("window_title") or "",
                "normalized_title_sample": row.get("normalized_window_title") or "",
                "extracted_entity_count": 0,
                "unclassified_evidence_count": 0,
                "matched_parser_names": set(),
                "parser_confidence_total": 0.0,
            },
        )
        bucket["occurrence_count"] += 1
        bucket["first_seen_ts"] = min(float(bucket["first_seen_ts"]), float(row["start_ts"]))
        bucket["last_seen_ts"] = max(float(bucket["last_seen_ts"]), float(row["end_ts"]))
        bucket["extracted_entity_count"] = max(int(bucket["extracted_entity_count"]), int(row["extracted_entity_count"]))
        bucket["unclassified_evidence_count"] = max(int(bucket["unclassified_evidence_count"]), int(row["unclassified_evidence_count"]))
        bucket["matched_parser_names"].update(str(name) for name in row.get("matched_parser_names", []) if str(name).strip())
        bucket["parser_confidence_total"] += float(row.get("parser_confidence", 0.0))
        if not bucket["sample_window_title"]:
            bucket["sample_window_title"] = row.get("window_title") or ""
        if not bucket["normalized_title_sample"]:
            bucket["normalized_title_sample"] = row.get("normalized_window_title") or ""

    rows: list[dict[str, Any]] = []
    for bucket in grouped.values():
        count = max(1, int(bucket["occurrence_count"]))
        rows.append(
            {
                "process_name": bucket["process_name"],
                "normalized_process_name": bucket["normalized_process_name"],
                "occurrence_count": count,
                "first_seen_ts": float(bucket["first_seen_ts"]),
                "last_seen_ts": float(bucket["last_seen_ts"]),
                "sample_window_title": bucket["sample_window_title"],
                "normalized_title_sample": bucket["normalized_title_sample"],
                "extracted_entity_count": int(bucket["extracted_entity_count"]),
                "unclassified_evidence_count": int(bucket["unclassified_evidence_count"]),
                "matched_parser_names": sorted(bucket["matched_parser_names"]),
                "parser_confidence": round(float(bucket["parser_confidence_total"]) / count, 3),
                "unknown_app": True,
            }
        )
    rows.sort(key=lambda item: (-int(item["occurrence_count"]), str(item["normalized_process_name"])))
    return rows


def _build_unknown_window_pattern_rows(parser_coverage_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in parser_coverage_rows:
        if not row.get("unknown_app"):
            continue
        key = (str(row.get("normalized_process_name") or ""), str(row.get("normalized_window_title") or ""))
        bucket = grouped.setdefault(
            key,
            {
                "process_name": row.get("process_name") or "",
                "normalized_process_name": row.get("normalized_process_name") or "",
                "sample_window_title": row.get("window_title") or "",
                "normalized_title_sample": row.get("normalized_window_title") or "",
                "occurrence_count": 0,
                "first_seen_ts": float(row["start_ts"]),
                "last_seen_ts": float(row["end_ts"]),
                "extracted_entity_count": 0,
                "candidate_patterns": set(),
            },
        )
        bucket["occurrence_count"] += 1
        bucket["first_seen_ts"] = min(float(bucket["first_seen_ts"]), float(row["start_ts"]))
        bucket["last_seen_ts"] = max(float(bucket["last_seen_ts"]), float(row["end_ts"]))
        bucket["extracted_entity_count"] = max(int(bucket["extracted_entity_count"]), int(row["extracted_entity_count"]))
        bucket["candidate_patterns"].update(str(name) for name in row.get("matched_parser_names", []) if str(name).strip())
        if not bucket["sample_window_title"]:
            bucket["sample_window_title"] = row.get("window_title") or ""
        if not bucket["normalized_title_sample"]:
            bucket["normalized_title_sample"] = row.get("normalized_window_title") or ""

    rows: list[dict[str, Any]] = []
    for bucket in grouped.values():
        candidate_patterns = sorted(bucket["candidate_patterns"])
        suggested_reason = "No specialized parser matched; preserve exact title tokens and consider a dedicated parser."
        if candidate_patterns:
            suggested_reason = (
                "No specialized parser matched; generic parsers extracted "
                + ", ".join(candidate_patterns[:6])
                + "."
            )
        rows.append(
            {
                "process_name": bucket["process_name"],
                "normalized_process_name": bucket["normalized_process_name"],
                "sample_window_title": bucket["sample_window_title"],
                "normalized_title_sample": bucket["normalized_title_sample"],
                "occurrence_count": int(bucket["occurrence_count"]),
                "first_seen_ts": float(bucket["first_seen_ts"]),
                "last_seen_ts": float(bucket["last_seen_ts"]),
                "extracted_entity_count": int(bucket["extracted_entity_count"]),
                "candidate_patterns": candidate_patterns,
                "suggested_parser_reason": suggested_reason,
                "unknown_app": True,
            }
        )
    rows.sort(key=lambda item: (-int(item["occurrence_count"]), str(item["normalized_process_name"]), str(item["normalized_title_sample"])))
    return rows


def _build_low_confidence_entity_rows(activity_entity_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        dict(row)
        for row in activity_entity_rows
        if float(row.get("confidence", 0.0)) < _LOW_CONFIDENCE_ENTITY_THRESHOLD
    ]
    rows.sort(key=lambda item: (-float(item["confidence"]), str(item["entity_type"]), str(item["entity_normalized"])))
    return rows


def _index_activity_entities_by_summary_id(activity_entity_rows: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in activity_entity_rows:
        summary_id = row.get("summary_id")
        if summary_id is None:
            continue
        grouped.setdefault(int(summary_id), []).append(dict(row))
    return grouped


def _build_evidence_quality_bundle(
    *,
    summaries: list[dict[str, object]],
    daily_summaries: list[dict[str, object]],
    activity_entities_by_summary_id: dict[int, list[dict[str, Any]]],
    parser_coverage_rows: list[dict[str, Any]],
    unknown_app_rows: list[dict[str, Any]],
    low_confidence_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    event_reports: list[dict[str, Any]] = []
    daily_reports: list[dict[str, Any]] = []
    event_reports_by_day: dict[str, list[dict[str, Any]]] = {}

    for summary in summaries:
        summary_json = summary.get("summary_json") if isinstance(summary.get("summary_json"), dict) else {}
        source_entities = activity_entities_by_summary_id.get(int(summary["summary_id"]), [])
        if not source_entities and isinstance(summary_json.get("activity_entities"), list):
            source_entities = [item for item in summary_json.get("activity_entities") if isinstance(item, dict)]
        report = score_event_evidence_quality(
            summary_id=int(summary["summary_id"]),
            day=datetime.fromtimestamp(float(summary["start_ts"])).date(),
            start_ts=float(summary["start_ts"]),
            end_ts=float(summary["end_ts"]),
            summary_json=summary_json,
            activity_entities=source_entities,
            parser_coverage=summary_json.get("parser_coverage") if isinstance(summary_json.get("parser_coverage"), list) else None,
            source_batch=summary_json.get("source_batch") if isinstance(summary_json.get("source_batch"), dict) else None,
            source_context=summary_json.get("source_context") if isinstance(summary_json.get("source_context"), dict) else None,
        )
        row = report.to_dict()
        event_reports.append(row)
        event_reports_by_day.setdefault(str(row["day"]), []).append(row)

    for daily_summary in daily_summaries:
        recap_json = daily_summary.get("recap_json") if isinstance(daily_summary.get("recap_json"), dict) else {}
        day_key = str(daily_summary["day"])
        source_event_reports = [
            _report_from_dict(item)
            for item in event_reports_by_day.get(day_key, [])
        ]
        report = score_daily_evidence_quality(
            summary_id=int(daily_summary["daily_summary_id"]),
            day=day_key,
            start_ts=_day_start_ts(datetime.fromisoformat(day_key).date()),
            end_ts=_day_start_ts(datetime.fromisoformat(day_key).date()) + 86400.0,
            recap_json=recap_json,
            source_event_reports=source_event_reports,
        )
        daily_reports.append(report.to_dict())

    all_reports = sorted(
        [*event_reports, *daily_reports],
        key=lambda item: (
            str(item.get("summary_kind") or ""),
            str(item.get("day") or ""),
            float(item.get("start_ts", 0.0)),
            float(item.get("end_ts", 0.0)),
            int(item.get("summary_id") or 0),
        ),
    )

    bucket_counts = Counter(str(row.get("bucket") or "unknown") for row in all_reports)
    average_score = round(mean(float(row.get("score", 0.0)) for row in all_reports), 3) if all_reports else 0.0
    poor_or_weak_summary_count = sum(str(row.get("bucket")) in {"poor", "weak"} for row in all_reports)
    summaries_without_file_or_task_entities = sum(
        not bool(row.get("has_file_evidence")) and not bool(row.get("has_task_evidence"))
        for row in event_reports
    )
    summaries_with_only_unclassified_evidence = sum(_has_only_unclassified_evidence(row) for row in event_reports)
    degraded_payload_count = sum(bool(row.get("degraded_payload")) for row in all_reports)
    unknown_app_count = sum(bool(row.get("unknown_app")) for row in event_reports)

    top_unknown_processes = _build_top_unknown_processes(unknown_app_rows)
    top_low_confidence_entity_types = _build_top_low_confidence_entity_types(low_confidence_rows)
    parser_coverage_by_process = _build_parser_coverage_diagnostics(parser_coverage_rows)

    summary = {
        "summary_count": len(all_reports),
        "event_summary_count": len(event_reports),
        "daily_summary_count": len(daily_reports),
        "bucket_counts": dict(bucket_counts),
        "average_score": average_score,
        "poor_or_weak_summary_count": poor_or_weak_summary_count,
        "summaries_without_file_or_task_entities": summaries_without_file_or_task_entities,
        "summaries_with_only_unclassified_evidence": summaries_with_only_unclassified_evidence,
        "degraded_payload_count": degraded_payload_count,
        "unknown_app_count": unknown_app_count,
        "top_unknown_processes": top_unknown_processes,
        "top_low_confidence_entity_types": top_low_confidence_entity_types,
        "parser_coverage_by_process": parser_coverage_by_process,
    }
    return all_reports, summary


def _report_from_dict(item: dict[str, Any]) -> Any:
    return type(
        "EvidenceQualityProxy",
        (),
        {
            "score": float(item.get("score", 0.0)),
            "bucket": str(item.get("bucket") or "poor"),
            "blocked_or_privacy_heavy": bool(item.get("blocked_or_privacy_heavy")),
            "entity_counts_by_type": dict(item.get("entity_counts_by_type") or {}),
            "unknown_app": bool(item.get("unknown_app")),
            "degraded_payload": bool(item.get("degraded_payload")),
            "has_file_evidence": bool(item.get("has_file_evidence")),
            "has_task_evidence": bool(item.get("has_task_evidence")),
            "has_conversation_evidence": bool(item.get("has_conversation_evidence")),
            "has_text_evidence": bool(item.get("has_text_evidence")),
            "has_screenshot_evidence": bool(item.get("has_screenshot_evidence")),
        },
    )()


def _has_only_unclassified_evidence(row: dict[str, Any]) -> bool:
    entity_counts = row.get("entity_counts_by_type")
    if not isinstance(entity_counts, dict) or not entity_counts:
        return False
    allowed = {"program", "window_title", "unclassified_window_title"}
    return set(str(key) for key in entity_counts.keys()) <= allowed and bool(row.get("unknown_app"))


def _build_top_unknown_processes(unknown_app_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in unknown_app_rows:
        key = str(row.get("normalized_process_name") or row.get("process_name") or "")
        bucket = grouped.setdefault(
            key,
            {
                "process_name": str(row.get("process_name") or ""),
                "normalized_process_name": str(row.get("process_name") or "").strip().lower(),
                "occurrence_count": 0,
            },
        )
        bucket["occurrence_count"] += int(row.get("occurrence_count", 1))
    rows = sorted(grouped.values(), key=lambda item: (-int(item["occurrence_count"]), str(item["normalized_process_name"])))
    return rows[:10]


def _build_top_low_confidence_entity_types(low_confidence_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    for row in low_confidence_rows:
        counts[str(row.get("entity_type") or "")] += 1
    return [
        {"entity_type": entity_type, "count": count}
        for entity_type, count in counts.most_common(10)
    ]


def _build_parser_coverage_diagnostics(parser_coverage_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_process: dict[str, dict[str, Any]] = {}
    for item in parser_coverage_rows:
        process_name = str(item.get("process_name") or "")
        key = process_name.strip().lower()
        bucket = by_process.setdefault(
            key,
            {
                "process_name": process_name,
                "normalized_process_name": key,
                "occurrence_count": 0,
                "unknown_app_count": 0,
                "used_generic_parser_count": 0,
                "used_specialized_parser_count": 0,
                "parser_confidence_total": 0.0,
            },
        )
        bucket["occurrence_count"] += 1
        bucket["unknown_app_count"] += int(bool(item.get("unknown_app")))
        bucket["used_generic_parser_count"] += int(bool(item.get("used_generic_parser")))
        bucket["used_specialized_parser_count"] += int(bool(item.get("used_specialized_parser")))
        bucket["parser_confidence_total"] += float(item.get("parser_confidence", 0.0))
    rows: list[dict[str, Any]] = []
    for bucket in by_process.values():
        count = max(1, int(bucket["occurrence_count"]))
        rows.append(
            {
                "process_name": bucket["process_name"],
                "normalized_process_name": bucket["normalized_process_name"],
                "occurrence_count": count,
                "unknown_app_count": int(bucket["unknown_app_count"]),
                "used_generic_parser_count": int(bucket["used_generic_parser_count"]),
                "used_specialized_parser_count": int(bucket["used_specialized_parser_count"]),
                "parser_confidence": round(float(bucket["parser_confidence_total"]) / count, 3),
            }
        )
    rows.sort(key=lambda item: (-int(item["occurrence_count"]), str(item["normalized_process_name"])))
    return rows[:25]


def _build_merge_diagnostic_row(row: dict[str, Any], *, config: AppConfig) -> dict[str, Any]:
    blockers = row.get("blockers_json") if isinstance(row.get("blockers_json"), list) else []
    reasons = row.get("reasons_json") if isinstance(row.get("reasons_json"), list) else []
    return {
        "day": row["day"],
        "left_summary_id": row["left_summary_id"],
        "right_summary_id": row["right_summary_id"],
        "decision": row["decision"],
        "merge_score": float(row["final_merge_score"]),
        "embedding_similarity": float(row["embedding_cosine_similarity"]),
        "temporal_gap_seconds": float(row["temporal_gap_seconds"]),
        "same_app": float(row["app_similarity_score"]) > 0.0,
        "same_window": float(row["window_similarity_score"]) > 0.0,
        "reason": blockers,
        "diagnostic_flags": reasons,
        "active_preset": "custom",
        "semantic_parameters": {
            "min_cosine_similarity": float(config.semantic_min_cosine_similarity),
            "min_merge_score": float(config.semantic_min_merge_score),
            "max_candidate_gap_seconds": int(config.semantic_max_candidate_gap_seconds),
            "same_app_boost": float(config.semantic_same_app_boost),
            "window_title_boost": float(config.semantic_window_title_boost),
            "keyword_overlap_boost": float(config.semantic_keyword_overlap_boost),
            "temporal_gap_penalty_weight": float(config.semantic_temporal_gap_penalty_weight),
            "app_switch_penalty": float(config.semantic_app_switch_penalty),
            "lock_boundary_blocks_merge": bool(config.semantic_lock_boundary_blocks_merge),
            "pause_boundary_blocks_merge": bool(config.semantic_pause_boundary_blocks_merge),
        },
        "created_at": float(row["created_ts"]),
    }


def _build_config_snapshot(config: AppConfig) -> dict[str, Any]:
    return {
        "semantic_coalescing": {
            "enabled": bool(config.semantic_coalescing_enabled),
            "preset": "custom",
            "thresholds": {
                "min_cosine_similarity": float(config.semantic_min_cosine_similarity),
                "min_merge_score": float(config.semantic_min_merge_score),
            },
            "max_temporal_gap_seconds": int(config.semantic_max_candidate_gap_seconds),
            "max_neighbor_count": int(config.semantic_max_neighbor_count),
            "embedding": {
                "base_url": config.semantic_embedding_base_url,
                "model": config.semantic_embedding_model,
            },
        },
        "screenshot_dedup": {
            "enabled": bool(config.screenshot_dedup_enabled),
            "exact_hash_enabled": bool(config.screenshot_dedup_exact_hash_enabled),
            "perceptual_hash_enabled": bool(config.screenshot_dedup_perceptual_hash_enabled),
            "phash_threshold": int(config.screenshot_dedup_phash_threshold),
            "ssim_enabled": bool(config.screenshot_dedup_ssim_enabled),
            "ssim_threshold": float(config.screenshot_dedup_ssim_threshold),
            "compare_recent_count": int(config.screenshot_dedup_compare_recent_count),
            "resize_width": int(config.screenshot_dedup_resize_width),
            "min_keep_interval_seconds": int(config.screenshot_min_keep_interval_seconds),
        },
        "summary_model": {
            "base_url": config.lmstudio_base_url,
            "model": config.lmstudio_model,
            "max_prompt_chars": int(config.lmstudio_max_prompt_chars),
            "max_text_segments_per_summary": int(config.max_text_segments_per_summary),
            "max_screenshots_per_summary": int(config.max_screenshots_per_summary),
        },
    }


def _build_audit_readme() -> str:
    return (
        "# WorkLog Diary Audit Bundle\n\n"
        "This bundle is intended for offline audit and Codex-assisted analysis of generated summaries and semantic merges.\n\n"
        "This bundle contains generated summaries, daily summaries, coalesced summaries, derived activity entities, merge diagnostics, and relevant configuration only. "
        "It does not contain screenshots, raw key logs, raw captured text, or other raw activity data.\n\n"
        "## Files\n"
        "- `summaries.jsonl`: Event-level generated summaries with source metadata and persisted summary-level counts.\n"
        "- `daily_summaries.jsonl`: Daily recap summaries and source-count metadata.\n"
        "- `coalesced_summaries.jsonl`: Semantic coalescing outputs and member summary linkage.\n"
        "- `activity_entities.jsonl`: Derived evidence entities extracted from activity evidence.\n"
        "- `evidence_quality.jsonl`: Deterministic evidence-quality reports for event and daily summaries.\n"
        "- `evidence_quality_summary.json`: Compact aggregate diagnostics for audit triage.\n"
        "- `merge_diagnostics.jsonl`: Pair-level merge diagnostics and merge decisions.\n"
        "- `config_snapshot.json`: Current semantic/summarization configuration snapshot.\n"
        "- `manifest.json`: Export metadata, options, and per-file counts.\n\n"
        "## Suggested Codex audit questions\n"
        "- Identify over-aggressive merges.\n"
        "- Identify likely missed merges.\n"
        "- Find repetitive or low-information summaries.\n"
        "- Suggest semantic coalescing threshold changes.\n"
        "- Suggest prompt/model changes to reduce redundancy.\n"
        "\n"
        "## Copy/paste prompt\n"
        "```text\n"
        "Analyze this WLD summary audit bundle. Focus on:\n"
        "1. repetitive event summaries,\n"
        "2. over-aggressive semantic merges,\n"
        "3. missed merge opportunities,\n"
        "4. summary prompt weaknesses,\n"
        "5. suggested semantic coalescing threshold changes.\n"
        "\n"
        "Use manifest.json and config_snapshot.json to understand the active configuration.\n"
        "Do not assume raw screenshots, raw key logs, or raw captured text are available.\n"
        "```\n"
    )


def _options_to_manifest(options: AuditExportOptions) -> dict[str, Any]:
    manifest = asdict(options)
    if options.start_day is not None:
        manifest["start_day"] = options.start_day.isoformat()
    if options.end_day is not None:
        manifest["end_day"] = options.end_day.isoformat()
    return manifest


def _day_start_ts(value: date) -> float:
    return datetime.combine(value, datetime.min.time()).timestamp()


def _write_jsonl_atomic(path: Path, rows: list[dict[str, Any]]) -> int:
    lines = [json.dumps(row, ensure_ascii=False) for row in rows]
    payload = "\n".join(lines) + ("\n" if lines else "")
    _write_text_atomic(path, payload)
    return len(rows)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    _write_text_atomic(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=str(path.parent)) as tmp:
        tmp.write(text)
        temp_name = tmp.name
    Path(temp_name).replace(path)
