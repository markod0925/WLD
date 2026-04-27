from __future__ import annotations

import json
import logging
import tempfile
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from worklog_diary import __version__

from .config import AppConfig

_LOGGER = logging.getLogger(__name__)
_AUDIT_EXPORT_FORMAT_VERSION = 1


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
        "summaries_and_coalescing_diagnostics",
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
            "export_scope": "summaries_and_coalescing_diagnostics",
            "counts": counts,
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
        "This bundle contains generated summaries, daily summaries, coalesced summaries, merge diagnostics, and relevant configuration only. "
        "It does not contain screenshots, raw key logs, raw captured text, or other raw activity data.\n\n"
        "## Files\n"
        "- `summaries.jsonl`: Event-level generated summaries with source metadata and persisted summary-level counts.\n"
        "- `daily_summaries.jsonl`: Daily recap summaries and source-count metadata.\n"
        "- `coalesced_summaries.jsonl`: Semantic coalescing outputs and member summary linkage.\n"
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
