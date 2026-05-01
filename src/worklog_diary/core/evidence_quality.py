from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from statistics import mean
from typing import Any, Iterable, Sequence

_LOW_CONFIDENCE_THRESHOLD = 0.8


@dataclass(slots=True)
class EvidenceQualityReport:
    summary_id: int | None
    day: str
    start_ts: float
    end_ts: float
    score: float
    bucket: str
    strengths: list[str] = field(default_factory=list)
    weaknesses: list[str] = field(default_factory=list)
    entity_counts_by_type: dict[str, int] = field(default_factory=dict)
    unknown_app: bool = False
    degraded_payload: bool = False
    has_file_evidence: bool = False
    has_task_evidence: bool = False
    has_conversation_evidence: bool = False
    has_text_evidence: bool = False
    has_screenshot_evidence: bool = False
    blocked_or_privacy_heavy: bool = False
    summary_kind: str = "event"
    source_summary_count: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary_id": self.summary_id,
            "day": self.day,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "score": self.score,
            "bucket": self.bucket,
            "strengths": list(self.strengths),
            "weaknesses": list(self.weaknesses),
            "entity_counts_by_type": dict(self.entity_counts_by_type),
            "unknown_app": self.unknown_app,
            "degraded_payload": self.degraded_payload,
            "has_file_evidence": self.has_file_evidence,
            "has_task_evidence": self.has_task_evidence,
            "has_conversation_evidence": self.has_conversation_evidence,
            "has_text_evidence": self.has_text_evidence,
            "has_screenshot_evidence": self.has_screenshot_evidence,
            "blocked_or_privacy_heavy": self.blocked_or_privacy_heavy,
            "summary_kind": self.summary_kind,
            "source_summary_count": self.source_summary_count,
        }


def score_event_evidence_quality(
    *,
    summary_id: int | None,
    day: date | str,
    start_ts: float,
    end_ts: float,
    summary_json: dict[str, Any] | None,
    activity_entities: Sequence[dict[str, Any]] | None = None,
    parser_coverage: Sequence[dict[str, Any]] | None = None,
    source_batch: dict[str, Any] | None = None,
    source_context: dict[str, Any] | None = None,
) -> EvidenceQualityReport:
    payload = summary_json if isinstance(summary_json, dict) else {}
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    source_batch = source_batch if isinstance(source_batch, dict) else payload.get("source_batch") if isinstance(payload.get("source_batch"), dict) else {}
    source_context = source_context if isinstance(source_context, dict) else payload.get("source_context") if isinstance(payload.get("source_context"), dict) else {}
    included_counts = metadata.get("included_counts") if isinstance(metadata.get("included_counts"), dict) else {}

    entities = _normalize_entities(activity_entities or payload.get("activity_entities"))
    coverage_items = list(parser_coverage or payload.get("parser_coverage") or [])
    entity_counts_by_type = dict(Counter(item["entity_type"] for item in entities))

    has_file_evidence = any(item["entity_type"] in {"file_path", "file_name", "folder_path"} for item in entities) or bool(
        _coerce_list(payload.get("files"))
    )
    has_task_evidence = any(item["entity_type"] == "task_candidate" for item in entities) or bool(_coerce_list(payload.get("task_candidates")))
    has_conversation_evidence = any(
        item["entity_type"] in {"conversation_subject", "mail_subject", "web_page_title"} for item in entities
    ) or bool(_coerce_list(payload.get("conversations")))
    has_text_evidence = _coerce_int(included_counts.get("text_segments"), default=0) > 0 or _coerce_int(source_batch.get("text_segment_count"), default=0) > 0
    has_screenshot_evidence = _coerce_int(included_counts.get("screenshots"), default=0) > 0 or _coerce_int(source_batch.get("screenshot_count"), default=0) > 0
    degraded_payload = str(metadata.get("parse_status") or "").lower() == "degraded"
    unknown_app = any(bool(item.get("unknown_app")) for item in coverage_items) or any(
        item["entity_type"] == "unclassified_window_title" for item in entities
    )
    blocked_or_privacy_heavy = bool(source_context.get("blocked")) or _coerce_int(source_batch.get("blocked_interval_count"), default=0) > 0
    process_evidence = bool(str(source_context.get("process_name") or payload.get("process_name") or "").strip()) or any(
        item["entity_type"] == "program" for item in entities
    )
    window_title_evidence = bool(str(source_context.get("window_title") or payload.get("window_title") or "").strip()) or any(
        item["entity_type"] == "window_title" for item in entities
    )
    low_confidence_entities = [
        item for item in entities if _coerce_float(item.get("confidence"), default=1.0) < _LOW_CONFIDENCE_THRESHOLD
    ]
    confident_outcomes = _count_confident_items(payload.get("outcomes"))
    parser_confidence = _average_parser_confidence(coverage_items)

    strengths: list[str] = []
    weaknesses: list[str] = []
    score = 0.05

    if not degraded_payload:
        score += 0.12
        strengths.append("structured payload validated")
    else:
        score -= 0.15
        weaknesses.append("structured payload degraded")

    if process_evidence:
        score += 0.07
        strengths.append("process evidence present")
    else:
        weaknesses.append("no process evidence")

    if window_title_evidence:
        score += 0.06
        strengths.append("window title evidence present")
    else:
        weaknesses.append("no window title evidence")

    if has_file_evidence:
        score += 0.20
        strengths.append("file evidence present")
    else:
        weaknesses.append("no file evidence")

    if has_task_evidence:
        score += 0.16
        strengths.append("task evidence present")
    else:
        weaknesses.append("no task evidence")

    if has_conversation_evidence:
        score += 0.10
        strengths.append("conversation evidence present")

    if has_text_evidence:
        score += 0.07
        strengths.append("text evidence present")
    else:
        weaknesses.append("no text evidence")

    if has_screenshot_evidence:
        score += 0.05
        strengths.append("screenshot evidence present")

    if confident_outcomes:
        score += 0.05
        strengths.append("confident outcomes present")

    if parser_confidence >= 0.75:
        score += 0.04
        strengths.append("specialized parser coverage strong")
    elif parser_confidence >= 0.45:
        score += 0.02
        strengths.append("generic parser coverage usable")

    derived_evidence_count = int(has_file_evidence) + int(has_task_evidence) + int(has_conversation_evidence)
    raw_evidence_count = int(process_evidence) + int(window_title_evidence)
    if unknown_app and derived_evidence_count == 0:
        score -= 0.12
        weaknesses.append("unknown app with no derived entities")
    elif unknown_app:
        weaknesses.append("unknown app handled via generic parsers")

    if derived_evidence_count == 0:
        score -= 0.10
        weaknesses.append("only raw process/window evidence")

    if low_confidence_entities and len(low_confidence_entities) >= max(3, len(entities) // 2 + 1):
        score -= 0.06
        weaknesses.append("many low-confidence entities")

    if not (has_file_evidence or has_task_evidence or has_conversation_evidence) and _coerce_text(payload.get("summary_text")):
        score -= 0.05
        weaknesses.append("generic summary text without concrete entities")

    if blocked_or_privacy_heavy:
        score -= 0.10
        weaknesses.append("blocked or privacy-heavy interval")

    score = max(0.0, min(1.0, round(score, 3)))
    bucket = _bucket_for_score(score)
    if bucket == "excellent":
        strengths.append("strong evidence for historical reconstruction")
    elif bucket == "good":
        strengths.append("usable evidence for diary reconstruction")
    elif bucket == "weak":
        weaknesses.append("usable only with caution")
    else:
        weaknesses.append("insufficient evidence for reliable reconstruction")

    return EvidenceQualityReport(
        summary_id=summary_id,
        day=_day_to_string(day),
        start_ts=float(start_ts),
        end_ts=float(end_ts),
        score=score,
        bucket=bucket,
        strengths=_dedupe_preserve_order(strengths),
        weaknesses=_dedupe_preserve_order(weaknesses),
        entity_counts_by_type=entity_counts_by_type,
        unknown_app=unknown_app,
        degraded_payload=degraded_payload,
        has_file_evidence=has_file_evidence,
        has_task_evidence=has_task_evidence,
        has_conversation_evidence=has_conversation_evidence,
        has_text_evidence=has_text_evidence,
        has_screenshot_evidence=has_screenshot_evidence,
        blocked_or_privacy_heavy=blocked_or_privacy_heavy,
    )


def score_daily_evidence_quality(
    *,
    summary_id: int | None,
    day: date | str,
    start_ts: float,
    end_ts: float,
    recap_json: dict[str, Any] | None,
    source_event_reports: Sequence[EvidenceQualityReport],
) -> EvidenceQualityReport:
    recap = recap_json if isinstance(recap_json, dict) else {}
    metadata = recap.get("metadata") if isinstance(recap.get("metadata"), dict) else {}
    source_count = len(source_event_reports)
    recap_degraded = str(metadata.get("parse_status") or "").lower() == "degraded"
    recap_summary_text = _coerce_text(recap.get("executive_summary") or recap.get("summary_text"))
    recap_files = _coerce_list(recap.get("files_observed")) or _coerce_list(recap.get("files_likely_modified"))
    recap_tasks = _coerce_list(recap.get("tasks_advanced")) or _coerce_list(recap.get("jira_update_candidates"))
    recap_conversations = _coerce_list(recap.get("conversations_or_meetings"))
    confidence_notes = _coerce_list(recap.get("confidence_notes"))

    strengths: list[str] = []
    weaknesses: list[str] = []
    if recap_degraded:
        weaknesses.append("daily recap payload degraded")

    source_scores = [report.score for report in source_event_reports]
    source_average = mean(source_scores) if source_scores else 0.0
    if source_scores:
        strengths.append("source event summaries available")
        if source_average >= 0.65:
            strengths.append("source events provide good evidence")
        elif source_average < 0.35:
            weaknesses.append("source events are weak")

    recap_score = 0.0
    if recap_summary_text:
        recap_score += 0.05
        strengths.append("daily executive summary present")
    if _coerce_list(recap.get("program_activity_breakdown")):
        recap_score += 0.04
        strengths.append("program activity breakdown present")
    if recap_files:
        recap_score += 0.05
        strengths.append("daily file evidence present")
    if recap_tasks:
        recap_score += 0.05
        strengths.append("daily task evidence present")
    if recap_conversations:
        recap_score += 0.04
        strengths.append("daily conversation evidence present")
    if _coerce_list(recap.get("decisions")):
        recap_score += 0.03
        strengths.append("daily decisions present")
    if _coerce_list(recap.get("follow_ups")):
        recap_score += 0.03
        strengths.append("daily follow-ups present")
    if confidence_notes:
        recap_score += 0.03
        strengths.append("daily confidence notes present")

    if recap_degraded:
        recap_score -= 0.10

    if source_count and any(report.blocked_or_privacy_heavy for report in source_event_reports):
        recap_score -= 0.03

    if source_average:
        score = (source_average * 0.78) + (max(0.0, min(1.0, recap_score)) * 0.22)
    else:
        score = max(0.0, min(1.0, recap_score))

    score = max(0.0, min(1.0, round(score, 3)))
    bucket = _bucket_for_score(score)
    if bucket == "excellent":
        strengths.append("daily recap is supported by strong source evidence")
    elif bucket == "good":
        strengths.append("daily recap is reasonably grounded")
    elif bucket == "weak":
        weaknesses.append("daily recap needs source validation")
    else:
        weaknesses.append("daily recap lacks strong source evidence")

    entity_counts = Counter()
    unknown_app = False
    degraded_sources = 0
    has_file_evidence = False
    has_task_evidence = False
    has_conversation_evidence = False
    has_text_evidence = False
    has_screenshot_evidence = False
    blocked_or_privacy_heavy = False
    for report in source_event_reports:
        entity_counts.update(report.entity_counts_by_type)
        unknown_app = unknown_app or report.unknown_app
        degraded_sources += int(report.degraded_payload)
        has_file_evidence = has_file_evidence or report.has_file_evidence
        has_task_evidence = has_task_evidence or report.has_task_evidence
        has_conversation_evidence = has_conversation_evidence or report.has_conversation_evidence
        has_text_evidence = has_text_evidence or report.has_text_evidence
        has_screenshot_evidence = has_screenshot_evidence or report.has_screenshot_evidence
        blocked_or_privacy_heavy = blocked_or_privacy_heavy or report.blocked_or_privacy_heavy

    if source_count and degraded_sources:
        weaknesses.append("some source events degraded")
    if unknown_app:
        weaknesses.append("unknown app activity present")
    if not (has_file_evidence or has_task_evidence or has_conversation_evidence):
        weaknesses.append("daily summary lacks file/task/conversation evidence")
    if blocked_or_privacy_heavy and not (has_file_evidence or has_task_evidence or has_conversation_evidence):
        weaknesses.append("daily evidence is privacy-heavy")

    return EvidenceQualityReport(
        summary_id=summary_id,
        day=_day_to_string(day),
        start_ts=float(start_ts),
        end_ts=float(end_ts),
        score=score,
        bucket=bucket,
        strengths=_dedupe_preserve_order(strengths),
        weaknesses=_dedupe_preserve_order(weaknesses),
        entity_counts_by_type=dict(entity_counts),
        unknown_app=unknown_app,
        degraded_payload=recap_degraded or degraded_sources > 0,
        has_file_evidence=has_file_evidence,
        has_task_evidence=has_task_evidence,
        has_conversation_evidence=has_conversation_evidence,
        has_text_evidence=has_text_evidence or bool(recap_summary_text),
        has_screenshot_evidence=has_screenshot_evidence,
        blocked_or_privacy_heavy=blocked_or_privacy_heavy,
        summary_kind="daily",
        source_summary_count=source_count,
    )


def _bucket_for_score(score: float) -> str:
    if score >= 0.85:
        return "excellent"
    if score >= 0.65:
        return "good"
    if score >= 0.35:
        return "weak"
    return "poor"


def _normalize_entities(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    result: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            result.append(item)
    return result


def _count_confident_items(value: Any) -> bool:
    if not isinstance(value, list):
        return False
    for item in value:
        if isinstance(item, dict) and _coerce_float(item.get("confidence"), default=0.0) >= 0.6:
            return True
    return False


def _average_parser_confidence(parser_coverage: Iterable[dict[str, Any]]) -> float:
    confidences: list[float] = []
    for item in parser_coverage:
        if not isinstance(item, dict):
            continue
        confidence = item.get("parser_confidence")
        if confidence is None:
            continue
        confidences.append(_coerce_float(confidence, default=0.0))
    return round(mean(confidences), 3) if confidences else 0.0


def _coerce_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        text = value.get("text")
        return str(text).strip() if text is not None else ""
    return str(value).strip()


def _coerce_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _day_to_string(day: date | str) -> str:
    return day.isoformat() if isinstance(day, date) else str(day)


def _dedupe_preserve_order(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result
