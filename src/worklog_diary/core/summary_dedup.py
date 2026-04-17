from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any

from .batching import SummaryBatch
from .models import SummaryRecord

_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]+")


@dataclass(slots=True)
class SummaryDedupDecision:
    action: str
    reason: str
    similarity: float
    matched_summary_id: int | None = None


class SummaryDeduplicator:
    def __init__(
        self,
        *,
        suppress_threshold: float = 0.86,
        merge_threshold: float = 0.74,
        cooldown_seconds: int = 240,
        recent_compare_count: int = 5,
    ) -> None:
        self.suppress_threshold = max(0.0, min(1.0, float(suppress_threshold)))
        self.merge_threshold = max(0.0, min(self.suppress_threshold, float(merge_threshold)))
        self.cooldown_seconds = max(0, int(cooldown_seconds))
        self.recent_compare_count = max(1, int(recent_compare_count))

    def evaluate(
        self,
        *,
        batch: SummaryBatch,
        summary_text: str,
        summary_json: dict[str, Any],
        recent_summaries: list[SummaryRecord],
    ) -> SummaryDedupDecision:
        context = _batch_context(batch, summary_json)
        recent = recent_summaries[: self.recent_compare_count]
        best_similarity = 0.0
        best_record: SummaryRecord | None = None
        best_context_similarity = 0.0

        for record in recent:
            previous_context = _record_context(record)
            similarity = _summary_similarity(summary_text, record.summary_text)
            context_similarity = _context_similarity(context, previous_context)
            combined = max(similarity, context_similarity)
            if combined > best_similarity:
                best_similarity = combined
                best_record = record
                best_context_similarity = context_similarity

        if best_record is None:
            return SummaryDedupDecision(action="store", reason="no_recent_summary", similarity=0.0)

        gap_seconds = max(0.0, batch.start_ts - best_record.end_ts)
        same_activity_context = best_context_similarity >= 0.8
        within_cooldown = gap_seconds <= self.cooldown_seconds

        if same_activity_context and within_cooldown and best_similarity >= self.merge_threshold:
            return SummaryDedupDecision(
                action="merge_previous",
                reason="same_activity_continuation",
                similarity=best_similarity,
                matched_summary_id=best_record.id,
            )

        if same_activity_context and within_cooldown and best_similarity >= self.suppress_threshold:
            return SummaryDedupDecision(
                action="suppress",
                reason="too_similar_recent_summary",
                similarity=best_similarity,
                matched_summary_id=best_record.id,
            )

        if within_cooldown and best_similarity >= self.suppress_threshold:
            return SummaryDedupDecision(
                action="suppress",
                reason="too_similar_recent_summary",
                similarity=best_similarity,
                matched_summary_id=best_record.id,
            )

        return SummaryDedupDecision(action="store", reason="distinct_activity", similarity=best_similarity, matched_summary_id=best_record.id)


def _batch_context(batch: SummaryBatch, summary_json: dict[str, Any]) -> dict[str, Any]:
    segment = batch.activity_segments[0] if batch.activity_segments else None
    if segment is not None:
        return {
            "process_name": segment.dominant_process_name,
            "window_title": segment.dominant_window_title,
            "closure_reason": segment.closure_reason,
            "segment_id": segment.segment_id,
            "blocked": segment.blocked,
        }

    if batch.active_intervals:
        interval = batch.active_intervals[0]
        return {
            "process_name": interval.process_name,
            "window_title": interval.window_title,
            "closure_reason": "interval_only",
            "segment_id": None,
            "blocked": interval.blocked,
        }

    if batch.text_segments:
        segment = batch.text_segments[0]
        return {
            "process_name": segment.process_name,
            "window_title": segment.window_title,
            "closure_reason": "text_only",
            "segment_id": None,
            "blocked": False,
        }

    if batch.screenshots:
        screenshot = batch.screenshots[0]
        return {
            "process_name": screenshot.process_name,
            "window_title": screenshot.window_title,
            "closure_reason": "screenshot_only",
            "segment_id": None,
            "blocked": False,
        }

    return summary_json.get("source_context", {}) if isinstance(summary_json.get("source_context"), dict) else {}


def _record_context(record: SummaryRecord) -> dict[str, Any]:
    metadata = record.summary_json
    context = metadata.get("source_context") if isinstance(metadata, dict) else None
    if isinstance(context, dict):
        return context
    return {}


def _context_similarity(lhs: dict[str, Any], rhs: dict[str, Any]) -> float:
    if not lhs or not rhs:
        return 0.0

    process_similarity = 1.0 if str(lhs.get("process_name", "")) == str(rhs.get("process_name", "")) else 0.0
    title_similarity = _text_similarity(str(lhs.get("window_title", "")), str(rhs.get("window_title", "")))
    blocked_similarity = 1.0 if bool(lhs.get("blocked")) == bool(rhs.get("blocked")) else 0.0
    closure_similarity = 1.0 if str(lhs.get("closure_reason", "")) == str(rhs.get("closure_reason", "")) else 0.0
    return max(
        process_similarity * 0.6 + title_similarity * 0.3 + blocked_similarity * 0.1,
        process_similarity * 0.5 + title_similarity * 0.4 + closure_similarity * 0.1,
    )


def _summary_similarity(lhs: str, rhs: str) -> float:
    lhs_norm = _normalize_text(lhs)
    rhs_norm = _normalize_text(rhs)
    if not lhs_norm and not rhs_norm:
        return 1.0
    if not lhs_norm or not rhs_norm:
        return 0.0

    sequence = SequenceMatcher(a=lhs_norm, b=rhs_norm).ratio()
    lhs_tokens = set(lhs_norm.split())
    rhs_tokens = set(rhs_norm.split())
    if not lhs_tokens or not rhs_tokens:
        return sequence
    jaccard = len(lhs_tokens & rhs_tokens) / len(lhs_tokens | rhs_tokens)
    return max(sequence, jaccard)


def _text_similarity(lhs: str, rhs: str) -> float:
    return _summary_similarity(lhs, rhs)


def _normalize_text(value: str) -> str:
    stripped = value.strip().lower()
    stripped = _PUNCT_RE.sub(" ", stripped)
    return _WHITESPACE_RE.sub(" ", stripped).strip()
