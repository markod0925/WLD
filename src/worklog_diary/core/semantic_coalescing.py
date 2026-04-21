from __future__ import annotations

import json
import logging
import math
import re
import time
from dataclasses import dataclass, field
from datetime import date
from difflib import SequenceMatcher
from typing import Protocol

from .models import SummaryRecord

_TOKEN_RE = re.compile(r"\w+")
_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(slots=True)
class SemanticCoalescingConfig:
    enabled: bool = False
    embedding_base_url: str = "http://127.0.0.1:1234/v1"
    embedding_model: str = "text-embedding-nomic-embed-text-v1.5"
    max_candidate_gap_seconds: int = 900
    max_neighbor_count: int = 2
    min_cosine_similarity: float = 0.90
    min_merge_score: float = 0.85
    semantic_weight: float = 0.45
    same_app_boost: float = 0.20
    window_title_boost: float = 0.10
    keyword_overlap_boost: float = 0.10
    temporal_gap_penalty_weight: float = 0.12
    app_switch_penalty: float = 0.20
    lock_boundary_blocks_merge: bool = True
    pause_boundary_blocks_merge: bool = True
    transition_keywords: list[str] = field(
        default_factory=lambda: [
            "then",
            "afterward",
            "next",
            "switched",
            "meeting",
            "call",
            "pausa",
            "riunione",
            "poi",
            "successivamente",
        ]
    )
    store_merge_diagnostics: bool = True
    recompute_missing_embeddings_on_startup: bool = False


@dataclass(slots=True)
class CandidateFeatures:
    left_summary_id: int
    right_summary_id: int
    semantic_similarity: float
    app_similarity: float
    window_similarity: float
    keyword_overlap: float
    gap_seconds: float
    blockers: list[str]
    final_score: float
    decision: str
    reasons: list[str]


@dataclass(slots=True)
class CoalescedSummaryPlan:
    start_ts: float
    end_ts: float
    summary_text: str
    summary_json: dict[str, object]
    source_summary_ids: list[int]


class EmbeddingProvider(Protocol):
    def embedding_for_summary(self, summary: SummaryRecord) -> list[float] | None: ...


class SemanticCoalescingEngine:
    def __init__(
        self,
        *,
        config: SemanticCoalescingConfig,
        embedding_provider: EmbeddingProvider,
        logger: logging.Logger | None = None,
    ) -> None:
        self.config = config
        self.embedding_provider = embedding_provider
        self.logger = logger or logging.getLogger(__name__)

    def build_coalesced_plans(
        self,
        summaries: list[SummaryRecord],
    ) -> tuple[list[CoalescedSummaryPlan], list[CandidateFeatures]]:
        if not summaries:
            return [], []

        ordered = sorted(summaries, key=lambda item: (item.start_ts, item.end_ts, int(item.id or 0)))
        diagnostics: list[CandidateFeatures] = []
        plans: list[CoalescedSummaryPlan] = []

        cluster: list[SummaryRecord] = [ordered[0]]
        for summary in ordered[1:]:
            left = cluster[-1]
            features = self._evaluate_pair(left, summary)
            diagnostics.append(features)
            if features.decision == "merge":
                cluster.append(summary)
            else:
                plans.append(self._compose_cluster(cluster))
                cluster = [summary]
        plans.append(self._compose_cluster(cluster))
        return plans, diagnostics

    def _evaluate_pair(self, left: SummaryRecord, right: SummaryRecord) -> CandidateFeatures:
        gap_seconds = max(0.0, right.start_ts - left.end_ts)
        blockers: list[str] = []
        reasons: list[str] = []

        if gap_seconds > self.config.max_candidate_gap_seconds:
            blockers.append("gap_too_large")

        if self._has_transition_marker(left.summary_text) or self._has_transition_marker(right.summary_text):
            blockers.append("transition_keyword")

        if self.config.lock_boundary_blocks_merge and self._is_lock_boundary(left, right):
            blockers.append("lock_boundary")
        if self.config.pause_boundary_blocks_merge and self._is_pause_boundary(left, right):
            blockers.append("pause_boundary")
        if self._is_force_boundary(left, right):
            blockers.append("forced_boundary")

        left_app = _summary_context_value(left, "process_name")
        right_app = _summary_context_value(right, "process_name")
        app_similarity = 1.0 if left_app and left_app == right_app else 0.0

        left_window = _summary_context_value(left, "window_title")
        right_window = _summary_context_value(right, "window_title")
        window_similarity = _text_similarity(left_window, right_window)
        keyword_overlap = _keyword_overlap(left.summary_text, right.summary_text)

        semantic_similarity = 0.0
        left_embedding = self.embedding_provider.embedding_for_summary(left)
        right_embedding = self.embedding_provider.embedding_for_summary(right)
        if left_embedding and right_embedding:
            semantic_similarity = _cosine_similarity(left_embedding, right_embedding)
        else:
            reasons.append("embedding_unavailable")

        if semantic_similarity < self.config.min_cosine_similarity:
            reasons.append("below_min_cosine")

        temporal_penalty = min(1.0, gap_seconds / max(1.0, float(self.config.max_candidate_gap_seconds)))
        context_switch_penalty = 1.0 if app_similarity == 0.0 else 0.0

        final_score = (
            self.config.semantic_weight * semantic_similarity
            + self.config.same_app_boost * app_similarity
            + self.config.window_title_boost * window_similarity
            + self.config.keyword_overlap_boost * keyword_overlap
            - self.config.temporal_gap_penalty_weight * temporal_penalty
            - self.config.app_switch_penalty * context_switch_penalty
        )

        if blockers:
            decision = "no_merge"
            reasons.extend(blockers)
        elif semantic_similarity < self.config.min_cosine_similarity:
            decision = "no_merge"
        elif final_score >= self.config.min_merge_score:
            decision = "merge"
            reasons.append("score_threshold_met")
        else:
            decision = "no_merge"
            reasons.append("score_below_threshold")

        return CandidateFeatures(
            left_summary_id=int(left.id or 0),
            right_summary_id=int(right.id or 0),
            semantic_similarity=semantic_similarity,
            app_similarity=app_similarity,
            window_similarity=window_similarity,
            keyword_overlap=keyword_overlap,
            gap_seconds=gap_seconds,
            blockers=blockers,
            final_score=final_score,
            decision=decision,
            reasons=reasons,
        )

    def _compose_cluster(self, cluster: list[SummaryRecord]) -> CoalescedSummaryPlan:
        if len(cluster) == 1:
            item = cluster[0]
            return CoalescedSummaryPlan(
                start_ts=item.start_ts,
                end_ts=item.end_ts,
                summary_text=item.summary_text,
                summary_json=dict(item.summary_json),
                source_summary_ids=[int(item.id or 0)],
            )

        source_ids = [int(item.id or 0) for item in cluster]
        ordered_text = [item.summary_text.strip() for item in cluster if item.summary_text.strip()]
        unique_text: list[str] = []
        for text in ordered_text:
            if text not in unique_text:
                unique_text.append(text)
        merged_text = " Then ".join(unique_text)
        merged_text = _WHITESPACE_RE.sub(" ", merged_text).strip()

        app_values = [_summary_context_value(item, "process_name") for item in cluster if _summary_context_value(item, "process_name")]
        window_values = [_summary_context_value(item, "window_title") for item in cluster if _summary_context_value(item, "window_title")]

        source_context = {
            "process_name": _representative_or_mixed(app_values),
            "window_title": _representative_or_mixed(window_values),
            "coalesced": True,
            "source_summary_ids": source_ids,
        }
        summary_json: dict[str, object] = {
            "summary_text": merged_text,
            "source_context": source_context,
            "coalesced_from": source_ids,
            "coalesced_count": len(source_ids),
        }
        return CoalescedSummaryPlan(
            start_ts=min(item.start_ts for item in cluster),
            end_ts=max(item.end_ts for item in cluster),
            summary_text=merged_text,
            summary_json=summary_json,
            source_summary_ids=source_ids,
        )

    def _has_transition_marker(self, value: str) -> bool:
        lowered = value.lower()
        return any(keyword.lower() in lowered for keyword in self.config.transition_keywords)

    def _is_lock_boundary(self, left: SummaryRecord, right: SummaryRecord) -> bool:
        keys = {"lock_state_changed", "session_locked", "session_unlocked"}
        return _has_closure_reason(left, keys) or _has_closure_reason(right, keys)

    def _is_pause_boundary(self, left: SummaryRecord, right: SummaryRecord) -> bool:
        keys = {"idle_gap", "manual_pause", "paused"}
        return _has_closure_reason(left, keys) or _has_closure_reason(right, keys)

    def _is_force_boundary(self, left: SummaryRecord, right: SummaryRecord) -> bool:
        for record in (left, right):
            marker = record.summary_json.get("boundary") if isinstance(record.summary_json, dict) else None
            if marker:
                return True
            source_batch = record.summary_json.get("source_batch") if isinstance(record.summary_json, dict) else None
            if isinstance(source_batch, dict) and bool(source_batch.get("force_flush", False)):
                return True
        return False


def _summary_context_value(record: SummaryRecord, key: str) -> str:
    if not isinstance(record.summary_json, dict):
        return ""
    context = record.summary_json.get("source_context")
    if isinstance(context, dict):
        value = context.get(key)
        return str(value).strip().lower() if value is not None else ""
    return ""


def _has_closure_reason(record: SummaryRecord, values: set[str]) -> bool:
    if not isinstance(record.summary_json, dict):
        return False
    context = record.summary_json.get("source_context")
    if not isinstance(context, dict):
        return False
    closure_reason = str(context.get("closure_reason", "")).strip().lower()
    return closure_reason in values


def _keyword_overlap(lhs: str, rhs: str) -> float:
    lhs_tokens = set(_TOKEN_RE.findall(lhs.lower()))
    rhs_tokens = set(_TOKEN_RE.findall(rhs.lower()))
    if not lhs_tokens or not rhs_tokens:
        return 0.0
    return len(lhs_tokens & rhs_tokens) / len(lhs_tokens | rhs_tokens)


def _text_similarity(lhs: str, rhs: str) -> float:
    if not lhs and not rhs:
        return 1.0
    if not lhs or not rhs:
        return 0.0
    return SequenceMatcher(a=lhs, b=rhs).ratio()


def _cosine_similarity(lhs: list[float], rhs: list[float]) -> float:
    if len(lhs) != len(rhs) or not lhs:
        return 0.0
    dot = sum(a * b for a, b in zip(lhs, rhs, strict=False))
    norm_l = math.sqrt(sum(a * a for a in lhs))
    norm_r = math.sqrt(sum(b * b for b in rhs))
    if norm_l <= 0 or norm_r <= 0:
        return 0.0
    return dot / (norm_l * norm_r)


def _representative_or_mixed(values: list[str]) -> str:
    if not values:
        return ""
    distinct = list(dict.fromkeys(values))
    if len(distinct) == 1:
        return distinct[0]
    return "mixed"


class SemanticCoalescer:
    def __init__(
        self,
        *,
        storage: object,
        engine: SemanticCoalescingEngine,
        diagnostics_enabled: bool,
        logger: logging.Logger | None = None,
    ) -> None:
        self.storage = storage
        self.engine = engine
        self.diagnostics_enabled = diagnostics_enabled
        self.logger = logger or logging.getLogger(__name__)

    @property
    def enabled(self) -> bool:
        return self.engine.config.enabled

    def refresh_day(self, day: date) -> None:
        if not self.enabled:
            return
        self.logger.info("event=semantic_coalescing_run_start day=%s", day.isoformat())
        summaries = self.storage.list_summaries_for_day(day)
        plans, diagnostics = self.engine.build_coalesced_plans(summaries)
        merged_cluster_count = sum(1 for plan in plans if len(plan.source_summary_ids) > 1)
        merged_source_count = sum(max(0, len(plan.source_summary_ids) - 1) for plan in plans)
        coalesced_ids = self.storage.replace_coalesced_summaries_for_day(day, plans)
        if self.diagnostics_enabled:
            self.storage.replace_coalescing_diagnostics_for_day(day, diagnostics)
        if merged_cluster_count > 0:
            for coalesced_id, plan in zip(coalesced_ids, plans, strict=False):
                if len(plan.source_summary_ids) <= 1:
                    continue
                representative_score = _representative_cluster_score(plan.source_summary_ids, diagnostics)
                context = plan.summary_json.get("source_context", {}) if isinstance(plan.summary_json, dict) else {}
                app_label = ""
                if isinstance(context, dict):
                    app_label = str(context.get("process_name", "")).strip()
                self.logger.info(
                    "event=semantic_coalescing_merge merged_count=%s coalesced_summary_id=%s representative_score=%.3f app=%s",
                    len(plan.source_summary_ids),
                    coalesced_id,
                    representative_score,
                    app_label or "unknown",
                )
        else:
            degraded = any("embedding_unavailable" in item.reasons for item in diagnostics)
            self.logger.info(
                "event=semantic_coalescing_no_merge comparisons=%s merges=0 embedding_degraded=%s",
                len(diagnostics),
                degraded,
            )
        self.logger.info(
            "event=semantic_coalescing_complete day=%s source_count=%s coalesced_count=%s merged_clusters=%s merged_source_rows=%s diagnostic_rows=%s",
            day.isoformat(),
            len(summaries),
            len(plans),
            merged_cluster_count,
            merged_source_count,
            len(diagnostics),
        )
        ratio = (len(plans) / len(summaries)) if summaries else 0.0
        self.logger.info(
            "event=semantic_coalescing_day_compression day=%s original=%s coalesced=%s ratio=%.2f",
            day.isoformat(),
            len(summaries),
            len(plans),
            ratio,
        )


def _representative_cluster_score(source_ids: list[int], diagnostics: list[CandidateFeatures]) -> float:
    if len(source_ids) <= 1:
        return 0.0
    source_set = set(source_ids)
    scores = [
        item.final_score
        for item in diagnostics
        if item.decision == "merge"
        and item.left_summary_id in source_set
        and item.right_summary_id in source_set
    ]
    if not scores:
        return 0.0
    return max(scores)
