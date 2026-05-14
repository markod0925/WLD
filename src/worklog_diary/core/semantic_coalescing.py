from __future__ import annotations

import json
import logging
import math
import re
import time
from dataclasses import dataclass, field
from datetime import date
from difflib import SequenceMatcher
from typing import Any, Protocol

from .models import SummaryRecord
from .internal_artifacts import is_internal_artifact_path

_TOKEN_RE = re.compile(r"\w+")
_WHITESPACE_RE = re.compile(r"\s+")
_GENERIC_WINDOW_TITLES = {
    "chromium",
    "edge",
    "file explorer",
    "google chrome",
    "matlab",
    "microsoft edge",
    "new tab",
    "simulink",
    "start page",
    "windows explorer",
}


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
            candidates = cluster[-max(1, int(self.config.max_neighbor_count)) :]
            candidate_features = [self._evaluate_pair(left, summary) for left in candidates]
            features = max(
                candidate_features,
                key=lambda item: (
                    item.decision == "merge",
                    item.final_score,
                    -item.gap_seconds,
                ),
            )
            if len(candidate_features) > 1 and features.decision == "merge" and features.left_summary_id != int(candidates[-1].id or 0):
                immediate = candidate_features[-1]
                if immediate.decision != "merge":
                    features = CandidateFeatures(
                        left_summary_id=features.left_summary_id,
                        right_summary_id=features.right_summary_id,
                        semantic_similarity=features.semantic_similarity,
                        app_similarity=features.app_similarity,
                        window_similarity=features.window_similarity,
                        keyword_overlap=features.keyword_overlap,
                        gap_seconds=features.gap_seconds,
                        blockers=list(features.blockers),
                        final_score=features.final_score,
                        decision="no_merge",
                        reasons=[*features.reasons, "transitive_bridge_blocked"],
                    )
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
        signature = _activity_signature_similarity(left, right)

        if gap_seconds > self.config.max_candidate_gap_seconds:
            blockers.append("gap_too_large")

        transition_keyword = self._has_transition_marker(left.summary_text) or self._has_transition_marker(right.summary_text)

        lock_boundary = self._is_lock_boundary(left, right)
        pause_boundary = self._is_pause_boundary(left, right)
        if self.config.lock_boundary_blocks_merge and lock_boundary and not signature["strong_continuity"]:
            blockers.append("lock_boundary")
        if self.config.pause_boundary_blocks_merge and pause_boundary and not signature["strong_continuity"]:
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

        semantic_gate_failed = semantic_similarity < self.config.min_cosine_similarity
        if semantic_gate_failed:
            reasons.append("below_min_cosine")

        temporal_penalty = min(1.0, gap_seconds / max(1.0, float(self.config.max_candidate_gap_seconds)))
        context_switch_penalty = 1.0 if app_similarity == 0.0 else 0.0
        transition_penalty = 0.18 if transition_keyword and not signature["strong_continuity"] else (0.08 if transition_keyword else 0.0)
        boundary_penalty = 0.0
        if lock_boundary and signature["strong_continuity"]:
            boundary_penalty += 0.08
            reasons.append("lock_boundary_penalty")
        if pause_boundary and signature["strong_continuity"]:
            boundary_penalty += 0.06
            reasons.append("pause_boundary_penalty")
        if transition_keyword:
            reasons.append("transition_keyword")

        final_score = (
            self.config.semantic_weight * semantic_similarity
            + self.config.same_app_boost * app_similarity
            + self.config.window_title_boost * window_similarity
            + self.config.keyword_overlap_boost * keyword_overlap
            + float(signature["score"])
            - self.config.temporal_gap_penalty_weight * temporal_penalty
            - self.config.app_switch_penalty * context_switch_penalty
            - transition_penalty
            - boundary_penalty
        )
        effective_min_merge_score = (
            min(self.config.min_merge_score, 0.70)
            if signature["has_entity_overlap"]
            else max(self.config.min_merge_score, 0.80)
        )

        if blockers:
            decision = "no_merge"
            reasons.extend(blockers)
        elif not bool(signature["concrete_overlap"]) and not bool(signature["same_window"]):
            decision = "no_merge"
            reasons.append("no_concrete_overlap")
        elif semantic_gate_failed and not signature["strong_continuity"]:
            decision = "no_merge"
        elif final_score >= effective_min_merge_score and (not semantic_gate_failed or signature["has_entity_overlap"]):
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
        task_candidates = _merge_cluster_values(
            _collect_cluster_values(cluster, ["task_candidates", "jira_update_candidates"], limit=6),
            _collect_cluster_entity_values(cluster, {"task_candidate"}, limit=6),
            limit=6,
        )
        files_and_documents = _merge_cluster_values(
            _collect_cluster_values(cluster, ["files_and_documents", "files"], limit=6),
            _collect_cluster_entity_values(cluster, {"file_path", "file_name"}, limit=6),
            limit=6,
        )
        programs_used = _merge_cluster_values(
            _collect_cluster_values(cluster, ["programs_used"], limit=4),
            _collect_cluster_entity_values(cluster, {"program"}, limit=4),
            limit=4,
        )
        conversations_or_references = _collect_cluster_values(
            cluster,
            ["conversations_or_references", "conversations"],
            limit=4,
        )
        outcomes = _collect_cluster_values(cluster, ["outcomes", "follow_ups"], limit=6)
        unknowns_and_privacy_limits = _collect_cluster_values(
            cluster,
            ["unknowns_and_privacy_limits", "unknowns", "blocked_activity", "blocked_observed_references"],
            limit=6,
        )
        activity_entities = _collect_cluster_activity_entities(cluster)
        merged_text = _compose_coalesced_text(
            task_candidates=task_candidates,
            files_and_documents=files_and_documents,
            programs_used=programs_used,
            outcomes=outcomes,
            unknowns_and_privacy_limits=unknowns_and_privacy_limits,
            fallback_texts=[item.summary_text for item in cluster],
        )

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
            "task_candidates": task_candidates,
            "files_and_documents": files_and_documents,
            "programs_used": programs_used,
            "conversations_or_references": conversations_or_references,
            "outcomes": outcomes,
            "unknowns_and_privacy_limits": unknowns_and_privacy_limits,
            "activity_entities": activity_entities,
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


def _collect_cluster_values(cluster: list[SummaryRecord], keys: list[str], *, limit: int) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for record in cluster:
        if not isinstance(record.summary_json, dict):
            continue
        for key in keys:
            for value in _flatten_cluster_value(record.summary_json.get(key)):
                normalized = value.lower()
                if is_internal_artifact_path(value):
                    continue
                if normalized in seen:
                    continue
                seen.add(normalized)
                values.append(value)
                if len(values) >= limit:
                    return values
    return values


def _collect_cluster_activity_entities(cluster: list[SummaryRecord], *, limit: int = 24) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for record in cluster:
        if not isinstance(record.summary_json, dict):
            continue
        activity_entities = record.summary_json.get("activity_entities")
        if not isinstance(activity_entities, list):
            continue
        for item in activity_entities:
            if not isinstance(item, dict):
                continue
            entity_type = str(item.get("entity_type") or "").strip().lower()
            entity_normalized = str(item.get("entity_normalized") or item.get("entity_value") or "").strip().lower()
            if not entity_type or not entity_normalized:
                continue
            if entity_type in {"file_path", "file_name", "folder_path", "project_candidate"} and is_internal_artifact_path(str(item.get("entity_value") or entity_normalized)):
                continue
            marker = (entity_type, entity_normalized)
            if marker in seen:
                continue
            seen.add(marker)
            rows.append(dict(item))
            if len(rows) >= limit:
                return rows
    return rows


def _collect_cluster_entity_values(cluster: list[SummaryRecord], entity_types: set[str], *, limit: int) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for record in cluster:
        if not isinstance(record.summary_json, dict):
            continue
        activity_entities = record.summary_json.get("activity_entities")
        if not isinstance(activity_entities, list):
            continue
        for item in activity_entities:
            if not isinstance(item, dict):
                continue
            entity_type = str(item.get("entity_type") or "").strip().lower()
            if entity_type not in entity_types:
                continue
            value = str(item.get("entity_value") or "").strip()
            if not value:
                continue
            if entity_type in {"file_path", "file_name", "folder_path", "project_candidate"} and is_internal_artifact_path(value):
                continue
            normalized = str(item.get("entity_normalized") or value).strip().lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            values.append(value)
            if len(values) >= limit:
                return values
    return values


def _merge_cluster_values(primary: list[str], secondary: list[str], *, limit: int) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for value in primary + secondary:
        normalized = value.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        merged.append(value)
        if len(merged) >= limit:
            break
    return merged


def _flatten_cluster_value(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        cleaned = _WHITESPACE_RE.sub(" ", value).strip()
        return [cleaned] if cleaned else []
    if isinstance(value, list):
        items: list[str] = []
        for entry in value:
            items.extend(_flatten_cluster_value(entry))
        return items
    if isinstance(value, dict):
        for key in ("text", "path/name", "path", "name", "reference", "program", "window_title"):
            if key in value and isinstance(value[key], str):
                cleaned = _WHITESPACE_RE.sub(" ", value[key]).strip()
                return [cleaned] if cleaned else []
        compact = json.dumps(value, ensure_ascii=False, sort_keys=True)
        compact = _WHITESPACE_RE.sub(" ", compact).strip()
        return [compact] if compact else []
    compact = _WHITESPACE_RE.sub(" ", str(value)).strip()
    return [compact] if compact else []


def _compose_coalesced_text(
    *,
    task_candidates: list[str],
    files_and_documents: list[str],
    programs_used: list[str],
    outcomes: list[str],
    unknowns_and_privacy_limits: list[str],
    fallback_texts: list[str],
) -> str:
    subject_from_tasks = bool(task_candidates)
    subjects = task_candidates[:2] if subject_from_tasks else files_and_documents[:2]
    if subjects:
        summary = f"Continued work on {', '.join(subjects)}."
    elif files_and_documents:
        summary = f"Continued work with {', '.join(files_and_documents[:2])}."
    elif programs_used:
        summary = f"Continued work in {', '.join(programs_used[:2])}."
    else:
        unique_text = _dedupe_texts(fallback_texts)
        summary = unique_text[0] if unique_text else "Continued work across related summaries."

    extras: list[str] = []
    if files_and_documents and subject_from_tasks:
        extras.append(f"Files: {', '.join(files_and_documents[:2])}.")
    if programs_used:
        extras.append(f"Programs: {', '.join(programs_used[:2])}.")
    if outcomes:
        extras.append(f"Outcome: {outcomes[0]}.")
    if unknowns_and_privacy_limits:
        extras.append("Privacy-limited observations remained separate.")
    return _WHITESPACE_RE.sub(" ", " ".join([summary] + extras)).strip()


def _dedupe_texts(values: list[str]) -> list[str]:
    items: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = _WHITESPACE_RE.sub(" ", value).strip()
        if not cleaned:
            continue
        normalized = cleaned.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        items.append(cleaned)
    return items


def _activity_signature_similarity(left: SummaryRecord, right: SummaryRecord) -> dict[str, float | bool]:
    left_signature = _record_signature(left)
    right_signature = _record_signature(right)

    same_file_path = bool(left_signature["file_paths"] & right_signature["file_paths"])
    same_file_name = bool(left_signature["file_names"] & right_signature["file_names"])
    same_folder = bool(left_signature["folders"] & right_signature["folders"])
    same_task = bool(left_signature["tasks"] & right_signature["tasks"])
    same_program = bool(left_signature["programs"] & right_signature["programs"])
    same_window = _has_meaningful_window_overlap(left_signature["windows"], right_signature["windows"])
    same_file_name_compatible = same_file_name and (same_folder or same_task or same_window)

    score = 0.0
    if same_file_path:
        score += 0.38
    if same_file_name_compatible:
        score += 0.18
    if same_folder:
        score += 0.14
    if same_task:
        score += 0.16
    if same_program:
        score += 0.08
    if same_window:
        score += 0.08

    concrete_overlap = same_file_path or same_file_name_compatible or same_folder or same_task
    has_entity_overlap = concrete_overlap or same_window
    strong_continuity = concrete_overlap and (same_program or same_window or same_task or same_folder)
    return {
        "score": score,
        "has_entity_overlap": has_entity_overlap,
        "strong_continuity": strong_continuity,
        "concrete_overlap": concrete_overlap,
        "same_window": same_window,
    }


def _record_signature(record: SummaryRecord) -> dict[str, set[str]]:
    signature = {
        "file_paths": set(),
        "file_names": set(),
        "folders": set(),
        "tasks": set(),
        "programs": set(),
        "windows": set(),
    }
    if not isinstance(record.summary_json, dict):
        return signature

    activity_entities = record.summary_json.get("activity_entities")
    if isinstance(activity_entities, list):
        for item in activity_entities:
            if not isinstance(item, dict):
                continue
            entity_type = str(item.get("entity_type") or "").strip().lower()
            normalized = str(item.get("entity_normalized") or item.get("entity_value") or "").strip().lower()
            if not normalized:
                continue
            if entity_type in {"file_path", "file_name", "folder_path", "project_candidate"} and is_internal_artifact_path(str(item.get("entity_value") or normalized)):
                continue
            if entity_type == "file_path":
                signature["file_paths"].add(normalized)
            elif entity_type == "file_name":
                signature["file_names"].add(normalized)
            elif entity_type in {"folder_path", "project_candidate"}:
                signature["folders"].add(normalized)
            elif entity_type == "task_candidate":
                signature["tasks"].add(normalized)
            elif entity_type == "program":
                signature["programs"].add(normalized)
            elif entity_type == "window_title":
                signature["windows"].add(_canonical_window_title(normalized))

    process_name = _summary_context_value(record, "process_name")
    if process_name:
        signature["programs"].add(process_name)
    window_title = _summary_context_value(record, "window_title")
    if window_title:
        signature["windows"].add(_canonical_window_title(window_title))
    return signature


def _canonical_window_title(value: str) -> str:
    stripped = value.strip().lower()
    for suffix in (" - google chrome", " - chromium", " - microsoft edge", " - edge"):
        if stripped.endswith(suffix):
            return stripped[: -len(suffix)].strip()
    return stripped


def _has_meaningful_window_overlap(left_windows: set[str], right_windows: set[str]) -> bool:
    shared = left_windows & right_windows
    return any(not _is_generic_window_title(title) for title in shared)


def _is_generic_window_title(value: str) -> bool:
    title = value.strip().lower()
    if not title:
        return True
    return title in _GENERIC_WINDOW_TITLES


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
