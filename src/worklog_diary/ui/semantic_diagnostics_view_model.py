from __future__ import annotations

from dataclasses import dataclass

from ..core.models import CoalescingDiagnosticRecord, SummaryRecord


@dataclass(slots=True)
class SemanticDiagnosticsTableRow:
    pair_label: str
    decision: str
    score_label: str
    cosine_label: str
    app_label: str
    window_label: str
    gap_label: str
    blockers_label: str
    reasons_label: str


@dataclass(slots=True)
class CoalescedTraceabilityInfo:
    source_summary_ids: list[int]
    representative_score: float
    confidence_bucket: str
    diagnostics_count: int


def build_semantic_diagnostics_rows(rows: list[CoalescingDiagnosticRecord]) -> list[SemanticDiagnosticsTableRow]:
    result: list[SemanticDiagnosticsTableRow] = []
    for item in rows:
        blockers = ", ".join(item.blockers_json)
        reasons = ", ".join(item.reasons_json)
        result.append(
            SemanticDiagnosticsTableRow(
                pair_label=f"{item.left_summary_id}→{item.right_summary_id}",
                decision=item.decision,
                score_label=f"{item.final_merge_score:.3f}",
                cosine_label=f"{item.embedding_cosine_similarity:.3f}",
                app_label=f"{item.app_similarity_score:.3f}",
                window_label=f"{item.window_similarity_score:.3f}",
                gap_label=f"{item.temporal_gap_seconds:.1f}",
                blockers_label=blockers,
                reasons_label=reasons,
            )
        )
    return result


def sort_semantic_diagnostics(
    rows: list[CoalescingDiagnosticRecord],
    *,
    key: str,
    descending: bool = True,
) -> list[CoalescingDiagnosticRecord]:
    key_fn = {
        "merge_score": lambda item: item.final_merge_score,
        "semantic_similarity": lambda item: item.embedding_cosine_similarity,
        "temporal_gap": lambda item: item.temporal_gap_seconds,
    }.get(key, lambda item: item.id or 0)
    return sorted(rows, key=key_fn, reverse=descending)


def confidence_bucket_for_score(score: float) -> str:
    if score >= 0.92:
        return "High"
    if score >= 0.85:
        return "Medium"
    return "Low"


def build_coalesced_traceability_map(
    summaries: list[SummaryRecord],
    diagnostics: list[CoalescingDiagnosticRecord],
) -> dict[int, CoalescedTraceabilityInfo]:
    result: dict[int, CoalescedTraceabilityInfo] = {}
    for summary in summaries:
        summary_id = int(summary.id or 0)
        if summary_id <= 0:
            continue
        payload = summary.summary_json if isinstance(summary.summary_json, dict) else {}
        source_ids_raw = payload.get("coalesced_from")
        if not isinstance(source_ids_raw, list) or len(source_ids_raw) < 2:
            continue
        source_ids = [int(item) for item in source_ids_raw if isinstance(item, int) or str(item).isdigit()]
        if len(source_ids) < 2:
            continue
        source_set = set(source_ids)
        matched = [
            row
            for row in diagnostics
            if row.decision == "merge"
            and row.left_summary_id in source_set
            and row.right_summary_id in source_set
        ]
        representative = max((row.final_merge_score for row in matched), default=0.0)
        result[summary_id] = CoalescedTraceabilityInfo(
            source_summary_ids=source_ids,
            representative_score=representative,
            confidence_bucket=confidence_bucket_for_score(representative),
            diagnostics_count=len(matched),
        )
    return result
