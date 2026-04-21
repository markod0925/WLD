from __future__ import annotations

from datetime import date

from worklog_diary.core.models import CoalescingDiagnosticRecord, SummaryRecord
from worklog_diary.ui.semantic_diagnostics_view_model import (
    build_coalesced_traceability_map,
    build_semantic_diagnostics_rows,
    confidence_bucket_for_score,
    sort_semantic_diagnostics,
)


def test_build_semantic_diagnostics_rows_formats_expected_columns() -> None:
    rows = build_semantic_diagnostics_rows(
        [
            CoalescingDiagnosticRecord(
                id=1,
                day=date(2026, 4, 10),
                left_summary_id=10,
                right_summary_id=11,
                embedding_cosine_similarity=0.93,
                app_similarity_score=1.0,
                window_similarity_score=0.75,
                keyword_overlap_score=0.5,
                temporal_gap_seconds=62.0,
                blockers_json=["lock_boundary"],
                final_merge_score=0.88,
                decision="merge",
                reasons_json=["score_threshold_met"],
                created_ts=1.0,
            )
        ]
    )
    assert len(rows) == 1
    assert rows[0].pair_label == "10→11"
    assert rows[0].decision == "merge"
    assert rows[0].score_label == "0.880"
    assert rows[0].cosine_label == "0.930"
    assert rows[0].blockers_label == "lock_boundary"
    assert rows[0].reasons_label == "score_threshold_met"


def test_confidence_bucket_classification() -> None:
    assert confidence_bucket_for_score(0.93) == "High"
    assert confidence_bucket_for_score(0.85) == "Medium"
    assert confidence_bucket_for_score(0.84) == "Low"


def test_traceability_mapping_links_coalesced_summary_to_diagnostics() -> None:
    summaries = [
        SummaryRecord(
            id=101,
            job_id=-1,
            start_ts=1.0,
            end_ts=2.0,
            summary_text="merged block",
            summary_json={"coalesced_from": [10, 11, 12], "coalesced_count": 3},
            created_ts=3.0,
        )
    ]
    diagnostics = [
        CoalescingDiagnosticRecord(
            id=1,
            day=date(2026, 4, 10),
            left_summary_id=10,
            right_summary_id=11,
            embedding_cosine_similarity=0.94,
            app_similarity_score=1.0,
            window_similarity_score=0.9,
            keyword_overlap_score=0.6,
            temporal_gap_seconds=30.0,
            blockers_json=[],
            final_merge_score=0.93,
            decision="merge",
            reasons_json=["score_threshold_met"],
            created_ts=1.0,
        ),
        CoalescingDiagnosticRecord(
            id=2,
            day=date(2026, 4, 10),
            left_summary_id=11,
            right_summary_id=12,
            embedding_cosine_similarity=0.88,
            app_similarity_score=1.0,
            window_similarity_score=0.8,
            keyword_overlap_score=0.5,
            temporal_gap_seconds=20.0,
            blockers_json=[],
            final_merge_score=0.87,
            decision="merge",
            reasons_json=["score_threshold_met"],
            created_ts=2.0,
        ),
    ]
    mapping = build_coalesced_traceability_map(summaries, diagnostics)
    info = mapping[101]
    assert info.source_summary_ids == [10, 11, 12]
    assert info.diagnostics_count == 2
    assert info.representative_score == 0.93
    assert info.confidence_bucket == "High"


def test_sort_semantic_diagnostics_by_score() -> None:
    rows = [
        CoalescingDiagnosticRecord(
            id=1,
            day=date(2026, 4, 10),
            left_summary_id=1,
            right_summary_id=2,
            embedding_cosine_similarity=0.91,
            app_similarity_score=1.0,
            window_similarity_score=1.0,
            keyword_overlap_score=0.5,
            temporal_gap_seconds=40.0,
            blockers_json=[],
            final_merge_score=0.80,
            decision="merge",
            reasons_json=[],
            created_ts=1.0,
        ),
        CoalescingDiagnosticRecord(
            id=2,
            day=date(2026, 4, 10),
            left_summary_id=3,
            right_summary_id=4,
            embedding_cosine_similarity=0.95,
            app_similarity_score=1.0,
            window_similarity_score=1.0,
            keyword_overlap_score=0.5,
            temporal_gap_seconds=10.0,
            blockers_json=[],
            final_merge_score=0.93,
            decision="merge",
            reasons_json=[],
            created_ts=2.0,
        ),
    ]
    sorted_rows = sort_semantic_diagnostics(rows, key="merge_score", descending=True)
    assert [row.id for row in sorted_rows] == [2, 1]
