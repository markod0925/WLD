from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from html import escape
from typing import Any

from ..core.models import DailySummaryRecord, SummaryRecord
from .semantic_diagnostics_view_model import CoalescedTraceabilityInfo


@dataclass(slots=True)
class SummaryCardView:
    summary_id: int | None
    time_range: str
    summary_text: str
    major_activities: list[str]
    blocked_notes: list[str]
    uncertainty_notes: list[str]
    is_coalesced: bool = False
    coalesced_member_count: int = 0
    coalesced_source_ids: list[int] | None = None
    confidence_bucket: str | None = None


@dataclass(slots=True)
class DaySummaryView:
    day: date
    cards: list[SummaryCardView]
    has_daily_recap: bool
    daily_recap_text: str | None
    daily_recap_created_label: str | None


def build_calendar_highlight_days(days: list[date]) -> set[date]:
    return set(days)


def build_day_summary_view(
    *,
    day: date,
    summaries: list[SummaryRecord],
    daily_summary: DailySummaryRecord | None,
    traceability_by_summary_id: dict[int, CoalescedTraceabilityInfo] | None = None,
) -> DaySummaryView:
    # Sort by the time range the summary covers, not by when the row was generated.
    cards = [
        build_summary_card_view(item, traceability=traceability_by_summary_id or {})
        for item in sorted(summaries, key=_summary_sort_key, reverse=True)
    ]
    recap_created_label: str | None = None
    if daily_summary is not None:
        recap_created_label = datetime.fromtimestamp(daily_summary.created_ts).strftime("%Y-%m-%d %H:%M:%S")

    return DaySummaryView(
        day=day,
        cards=cards,
        has_daily_recap=daily_summary is not None,
        daily_recap_text=daily_summary.recap_text if daily_summary is not None else None,
        daily_recap_created_label=recap_created_label,
    )


def build_summary_card_view(
    summary: SummaryRecord,
    traceability: dict[int, CoalescedTraceabilityInfo] | None = None,
) -> SummaryCardView:
    start = datetime.fromtimestamp(summary.start_ts).strftime("%H:%M:%S")
    end = datetime.fromtimestamp(summary.end_ts).strftime("%H:%M:%S")

    structured = summary.summary_json if isinstance(summary.summary_json, dict) else {}
    major_activities = _extract_string_list(structured, keys=["major_activities", "key_points", "activities"])
    blocked_notes = _extract_string_list(structured, keys=["blocked_activity", "blocked_note", "privacy_notes"])
    uncertainty_notes = _extract_string_list(
        structured,
        keys=["uncertainty", "notes", "assumptions", "uncertainty_notes"],
    )

    coalesced_count_raw = structured.get("coalesced_count")
    try:
        coalesced_member_count = int(coalesced_count_raw) if coalesced_count_raw is not None else 0
    except (TypeError, ValueError):
        coalesced_member_count = 0

    return SummaryCardView(
        summary_id=summary.id,
        time_range=f"{start} - {end}",
        summary_text=summary.summary_text.strip(),
        major_activities=major_activities,
        blocked_notes=blocked_notes,
        uncertainty_notes=uncertainty_notes,
        is_coalesced=bool(structured.get("coalesced_from")),
        coalesced_member_count=coalesced_member_count,
        coalesced_source_ids=(
            traceability.get(int(summary.id or 0)).source_summary_ids
            if traceability and int(summary.id or 0) in traceability
            else None
        ),
        confidence_bucket=(
            traceability.get(int(summary.id or 0)).confidence_bucket
            if traceability and int(summary.id or 0) in traceability
            else None
        ),
    )


def _summary_sort_key(summary: SummaryRecord) -> tuple[float, float, int]:
    summary_id = int(summary.id or 0)
    return (summary.end_ts, summary.start_ts, summary_id)


def _extract_string_list(payload: dict[str, Any], keys: list[str]) -> list[str]:
    for key in keys:
        if key not in payload:
            continue
        value = payload[key]
        values = _flatten_string_values(value)
        if values:
            return values
    return []


def _flatten_string_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        cleaned = value.strip()
        return [cleaned] if cleaned else []
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            result.extend(_flatten_string_values(item))
        return [item for item in result if item]
    if isinstance(value, dict):
        if "text" in value and isinstance(value["text"], str):
            cleaned = value["text"].strip()
            return [cleaned] if cleaned else []
        compact = json.dumps(value, ensure_ascii=True, sort_keys=True)
        return [compact]
    compact = str(value).strip()
    return [compact] if compact else []


def format_summary_html(text: str, query: str | None) -> str:
    if not query:
        return f"Summary: {escape(text)}"
    cleaned_query = query.strip()
    if not cleaned_query:
        return f"Summary: {escape(text)}"
    pattern = re.compile(re.escape(cleaned_query), re.IGNORECASE)
    matches = list(pattern.finditer(text))
    if not matches:
        return f"Summary: {escape(text)}"

    chunks: list[str] = []
    cursor = 0
    for match in matches:
        start, end = match.span()
        if start > cursor:
            chunks.append(escape(text[cursor:start]))
        matched = escape(text[start:end])
        chunks.append(f"<span style='background-color: #fff176; color: #000;'>{matched}</span>")
        cursor = end
    if cursor < len(text):
        chunks.append(escape(text[cursor:]))
    return "Summary: " + "".join(chunks)
