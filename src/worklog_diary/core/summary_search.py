from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum


class SummarySearchScope(str, Enum):
    DAY = "day"
    MONTH = "month"
    YEAR = "year"
    ALL = "all"


class SummarySearchType(str, Enum):
    EVENT = "event"
    DAY = "day"


@dataclass(slots=True)
class SummarySearchParams:
    query: str
    scope: SummarySearchScope
    anchor_day: date


@dataclass(slots=True)
class SummarySearchResult:
    summary_type: SummarySearchType
    source_id: int
    timestamp: float
    day: date
    text: str


@dataclass(slots=True)
class _SearchBounds:
    event_start_ts: float | None
    event_end_ts: float | None
    day_start: date | None
    day_end_exclusive: date | None


class SummarySearchService:
    def __init__(self, storage: object) -> None:
        self.storage = storage

    def search(self, params: SummarySearchParams) -> list[SummarySearchResult]:
        query = params.query.strip()
        if not query:
            return []

        bounds = _resolve_bounds(scope=params.scope, anchor_day=params.anchor_day)
        events = self.storage.search_event_summaries(
            query=query,
            start_ts=bounds.event_start_ts,
            end_ts=bounds.event_end_ts,
            limit=2000,
        )
        daily = self.storage.search_daily_summaries(
            query=query,
            start_day=bounds.day_start,
            end_day_exclusive=bounds.day_end_exclusive,
            limit=2000,
        )

        results: list[SummarySearchResult] = []
        for item in events:
            results.append(
                SummarySearchResult(
                    summary_type=SummarySearchType.EVENT,
                    source_id=int(item.id or 0),
                    timestamp=float(item.start_ts),
                    day=datetime.fromtimestamp(float(item.start_ts)).date(),
                    text=item.summary_text.strip(),
                )
            )
        for item in daily:
            results.append(
                SummarySearchResult(
                    summary_type=SummarySearchType.DAY,
                    source_id=int(item.id or 0),
                    timestamp=float(item.created_ts),
                    day=item.day,
                    text=item.recap_text.strip(),
                )
            )

        return sorted(results, key=lambda item: item.timestamp, reverse=True)


def _resolve_bounds(scope: SummarySearchScope, anchor_day: date) -> _SearchBounds:
    if scope == SummarySearchScope.ALL:
        return _SearchBounds(
            event_start_ts=None,
            event_end_ts=None,
            day_start=None,
            day_end_exclusive=None,
        )

    if scope == SummarySearchScope.DAY:
        start = datetime.combine(anchor_day, time.min).astimezone()
        end = start + timedelta(days=1)
        return _SearchBounds(
            event_start_ts=start.timestamp(),
            event_end_ts=end.timestamp(),
            day_start=anchor_day,
            day_end_exclusive=anchor_day + timedelta(days=1),
        )

    if scope == SummarySearchScope.MONTH:
        month_start = date(anchor_day.year, anchor_day.month, 1)
        if anchor_day.month == 12:
            month_end = date(anchor_day.year + 1, 1, 1)
        else:
            month_end = date(anchor_day.year, anchor_day.month + 1, 1)
        start = datetime.combine(month_start, time.min).astimezone()
        end = datetime.combine(month_end, time.min).astimezone()
        return _SearchBounds(
            event_start_ts=start.timestamp(),
            event_end_ts=end.timestamp(),
            day_start=month_start,
            day_end_exclusive=month_end,
        )

    year_start = date(anchor_day.year, 1, 1)
    year_end = date(anchor_day.year + 1, 1, 1)
    start = datetime.combine(year_start, time.min).astimezone()
    end = datetime.combine(year_end, time.min).astimezone()
    return _SearchBounds(
        event_start_ts=start.timestamp(),
        event_end_ts=end.timestamp(),
        day_start=year_start,
        day_end_exclusive=year_end,
    )
