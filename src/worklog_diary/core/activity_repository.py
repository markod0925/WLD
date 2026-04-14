from __future__ import annotations

from datetime import date
from typing import Protocol

from .models import ActiveInterval, BlockedInterval, ScreenshotRecord, SummaryRecord, TextSegment


class ActivityRepository(Protocol):
    def fetch_unsummarized_intervals(self, limit: int = 10000) -> list[ActiveInterval]:
        ...

    def fetch_unsummarized_blocked_intervals(self, limit: int = 10000) -> list[BlockedInterval]:
        ...

    def fetch_unsummarized_text_segments(self, limit: int = 200) -> list[TextSegment]:
        ...

    def fetch_unsummarized_screenshots(self, limit: int = 20) -> list[ScreenshotRecord]:
        ...

    def list_summaries_for_day(self, day: date, limit: int = 500) -> list[SummaryRecord]:
        ...
