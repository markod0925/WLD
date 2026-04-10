from __future__ import annotations

from dataclasses import dataclass

from .models import ActiveInterval, BlockedInterval, ScreenshotRecord, TextSegment
from .storage import SQLiteStorage


@dataclass(slots=True)
class SummaryBatch:
    start_ts: float
    end_ts: float
    active_intervals: list[ActiveInterval]
    blocked_intervals: list[BlockedInterval]
    text_segments: list[TextSegment]
    screenshots: list[ScreenshotRecord]

    def to_dict(self) -> dict:
        return {
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "active_intervals": [
                {
                    "start_ts": item.start_ts,
                    "end_ts": item.end_ts,
                    "process_name": item.process_name,
                    "window_title": item.window_title,
                    "blocked": item.blocked,
                }
                for item in self.active_intervals
            ],
            "blocked_intervals": [
                {
                    "start_ts": item.start_ts,
                    "end_ts": item.end_ts,
                    "process_name": item.process_name,
                    "window_title": item.window_title,
                }
                for item in self.blocked_intervals
            ],
            "text_segments": [
                {
                    "start_ts": item.start_ts,
                    "end_ts": item.end_ts,
                    "process_name": item.process_name,
                    "window_title": item.window_title,
                    "text": item.text,
                    "hotkeys": item.hotkeys,
                    "raw_key_count": item.raw_key_count,
                }
                for item in self.text_segments
            ],
            "screenshots": [
                {
                    "ts": item.ts,
                    "file_path": item.file_path,
                    "process_name": item.process_name,
                    "window_title": item.window_title,
                }
                for item in self.screenshots
            ],
        }


class BatchBuilder:
    def __init__(
        self,
        storage: SQLiteStorage,
        max_text_segments: int = 200,
        max_screenshots: int = 3,
    ) -> None:
        self.storage = storage
        self.max_text_segments = max_text_segments
        self.max_screenshots = max_screenshots

    def build_pending_batch(self) -> SummaryBatch | None:
        intervals = self.storage.fetch_unsummarized_intervals()
        blocked_intervals = self.storage.fetch_unsummarized_blocked_intervals()
        text_segments = self.storage.fetch_unsummarized_text_segments(limit=self.max_text_segments)
        screenshots = self.storage.fetch_unsummarized_screenshots(limit=self.max_screenshots)

        safe_end = _compute_safe_end_boundary(
            text_segments=text_segments,
            screenshots=screenshots,
            max_text_segments=self.max_text_segments,
            max_screenshots=self.max_screenshots,
        )
        if safe_end is not None:
            intervals = [item for item in intervals if (item.end_ts or item.start_ts) <= safe_end]
            blocked_intervals = [item for item in blocked_intervals if (item.end_ts or item.start_ts) <= safe_end]
            text_segments = [item for item in text_segments if item.end_ts <= safe_end]
            screenshots = [item for item in screenshots if item.ts <= safe_end]

        return build_batch_from_pending(
            intervals=intervals,
            blocked_intervals=blocked_intervals,
            text_segments=text_segments,
            screenshots=screenshots,
        )



def build_batch_from_pending(
    intervals: list[ActiveInterval],
    blocked_intervals: list[BlockedInterval],
    text_segments: list[TextSegment],
    screenshots: list[ScreenshotRecord],
) -> SummaryBatch | None:
    if not intervals and not blocked_intervals and not text_segments and not screenshots:
        return None

    all_starts: list[float] = []
    all_ends: list[float] = []

    for interval in intervals:
        all_starts.append(interval.start_ts)
        all_ends.append(interval.end_ts or interval.start_ts)

    for interval in blocked_intervals:
        all_starts.append(interval.start_ts)
        all_ends.append(interval.end_ts or interval.start_ts)

    for segment in text_segments:
        all_starts.append(segment.start_ts)
        all_ends.append(segment.end_ts)

    for screenshot in screenshots:
        all_starts.append(screenshot.ts)
        all_ends.append(screenshot.ts)

    start_ts = min(all_starts)
    end_ts = max(all_ends)

    return SummaryBatch(
        start_ts=start_ts,
        end_ts=end_ts,
        active_intervals=sorted(intervals, key=lambda item: item.start_ts),
        blocked_intervals=sorted(blocked_intervals, key=lambda item: item.start_ts),
        text_segments=sorted(text_segments, key=lambda item: item.start_ts),
        screenshots=sorted(screenshots, key=lambda item: item.ts),
    )


def _compute_safe_end_boundary(
    text_segments: list[TextSegment],
    screenshots: list[ScreenshotRecord],
    max_text_segments: int,
    max_screenshots: int,
) -> float | None:
    boundaries: list[float] = []
    if max_text_segments > 0 and len(text_segments) >= max_text_segments and text_segments:
        boundaries.append(max(item.end_ts for item in text_segments))
    if max_screenshots > 0 and len(screenshots) >= max_screenshots and screenshots:
        boundaries.append(max(item.ts for item in screenshots))
    if not boundaries:
        return None
    return min(boundaries)
