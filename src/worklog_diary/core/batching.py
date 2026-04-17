from __future__ import annotations

from dataclasses import dataclass, field
import logging

from .activity_segmenter import ActivitySegment, ActivitySegmenter, build_activity_observations
from .activity_repository import ActivityRepository
from .models import ActiveInterval, BlockedInterval, ScreenshotRecord, TextSegment
from .screenshot_dedup import select_representative_screenshots

_LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class SummaryBatch:
    """Immutable snapshot of the pending activity that will be summarized."""

    start_ts: float
    end_ts: float
    activity_segments: list[ActivitySegment] = field(default_factory=list)
    active_intervals: list[ActiveInterval] = field(default_factory=list)
    blocked_intervals: list[BlockedInterval] = field(default_factory=list)
    text_segments: list[TextSegment] = field(default_factory=list)
    screenshots: list[ScreenshotRecord] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "activity_segments": [segment.to_dict() for segment in self.activity_segments],
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
                    "fingerprint": item.fingerprint,
                    "exact_hash": item.exact_hash,
                    "perceptual_hash": item.perceptual_hash,
                    "nearest_phash_distance": item.nearest_phash_distance,
                    "nearest_ssim": item.nearest_ssim,
                    "dedup_reason": item.dedup_reason,
                    "visual_context_streak": item.visual_context_streak,
                }
                for item in self.screenshots
            ],
        }


class BatchBuilder:
    """Build a summary batch from the repository without touching SQLite directly."""

    def __init__(
        self,
        storage: ActivityRepository,
        max_text_segments: int = 400,
        max_screenshots: int = 3,
        dedup_enabled: bool = True,
        dedup_threshold: int = 6,
        min_keep_interval_seconds: float = 120.0,
        activity_segment_min_duration_seconds: float = 180.0,
        activity_segment_max_duration_seconds: float = 900.0,
        activity_segment_idle_gap_seconds: float = 20.0,
        activity_segment_title_similarity_threshold: float = 0.72,
        activity_segment_screenshot_phash_threshold: int = 6,
        activity_segment_screenshot_ssim_threshold: float = 0.985,
    ) -> None:
        self.storage = storage
        self.max_text_segments = max_text_segments
        self.max_screenshots = max_screenshots
        self.dedup_enabled = bool(dedup_enabled)
        self.dedup_threshold = max(0, int(dedup_threshold))
        self.min_keep_interval_seconds = max(0.0, float(min_keep_interval_seconds))
        self.activity_segment_min_duration_seconds = max(0.0, float(activity_segment_min_duration_seconds))
        self.activity_segment_max_duration_seconds = max(
            self.activity_segment_min_duration_seconds,
            float(activity_segment_max_duration_seconds),
        )
        self.activity_segment_idle_gap_seconds = max(0.0, float(activity_segment_idle_gap_seconds))
        self.activity_segment_title_similarity_threshold = max(
            0.0,
            min(1.0, float(activity_segment_title_similarity_threshold)),
        )
        self.activity_segment_screenshot_phash_threshold = max(0, int(activity_segment_screenshot_phash_threshold))
        self.activity_segment_screenshot_ssim_threshold = max(
            0.0,
            min(1.0, float(activity_segment_screenshot_ssim_threshold)),
        )

    def build_pending_batch(
        self,
        excluded_ranges: list[tuple[float, float]] | None = None,
        *,
        force_flush: bool = False,
    ) -> SummaryBatch | None:
        intervals = self.storage.fetch_unsummarized_intervals()
        blocked_intervals = self.storage.fetch_unsummarized_blocked_intervals()

        text_limit = self._expanded_fetch_limit(self.max_text_segments, excluded_ranges)
        screenshot_limit = self._expanded_fetch_limit(
            self.max_screenshots,
            excluded_ranges,
            multiplier=self._screenshot_fetch_multiplier(),
        )

        text_segments = self.storage.fetch_unsummarized_text_segments(limit=text_limit)
        screenshots = self.storage.fetch_unsummarized_screenshots(limit=screenshot_limit)

        if excluded_ranges:
            intervals = [
                item
                for item in intervals
                if not _overlaps_any_range(item.start_ts, item.end_ts or item.start_ts, excluded_ranges)
            ]
            blocked_intervals = [
                item
                for item in blocked_intervals
                if not _overlaps_any_range(item.start_ts, item.end_ts or item.start_ts, excluded_ranges)
            ]
            text_segments = [
            item for item in text_segments if not _overlaps_any_range(item.start_ts, item.end_ts, excluded_ranges)
        ]
        screenshots = [item for item in screenshots if not _overlaps_any_range(item.ts, item.ts, excluded_ranges)]

        observations = build_activity_observations(
            intervals=intervals,
            blocked_intervals=blocked_intervals,
            text_segments=text_segments,
            screenshots=screenshots,
        )
        segments = ActivitySegmenter(
            idle_gap_seconds=self.activity_segment_idle_gap_seconds,
            max_duration_seconds=self.activity_segment_max_duration_seconds,
            title_similarity_threshold=self.activity_segment_title_similarity_threshold,
            screenshot_phash_threshold=self.activity_segment_screenshot_phash_threshold,
            screenshot_ssim_threshold=self.activity_segment_screenshot_ssim_threshold,
        ).segment(observations, force_flush=force_flush)

        ready_segment = next((segment for segment in segments if segment.is_closed), None)
        if ready_segment is None:
            if not force_flush or not segments:
                _LOGGER.info(
                    "event=activity_segment_pending reason=%s detail=%s segment_count=%s",
                    "force_flush_requested" if force_flush else "open_segment_not_mature",
                    "no_closed_segment_ready",
                    len(segments),
                )
                return None
            ready_segment = segments[-1]
        _LOGGER.info(
            (
                "event=activity_segment_selected segment_id=%s reason=%s start_ts=%.3f end_ts=%.3f "
                "duration_seconds=%.3f process=%s title=%s observations=%s screenshots=%s text_segments=%s"
            ),
            ready_segment.segment_id,
            ready_segment.closure_reason,
            ready_segment.start_ts,
            ready_segment.end_ts,
            ready_segment.duration_seconds,
            ready_segment.dominant_process_name,
            ready_segment.dominant_window_title,
            ready_segment.observation_count,
            ready_segment.screenshot_count,
            ready_segment.text_segment_count,
        )

        segment_end = ready_segment.end_ts
        intervals = [item for item in intervals if (item.end_ts or item.start_ts) <= segment_end]
        blocked_intervals = [item for item in blocked_intervals if (item.end_ts or item.start_ts) <= segment_end]
        text_segments = [item for item in text_segments if item.end_ts <= segment_end]
        screenshots = [item for item in screenshots if item.ts <= segment_end]

        screenshots = select_representative_screenshots(
            screenshots,
            max_screenshots=self.max_screenshots,
            dedup_enabled=self.dedup_enabled,
            dedup_threshold=self.dedup_threshold,
            min_keep_interval_seconds=self.min_keep_interval_seconds,
        )

        return build_batch_from_pending(
            intervals=intervals,
            blocked_intervals=blocked_intervals,
            text_segments=text_segments,
            screenshots=screenshots,
            activity_segments=[ready_segment],
        )

    @staticmethod
    def _expanded_fetch_limit(
        base_limit: int,
        excluded_ranges: list[tuple[float, float]] | None,
        *,
        multiplier: int = 1,
    ) -> int:
        if base_limit <= 0:
            return 0
        if not excluded_ranges:
            return base_limit * max(1, multiplier)
        return base_limit * (len(excluded_ranges) + 1) * max(1, multiplier)

    def _screenshot_fetch_multiplier(self) -> int:
        return 5 if self.dedup_enabled else 1



def build_batch_from_pending(
    intervals: list[ActiveInterval],
    blocked_intervals: list[BlockedInterval],
    text_segments: list[TextSegment],
    screenshots: list[ScreenshotRecord],
    activity_segments: list[ActivitySegment] | None = None,
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
        activity_segments=list(activity_segments or []),
        active_intervals=sorted(intervals, key=lambda item: item.start_ts),
        blocked_intervals=sorted(blocked_intervals, key=lambda item: item.start_ts),
        text_segments=sorted(text_segments, key=lambda item: item.start_ts),
        screenshots=sorted(screenshots, key=lambda item: item.ts),
    )


def _overlaps_any_range(start_ts: float, end_ts: float, excluded_ranges: list[tuple[float, float]]) -> bool:
    for range_start, range_end in excluded_ranges:
        if _ranges_overlap(start_ts, end_ts, range_start, range_end):
            return True
    return False


def _ranges_overlap(a_start: float, a_end: float, b_start: float, b_end: float) -> bool:
    return not (a_end < b_start or a_start > b_end)
