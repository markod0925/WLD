from __future__ import annotations

import hashlib
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from difflib import SequenceMatcher

from .models import ActiveInterval, BlockedInterval, ScreenshotRecord, TextSegment
from .screenshot_dedup import fingerprint_hamming_distance

_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]+")


@dataclass(slots=True)
class ActivityObservation:
    start_ts: float
    end_ts: float
    kind: str
    process_name: str
    window_title: str
    blocked: bool
    hwnd: int | None = None
    active_interval_id: int | None = None
    text: str | None = None
    hotkeys: list[str] = field(default_factory=list)
    screenshot_id: int | None = None
    screenshot_fingerprint: str | None = None
    screenshot_exact_hash: str | None = None
    screenshot_nearest_distance: int | None = None
    screenshot_nearest_ssim: float | None = None
    screenshot_dedup_reason: str | None = None


@dataclass(slots=True)
class ActivitySegment:
    segment_id: str
    start_ts: float
    end_ts: float
    process_names: list[str]
    window_titles: list[str]
    blocked: bool
    active_interval_ids: list[int]
    screenshot_ids: list[int]
    text_snippets: list[str]
    hotkeys: list[str]
    observation_count: int
    screenshot_count: int
    text_segment_count: int
    duration_seconds: float
    idle_gap_seconds: float | None
    closure_reason: str
    is_closed: bool
    dominant_process_name: str
    dominant_window_title: str
    visual_similarity: float | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "segment_id": self.segment_id,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "process_names": self.process_names,
            "window_titles": self.window_titles,
            "blocked": self.blocked,
            "active_interval_ids": self.active_interval_ids,
            "screenshot_ids": self.screenshot_ids,
            "text_snippets": self.text_snippets,
            "hotkeys": self.hotkeys,
            "observation_count": self.observation_count,
            "screenshot_count": self.screenshot_count,
            "text_segment_count": self.text_segment_count,
            "duration_seconds": self.duration_seconds,
            "idle_gap_seconds": self.idle_gap_seconds,
            "closure_reason": self.closure_reason,
            "is_closed": self.is_closed,
            "dominant_process_name": self.dominant_process_name,
            "dominant_window_title": self.dominant_window_title,
            "visual_similarity": self.visual_similarity,
        }


@dataclass(slots=True)
class SegmentTransition:
    should_close: bool
    closure_reason: str
    visual_similarity: float | None = None


class ActivitySegmenter:
    def __init__(
        self,
        *,
        idle_gap_seconds: float = 20.0,
        max_duration_seconds: float = 900.0,
        title_similarity_threshold: float = 0.72,
        screenshot_phash_threshold: int = 6,
        screenshot_ssim_threshold: float = 0.985,
    ) -> None:
        self.idle_gap_seconds = max(0.0, float(idle_gap_seconds))
        self.max_duration_seconds = max(0.0, float(max_duration_seconds))
        self.title_similarity_threshold = max(0.0, min(1.0, float(title_similarity_threshold)))
        self.screenshot_phash_threshold = max(0, int(screenshot_phash_threshold))
        self.screenshot_ssim_threshold = max(0.0, min(1.0, float(screenshot_ssim_threshold)))

    def segment(
        self,
        observations: list[ActivityObservation],
        *,
        force_flush: bool = False,
    ) -> list[ActivitySegment]:
        if not observations:
            return []

        ordered = sorted(observations, key=lambda item: (item.start_ts, item.end_ts, item.kind))
        completed: list[ActivitySegment] = []
        current = _SegmentBuffer.from_observation(ordered[0])

        for observation in ordered[1:]:
            transition = self._classify_transition(current, observation)
            if transition.should_close:
                completed.append(current.finalize(transition.closure_reason, transition.visual_similarity, is_closed=True))
                current = _SegmentBuffer.from_observation(observation)
                continue
            current.add_observation(observation, visual_similarity=transition.visual_similarity)

        if force_flush:
            completed.append(current.finalize("manual_flush", current.visual_similarity, is_closed=True))
        else:
            completed.append(current.finalize("open", current.visual_similarity, is_closed=False))

        return completed

    def _classify_transition(self, current: _SegmentBuffer, observation: ActivityObservation) -> SegmentTransition:
        gap = max(0.0, observation.start_ts - current.end_ts)
        if gap >= self.idle_gap_seconds and current.observation_count > 0:
            return SegmentTransition(True, "idle_gap", None)

        current_duration = max(0.0, current.end_ts - current.start_ts)
        if self.max_duration_seconds > 0 and current_duration >= self.max_duration_seconds:
            return SegmentTransition(True, "max_duration_reached", None)

        if current.blocked != observation.blocked:
            return SegmentTransition(True, "lock_state_changed", None)

        process_changed = observation.process_name != current.dominant_process_name
        title_similarity = _title_similarity(observation.window_title, current.dominant_window_title)

        if process_changed and title_similarity < self.title_similarity_threshold:
            return SegmentTransition(True, "app_changed", None)

        screenshot_similarity = self._screenshot_similarity(current, observation)
        if screenshot_similarity is not None:
            if screenshot_similarity < self.screenshot_ssim_threshold and title_similarity < 0.9:
                return SegmentTransition(True, "visual_context_changed", screenshot_similarity)

        if title_similarity < 0.45 and process_changed:
            return SegmentTransition(True, "window_changed_significantly", screenshot_similarity)

        return SegmentTransition(False, "same_context", screenshot_similarity)

    def _screenshot_similarity(
        self,
        current: _SegmentBuffer,
        observation: ActivityObservation,
    ) -> float | None:
        if current.last_screenshot_fingerprint is None or observation.screenshot_fingerprint is None:
            return None

        distance = fingerprint_hamming_distance(
            current.last_screenshot_fingerprint,
            observation.screenshot_fingerprint,
        )
        if distance is None:
            return None
        if distance <= self.screenshot_phash_threshold:
            return 1.0 - (distance / max(1, len(current.last_screenshot_fingerprint) * 4))
        return max(0.0, 1.0 - (distance / max(1, len(current.last_screenshot_fingerprint) * 4)))


@dataclass(slots=True)
class _SegmentBuffer:
    start_ts: float
    end_ts: float
    blocked: bool
    process_names: list[str] = field(default_factory=list)
    window_titles: list[str] = field(default_factory=list)
    active_interval_ids: list[int] = field(default_factory=list)
    screenshot_ids: list[int] = field(default_factory=list)
    text_snippets: list[str] = field(default_factory=list)
    hotkeys: list[str] = field(default_factory=list)
    observation_count: int = 0
    screenshot_count: int = 0
    text_segment_count: int = 0
    last_screenshot_fingerprint: str | None = None
    last_visual_similarity: float | None = None
    max_idle_gap_seconds: float = 0.0

    @classmethod
    def from_observation(cls, observation: ActivityObservation) -> _SegmentBuffer:
        buffer = cls(
            start_ts=observation.start_ts,
            end_ts=observation.end_ts,
            blocked=observation.blocked,
        )
        buffer.add_observation(observation, visual_similarity=None)
        return buffer

    def add_observation(self, observation: ActivityObservation, *, visual_similarity: float | None) -> None:
        gap = max(0.0, observation.start_ts - self.end_ts)
        self.max_idle_gap_seconds = max(self.max_idle_gap_seconds, gap)
        self.end_ts = max(self.end_ts, observation.end_ts)
        self.observation_count += 1
        self.last_visual_similarity = visual_similarity if visual_similarity is not None else self.last_visual_similarity

        if observation.process_name:
            self.process_names.append(observation.process_name)
        if observation.window_title:
            self.window_titles.append(observation.window_title)
        if observation.active_interval_id is not None:
            self.active_interval_ids.append(observation.active_interval_id)
        if observation.screenshot_id is not None:
            self.screenshot_ids.append(observation.screenshot_id)
            self.screenshot_count += 1
        if observation.text:
            self.text_snippets.append(observation.text)
            self.text_segment_count += 1
        if observation.hotkeys:
            self.hotkeys.extend(observation.hotkeys)
        if observation.screenshot_fingerprint:
            self.last_screenshot_fingerprint = observation.screenshot_fingerprint

    @property
    def dominant_process_name(self) -> str:
        return _most_common(self.process_names)

    @property
    def dominant_window_title(self) -> str:
        return _most_common(self.window_titles)

    def finalize(self, closure_reason: str, visual_similarity: float | None, *, is_closed: bool) -> ActivitySegment:
        process_names = _unique_preserve_order(self.process_names)
        window_titles = _unique_preserve_order(self.window_titles)
        segment_id = _segment_id(self.start_ts, self.end_ts, process_names, window_titles)
        return ActivitySegment(
            segment_id=segment_id,
            start_ts=self.start_ts,
            end_ts=self.end_ts,
            process_names=process_names,
            window_titles=window_titles,
            blocked=self.blocked,
            active_interval_ids=_unique_ints(self.active_interval_ids),
            screenshot_ids=_unique_ints(self.screenshot_ids),
            text_snippets=[snippet for snippet in self.text_snippets if snippet],
            hotkeys=_unique_preserve_order(self.hotkeys),
            observation_count=self.observation_count,
            screenshot_count=self.screenshot_count,
            text_segment_count=self.text_segment_count,
            duration_seconds=max(0.0, self.end_ts - self.start_ts),
            idle_gap_seconds=self.max_idle_gap_seconds if self.max_idle_gap_seconds > 0 else None,
            closure_reason=closure_reason,
            is_closed=is_closed,
            dominant_process_name=self.dominant_process_name,
            dominant_window_title=self.dominant_window_title,
            visual_similarity=visual_similarity if visual_similarity is not None else self.last_visual_similarity,
        )


def build_activity_observations(
    *,
    intervals: list[ActiveInterval],
    blocked_intervals: list[BlockedInterval],
    text_segments: list[TextSegment],
    screenshots: list[ScreenshotRecord],
) -> list[ActivityObservation]:
    observations: list[ActivityObservation] = []

    for interval in intervals:
        observations.append(
            ActivityObservation(
                start_ts=interval.start_ts,
                end_ts=interval.end_ts or interval.start_ts,
                kind="active_interval",
                process_name=interval.process_name,
                window_title=interval.window_title,
                blocked=interval.blocked,
                hwnd=interval.hwnd,
                active_interval_id=interval.id,
            )
        )

    for interval in blocked_intervals:
        observations.append(
            ActivityObservation(
                start_ts=interval.start_ts,
                end_ts=interval.end_ts or interval.start_ts,
                kind="blocked_interval",
                process_name=interval.process_name,
                window_title=interval.window_title,
                blocked=True,
                active_interval_id=interval.active_interval_id,
            )
        )

    for segment in text_segments:
        observations.append(
            ActivityObservation(
                start_ts=segment.start_ts,
                end_ts=segment.end_ts,
                kind="text_segment",
                process_name=segment.process_name,
                window_title=segment.window_title,
                blocked=False,
                text=segment.text,
                hotkeys=list(segment.hotkeys),
            )
        )

    for screenshot in screenshots:
        observations.append(
            ActivityObservation(
                start_ts=screenshot.ts,
                end_ts=screenshot.ts,
                kind="screenshot",
                process_name=screenshot.process_name,
                window_title=screenshot.window_title,
                blocked=False,
                hwnd=screenshot.window_hwnd,
                active_interval_id=screenshot.active_interval_id,
                screenshot_id=screenshot.id,
                screenshot_fingerprint=screenshot.perceptual_hash or screenshot.fingerprint,
                screenshot_exact_hash=screenshot.exact_hash,
                screenshot_nearest_distance=screenshot.nearest_phash_distance,
                screenshot_nearest_ssim=screenshot.nearest_ssim,
                screenshot_dedup_reason=screenshot.dedup_reason,
            )
        )

    observations.sort(key=lambda item: (item.start_ts, item.end_ts, item.kind))
    return observations


def _title_similarity(lhs: str, rhs: str) -> float:
    lhs_norm = _normalize_title(lhs)
    rhs_norm = _normalize_title(rhs)
    if not lhs_norm and not rhs_norm:
        return 1.0
    if not lhs_norm or not rhs_norm:
        return 0.0
    return SequenceMatcher(a=lhs_norm, b=rhs_norm).ratio()


def _normalize_title(value: str) -> str:
    stripped = value.strip().lower()
    stripped = _PUNCT_RE.sub(" ", stripped)
    return _WHITESPACE_RE.sub(" ", stripped).strip()


def _most_common(values: list[str]) -> str:
    if not values:
        return ""
    counts = Counter(values)
    return counts.most_common(1)[0][0]


def _unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _unique_ints(values: list[int]) -> list[int]:
    seen: set[int] = set()
    ordered: list[int] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _segment_id(start_ts: float, end_ts: float, process_names: list[str], window_titles: list[str]) -> str:
    payload = "|".join(
        [
            f"{start_ts:.3f}",
            f"{end_ts:.3f}",
            ",".join(process_names),
            ",".join(window_titles),
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]
