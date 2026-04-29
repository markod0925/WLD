from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from threading import Lock
from typing import Any


@dataclass(slots=True)
class ForegroundInfo:
    timestamp: float
    hwnd: int
    pid: int
    process_name: str
    window_title: str


@dataclass(slots=True)
class ActiveInterval:
    id: int | None
    start_ts: float
    end_ts: float | None
    hwnd: int
    pid: int
    process_name: str
    window_title: str
    blocked: bool
    summarized: bool = False


@dataclass(slots=True)
class BlockedInterval:
    id: int | None
    active_interval_id: int | None
    start_ts: float
    end_ts: float | None
    process_name: str
    window_title: str
    summarized: bool = False


@dataclass(slots=True)
class KeyEvent:
    id: int | None
    ts: float
    key: str
    event_type: str
    modifiers: list[str]
    process_name: str
    window_title: str
    hwnd: int
    active_interval_id: int | None
    processed: bool = False


@dataclass(slots=True)
class TextSegment:
    id: int | None
    start_ts: float
    end_ts: float
    process_name: str
    window_title: str
    text: str
    hotkeys: list[str]
    raw_key_count: int


@dataclass(slots=True)
class ScreenshotRecord:
    id: int | None
    ts: float
    file_path: str
    process_name: str
    window_title: str
    active_interval_id: int | None
    window_hwnd: int | None = None
    fingerprint: str | None = None
    exact_hash: str | None = None
    perceptual_hash: str | None = None
    image_width: int | None = None
    image_height: int | None = None
    nearest_phash_distance: int | None = None
    nearest_ssim: float | None = None
    dedup_reason: str | None = None
    visual_context_streak: int = 0


@dataclass(slots=True)
class SummaryJob:
    id: int | None
    start_ts: float
    end_ts: float
    status: str
    error: str | None
    created_ts: float
    updated_ts: float


@dataclass(slots=True)
class SummaryRecord:
    id: int | None
    job_id: int
    start_ts: float
    end_ts: float
    summary_text: str
    summary_json: dict[str, Any]
    created_ts: float




@dataclass(slots=True)
class CoalescingDiagnosticRecord:
    id: int | None
    day: date
    left_summary_id: int
    right_summary_id: int
    embedding_cosine_similarity: float
    app_similarity_score: float
    window_similarity_score: float
    keyword_overlap_score: float
    temporal_gap_seconds: float
    blockers_json: list[str]
    final_merge_score: float
    decision: str
    reasons_json: list[str]
    created_ts: float


@dataclass(slots=True)
class CoalescedSummaryRecord:
    id: int | None
    start_ts: float
    end_ts: float
    summary_text: str
    summary_json: dict[str, Any]
    created_ts: float
    source_summary_ids: list[int]


@dataclass(slots=True)
class DailySummaryRecord:
    id: int | None
    day: date
    recap_text: str
    recap_json: dict[str, Any] | None
    source_batch_count: int
    created_ts: float


@dataclass(slots=True)
class StateSnapshot:
    monitoring_active: bool
    blocked: bool
    foreground_info: ForegroundInfo | None
    active_interval_id: int | None
    last_flush_ts: float | None
    next_flush_ts: float | None


@dataclass(slots=True)
class SharedState:
    monitoring_active: bool = False
    blocked: bool = False
    foreground_info: ForegroundInfo | None = None
    active_interval_id: int | None = None
    last_flush_ts: float | None = None
    next_flush_ts: float | None = None
    _lock: Lock = field(default_factory=Lock, repr=False)

    def set_monitoring_active(self, value: bool) -> None:
        with self._lock:
            self.monitoring_active = value
            if not value:
                self.active_interval_id = None
                self.foreground_info = None
                self.blocked = False

    def update_foreground(
        self,
        info: ForegroundInfo | None,
        blocked: bool,
        active_interval_id: int | None,
    ) -> None:
        with self._lock:
            self.foreground_info = info
            self.blocked = blocked
            self.active_interval_id = active_interval_id

    def set_flush_times(self, last_flush_ts: float | None, next_flush_ts: float | None) -> None:
        with self._lock:
            self.last_flush_ts = last_flush_ts
            self.next_flush_ts = next_flush_ts

    def snapshot(self) -> StateSnapshot:
        with self._lock:
            return StateSnapshot(
                monitoring_active=self.monitoring_active,
                blocked=self.blocked,
                foreground_info=self.foreground_info,
                active_interval_id=self.active_interval_id,
                last_flush_ts=self.last_flush_ts,
                next_flush_ts=self.next_flush_ts,
            )
