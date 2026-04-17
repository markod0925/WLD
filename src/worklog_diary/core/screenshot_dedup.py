from __future__ import annotations

import hashlib
from collections import deque
from dataclasses import dataclass, field

from .models import ScreenshotRecord


@dataclass(slots=True)
class ScreenshotFingerprintAnalysis:
    exact_hash: str
    perceptual_hash: str
    thumbnail_values: tuple[int, ...]
    width: int
    height: int


@dataclass(slots=True)
class ScreenshotDedupDecision:
    keep: bool
    reason: str
    nearest_phash_distance: int | None
    nearest_ssim: float | None
    visual_context_streak: int


@dataclass(slots=True)
class _ScreenshotHistoryEntry:
    ts: float
    process_name: str
    window_title: str
    window_hwnd: int | None
    active_interval_id: int | None
    exact_hash: str | None
    perceptual_hash: str | None
    thumbnail_values: tuple[int, ...] | None


@dataclass(slots=True)
class ScreenshotDedupState:
    compare_recent_count: int = 8
    exact_hash_enabled: bool = True
    perceptual_hash_enabled: bool = True
    phash_threshold: int = 6
    ssim_enabled: bool = True
    ssim_threshold: float = 0.985
    min_interval_same_visual_context_seconds: float = 120.0
    _recent: deque[_ScreenshotHistoryEntry] = field(default_factory=deque, repr=False)
    _last_kept_visual_context_ts: float | None = None
    _visual_context_streak: int = 0

    def seed(self, screenshots: list[ScreenshotRecord]) -> None:
        for screenshot in screenshots:
            self._recent.appendleft(
                _ScreenshotHistoryEntry(
                    ts=screenshot.ts,
                    process_name=screenshot.process_name,
                    window_title=screenshot.window_title,
                    window_hwnd=screenshot.window_hwnd,
                    active_interval_id=screenshot.active_interval_id,
                    exact_hash=screenshot.exact_hash,
                    perceptual_hash=screenshot.perceptual_hash or screenshot.fingerprint,
                    thumbnail_values=None,
                )
            )
        self._trim_history()

    def consider(
        self,
        *,
        ts: float,
        process_name: str,
        window_title: str,
        window_hwnd: int | None,
        active_interval_id: int | None,
        analysis: ScreenshotFingerprintAnalysis,
    ) -> ScreenshotDedupDecision:
        recent = [item for item in self._recent if _foreground_context_matches(item, process_name, window_title, window_hwnd, active_interval_id)]
        if not recent:
            return ScreenshotDedupDecision(
                keep=True,
                reason="new_context",
                nearest_phash_distance=None,
                nearest_ssim=None,
                visual_context_streak=self._visual_context_streak + 1,
            )

        nearest_distance: int | None = None
        nearest_ssim: float | None = None
        nearest_entry: _ScreenshotHistoryEntry | None = None
        for entry in recent[: self.compare_recent_count]:
            distance = fingerprint_hamming_distance(analysis.perceptual_hash, entry.perceptual_hash)
            if distance is None:
                continue
            if nearest_distance is None or distance < nearest_distance:
                nearest_distance = distance
                nearest_entry = entry
                nearest_ssim = _thumbnail_ssim(analysis.thumbnail_values, entry.thumbnail_values) if entry.thumbnail_values is not None else None

        if nearest_entry is None:
            return ScreenshotDedupDecision(
                keep=True,
                reason="unmatched_context",
                nearest_phash_distance=None,
                nearest_ssim=None,
                visual_context_streak=self._visual_context_streak + 1,
            )

        time_since_last = max(0.0, ts - nearest_entry.ts)
        exact_match = (
            self.exact_hash_enabled
            and analysis.exact_hash
            and nearest_entry.exact_hash
            and analysis.exact_hash == nearest_entry.exact_hash
        )
        phash_match = (
            self.perceptual_hash_enabled
            and nearest_distance is not None
            and nearest_distance <= self.phash_threshold
        )
        ssim_match = (
            self.ssim_enabled
            and nearest_ssim is not None
            and nearest_ssim >= self.ssim_threshold
        )

        if exact_match:
            if time_since_last < self.min_interval_same_visual_context_seconds:
                return ScreenshotDedupDecision(
                    keep=False,
                    reason="exact_duplicate",
                    nearest_phash_distance=nearest_distance,
                    nearest_ssim=nearest_ssim,
                    visual_context_streak=self._visual_context_streak,
                )
            return ScreenshotDedupDecision(
                keep=True,
                reason="exact_duplicate_heartbeat",
                nearest_phash_distance=nearest_distance,
                nearest_ssim=nearest_ssim,
                visual_context_streak=self._visual_context_streak + 1,
            )

        if phash_match and ssim_match:
            if time_since_last < self.min_interval_same_visual_context_seconds:
                return ScreenshotDedupDecision(
                    keep=False,
                    reason="high_ssim_duplicate",
                    nearest_phash_distance=nearest_distance,
                    nearest_ssim=nearest_ssim,
                    visual_context_streak=self._visual_context_streak,
                )
            return ScreenshotDedupDecision(
                keep=True,
                reason="visual_context_heartbeat",
                nearest_phash_distance=nearest_distance,
                nearest_ssim=nearest_ssim,
                visual_context_streak=self._visual_context_streak + 1,
            )

        if phash_match and time_since_last < self.min_interval_same_visual_context_seconds:
            return ScreenshotDedupDecision(
                keep=False,
                reason="perceptual_duplicate",
                nearest_phash_distance=nearest_distance,
                nearest_ssim=nearest_ssim,
                visual_context_streak=self._visual_context_streak,
            )

        return ScreenshotDedupDecision(
            keep=True,
            reason="visual_context_changed",
            nearest_phash_distance=nearest_distance,
            nearest_ssim=nearest_ssim,
            visual_context_streak=self._visual_context_streak + 1,
        )

    def record_kept(
        self,
        *,
        ts: float,
        process_name: str,
        window_title: str,
        window_hwnd: int | None,
        active_interval_id: int | None,
        analysis: ScreenshotFingerprintAnalysis,
    ) -> None:
        if self._last_kept_visual_context_ts is None:
            self._visual_context_streak = 1
        else:
            if ts - self._last_kept_visual_context_ts < self.min_interval_same_visual_context_seconds:
                self._visual_context_streak += 1
            else:
                self._visual_context_streak = 1
        self._last_kept_visual_context_ts = ts

        self._recent.appendleft(
            _ScreenshotHistoryEntry(
                ts=ts,
                process_name=process_name,
                window_title=window_title,
                window_hwnd=window_hwnd,
                active_interval_id=active_interval_id,
                exact_hash=analysis.exact_hash,
                perceptual_hash=analysis.perceptual_hash,
                thumbnail_values=analysis.thumbnail_values,
            )
        )
        self._trim_history()

    def _trim_history(self) -> None:
        while len(self._recent) > max(1, self.compare_recent_count):
            self._recent.pop()


def analyze_screenshot(
    rgb_bytes: bytes,
    size: tuple[int, int],
    *,
    resize_width: int = 32,
    hash_size: int = 8,
) -> ScreenshotFingerprintAnalysis | None:
    width, height = size
    if width <= 0 or height <= 0 or resize_width <= 0 or hash_size <= 0:
        return None

    expected_length = width * height * 3
    if len(rgb_bytes) < expected_length:
        return None

    exact_hash = hashlib.sha256(rgb_bytes[:expected_length] + f"{width}x{height}".encode("ascii")).hexdigest()
    thumbnail_values = _sample_grayscale_grid(rgb_bytes, width, height, resize_width)
    hash_values = _sample_grayscale_grid(rgb_bytes, width, height, hash_size)
    perceptual_hash = _average_hash(hash_values)
    return ScreenshotFingerprintAnalysis(
        exact_hash=exact_hash,
        perceptual_hash=perceptual_hash,
        thumbnail_values=thumbnail_values,
        width=width,
        height=height,
    )


def compute_screenshot_fingerprint(rgb_bytes: bytes, size: tuple[int, int], hash_size: int = 8) -> str | None:
    analysis = analyze_screenshot(rgb_bytes, size, hash_size=hash_size)
    if analysis is None:
        return None
    return analysis.perceptual_hash


def fingerprint_hamming_distance(lhs: str | None, rhs: str | None) -> int | None:
    if not lhs or not rhs:
        return None
    try:
        return (int(lhs, 16) ^ int(rhs, 16)).bit_count()
    except ValueError:
        return None


def select_representative_screenshots(
    screenshots: list[ScreenshotRecord],
    *,
    max_screenshots: int,
    dedup_enabled: bool,
    dedup_threshold: int,
    min_keep_interval_seconds: float,
    recent_compare_count: int = 8,
) -> list[ScreenshotRecord]:
    if max_screenshots <= 0 or not screenshots:
        return []
    if not dedup_enabled:
        return screenshots[:max_screenshots]

    selected: list[ScreenshotRecord] = []
    recent: deque[ScreenshotRecord] = deque(maxlen=max(1, recent_compare_count))
    for screenshot in screenshots:
        if len(selected) >= max_screenshots:
            break
        if not selected:
            selected.append(screenshot)
            recent.appendleft(screenshot)
            continue
        if _should_keep_screenshot(
            screenshot,
            list(recent),
            dedup_threshold=dedup_threshold,
            min_keep_interval_seconds=min_keep_interval_seconds,
        ):
            selected.append(screenshot)
            recent.appendleft(screenshot)
    return selected


def _should_keep_screenshot(
    screenshot: ScreenshotRecord,
    recent_kept: list[ScreenshotRecord],
    *,
    dedup_threshold: int,
    min_keep_interval_seconds: float,
) -> bool:
    if not recent_kept:
        return True

    same_context_matches = [previous for previous in recent_kept if not _foreground_context_changed(screenshot, previous)]
    if not same_context_matches:
        return True

    for previous in same_context_matches:
        if min_keep_interval_seconds > 0 and (screenshot.ts - previous.ts) >= min_keep_interval_seconds:
            return True

        exact_hash = screenshot.exact_hash or screenshot.fingerprint
        previous_hash = previous.exact_hash or previous.fingerprint
        if exact_hash and previous_hash and exact_hash == previous_hash:
            return False

        distance = fingerprint_hamming_distance(
            screenshot.perceptual_hash or screenshot.fingerprint,
            previous.perceptual_hash or previous.fingerprint,
        )
        if distance is None:
            return True
        if distance <= dedup_threshold:
            return False

    return True


def _foreground_context_changed(current: ScreenshotRecord, previous: ScreenshotRecord) -> bool:
    if current.window_hwnd is not None and previous.window_hwnd is not None:
        if current.window_hwnd != previous.window_hwnd:
            return True
    elif current.active_interval_id is not None and previous.active_interval_id is not None:
        if current.active_interval_id != previous.active_interval_id:
            return True

    if current.window_title != previous.window_title:
        return True
    if current.process_name != previous.process_name:
        return True
    return False


def _sample_grayscale_grid(rgb_bytes: bytes, width: int, height: int, grid_size: int) -> tuple[int, ...]:
    values: list[int] = []
    for grid_y in range(grid_size):
        src_y = min(height - 1, int(((grid_y + 0.5) * height) / grid_size))
        row_offset = src_y * width * 3
        for grid_x in range(grid_size):
            src_x = min(width - 1, int(((grid_x + 0.5) * width) / grid_size))
            pixel_offset = row_offset + src_x * 3
            red = rgb_bytes[pixel_offset]
            green = rgb_bytes[pixel_offset + 1]
            blue = rgb_bytes[pixel_offset + 2]
            values.append((red * 299 + green * 587 + blue * 114) // 1000)
    return tuple(values)


def _average_hash(luma_values: tuple[int, ...]) -> str:
    average = sum(luma_values) / len(luma_values)
    fingerprint = 0
    for luma in luma_values:
        fingerprint = (fingerprint << 1) | int(luma >= average)
    hex_width = (len(luma_values) + 3) // 4
    return f"{fingerprint:0{hex_width}x}"


def _thumbnail_ssim(lhs: tuple[int, ...] | None, rhs: tuple[int, ...] | None) -> float | None:
    if lhs is None or rhs is None or len(lhs) != len(rhs) or not lhs:
        return None

    count = len(lhs)
    mean_lhs = sum(lhs) / count
    mean_rhs = sum(rhs) / count

    var_lhs = sum((value - mean_lhs) ** 2 for value in lhs) / count
    var_rhs = sum((value - mean_rhs) ** 2 for value in rhs) / count
    covariance = sum((lhs[idx] - mean_lhs) * (rhs[idx] - mean_rhs) for idx in range(count)) / count

    c1 = (0.01 * 255) ** 2
    c2 = (0.03 * 255) ** 2
    numerator = (2 * mean_lhs * mean_rhs + c1) * (2 * covariance + c2)
    denominator = (mean_lhs**2 + mean_rhs**2 + c1) * (var_lhs + var_rhs + c2)
    if denominator == 0:
        return 1.0 if lhs == rhs else 0.0
    similarity = numerator / denominator
    return max(0.0, min(1.0, similarity))


def _foreground_context_matches(
    entry: _ScreenshotHistoryEntry,
    process_name: str,
    window_title: str,
    window_hwnd: int | None,
    active_interval_id: int | None,
) -> bool:
    if window_hwnd is not None and entry.window_hwnd is not None and window_hwnd != entry.window_hwnd:
        return False
    if active_interval_id is not None and entry.active_interval_id is not None and active_interval_id != entry.active_interval_id:
        return False
    return entry.process_name == process_name and entry.window_title == window_title
