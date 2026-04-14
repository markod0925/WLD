from __future__ import annotations

from .models import ScreenshotRecord


def compute_screenshot_fingerprint(rgb_bytes: bytes, size: tuple[int, int], hash_size: int = 8) -> str | None:
    width, height = size
    if hash_size <= 0 or width <= 0 or height <= 0:
        return None

    expected_length = width * height * 3
    if len(rgb_bytes) < expected_length:
        return None

    luma_values: list[int] = []
    for thumb_y in range(hash_size):
        src_y = min(height - 1, (thumb_y * height) // hash_size)
        row_offset = src_y * width * 3
        for thumb_x in range(hash_size):
            src_x = min(width - 1, (thumb_x * width) // hash_size)
            pixel_offset = row_offset + src_x * 3
            red = rgb_bytes[pixel_offset]
            green = rgb_bytes[pixel_offset + 1]
            blue = rgb_bytes[pixel_offset + 2]
            luma_values.append((red * 299 + green * 587 + blue * 114) // 1000)

    average = sum(luma_values) / len(luma_values)
    fingerprint = 0
    for luma in luma_values:
        fingerprint = (fingerprint << 1) | int(luma >= average)

    hex_width = (hash_size * hash_size + 3) // 4
    return f"{fingerprint:0{hex_width}x}"


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
) -> list[ScreenshotRecord]:
    if max_screenshots <= 0 or not screenshots:
        return []
    if not dedup_enabled:
        return screenshots[:max_screenshots]

    selected: list[ScreenshotRecord] = []
    for screenshot in screenshots:
        if len(selected) >= max_screenshots:
            break
        if not selected:
            selected.append(screenshot)
            continue
        if _should_keep_screenshot(
            screenshot,
            selected[-1],
            dedup_threshold=dedup_threshold,
            min_keep_interval_seconds=min_keep_interval_seconds,
        ):
            selected.append(screenshot)
    return selected


def _should_keep_screenshot(
    screenshot: ScreenshotRecord,
    last_kept: ScreenshotRecord,
    *,
    dedup_threshold: int,
    min_keep_interval_seconds: float,
) -> bool:
    if _foreground_context_changed(screenshot, last_kept):
        return True

    if min_keep_interval_seconds > 0 and (screenshot.ts - last_kept.ts) >= min_keep_interval_seconds:
        return True

    distance = fingerprint_hamming_distance(screenshot.fingerprint, last_kept.fingerprint)
    if distance is None:
        return True
    return distance > dedup_threshold


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
