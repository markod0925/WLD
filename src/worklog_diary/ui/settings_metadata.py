from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping
from typing import Literal

from ..core.config import AppConfig, SUPPORTED_CAPTURE_MODES

ExposureLevel = Literal["user", "advanced", "debug", "internal"]
WidgetType = Literal["text", "multiline", "bool", "int", "float", "select", "readonly"]


@dataclass(frozen=True, slots=True)
class SettingUiMetadata:
    key: str
    label: str
    tooltip: str
    exposure: ExposureLevel
    widget: WidgetType
    min_value: float | int | None = None
    max_value: float | int | None = None
    step: float | int | None = None
    options: tuple[str, ...] = ()
    requires_restart: bool = False
    is_experimental: bool = False


DEFAULTS = AppConfig().to_dict()


EXPOSURE_BY_KEY: dict[str, ExposureLevel] = {
    "config_version": "internal",
    "blocked_processes": "user",
    "screenshot_interval_seconds": "user",
    "capture_mode": "user",
    "foreground_poll_interval_seconds": "advanced",
    "text_inactivity_gap_seconds": "advanced",
    "reconstruction_poll_interval_seconds": "advanced",
    "flush_interval_seconds": "user",
    "lmstudio_base_url": "user",
    "lmstudio_model": "user",
    "lmstudio_max_prompt_chars": "debug",
    "app_data_dir": "advanced",
    "screenshot_dir": "advanced",
    "log_dir": "advanced",
    "db_path": "advanced",
    "config_path": "internal",
    "start_monitoring_on_launch": "user",
    "max_screenshots_per_summary": "advanced",
    "max_text_segments_per_summary": "advanced",
    "max_parallel_summary_jobs": "advanced",
    "request_timeout_seconds": "advanced",
    "activity_segment_min_duration_seconds": "debug",
    "activity_segment_max_duration_seconds": "debug",
    "activity_segment_idle_gap_seconds": "debug",
    "activity_segment_title_similarity_threshold": "debug",
    "summary_similarity_suppress_threshold": "debug",
    "summary_similarity_merge_threshold": "debug",
    "summary_cooldown_seconds": "debug",
    "recent_summary_compare_count": "debug",
    "screenshot_dedup_exact_hash_enabled": "debug",
    "screenshot_dedup_perceptual_hash_enabled": "debug",
    "screenshot_dedup_phash_threshold": "debug",
    "screenshot_dedup_ssim_enabled": "debug",
    "screenshot_dedup_ssim_threshold": "debug",
    "screenshot_dedup_resize_width": "debug",
    "screenshot_dedup_compare_recent_count": "debug",
    "screenshot_dedup_enabled": "debug",
    "screenshot_dedup_threshold": "internal",
    "screenshot_min_keep_interval_seconds": "debug",
}


def _setting(**kwargs: object) -> SettingUiMetadata:
    exposure = kwargs.get("exposure")
    kwargs.setdefault("requires_restart", False)
    kwargs.setdefault("is_experimental", exposure == "debug")
    return SettingUiMetadata(**kwargs)


EXPOSED_SETTINGS: tuple[SettingUiMetadata, ...] = (
    _setting(
        key="blocked_processes",
        label="Blocked processes:",
        tooltip=(
            "Description: Process names that should never have text or screenshots recorded.\n"
            "Impact: Matching windows are treated as private and excluded from capture.\n"
            "Tip: One process per line; names are normalized to lowercase on save.\n"
            f"Default: {', '.join(DEFAULTS['blocked_processes'])}."
        ),
        exposure="user",
        widget="multiline",
    ),
    _setting(
        key="screenshot_interval_seconds",
        label="Screenshot interval (s):",
        tooltip=(
            "Description: How often screenshot capture runs while monitoring is active.\n"
            "Impact: Lower values provide denser history but increase storage and processing load.\n"
            "Range: Runtime enforces at least 5 seconds.\n"
            f"Default: {DEFAULTS['screenshot_interval_seconds']} seconds."
        ),
        exposure="user",
        widget="int",
        min_value=5,
        max_value=3600,
    ),
    _setting(
        key="capture_mode",
        label="Screenshot capture mode:",
        tooltip=(
            "Description: Choose active window capture or full-screen capture.\n"
            "Impact: active_window reduces unrelated content; full_screen preserves broader context.\n"
            "Options: active_window, full_screen.\n"
            f"Default: {DEFAULTS['capture_mode']}."
        ),
        exposure="user",
        widget="select",
        options=tuple(sorted(SUPPORTED_CAPTURE_MODES)),
    ),
    _setting(
        key="flush_interval_seconds",
        label="Summary flush interval (s):",
        tooltip=(
            "Description: How often pending activity is summarized automatically.\n"
            "Impact: Lower values generate summaries sooner with less context per run.\n"
            "Range: Runtime enforces at least 30 seconds.\n"
            f"Default: {DEFAULTS['flush_interval_seconds']} seconds."
        ),
        exposure="user",
        widget="int",
        min_value=30,
        max_value=7200,
    ),
    _setting(
        key="lmstudio_base_url",
        label="LM Studio base URL:",
        tooltip=(
            "Description: OpenAI-compatible base endpoint for LM Studio summary requests.\n"
            "Impact: Must point to your running LM Studio server, or summaries will fail.\n"
            "Tip: Include protocol and host, e.g. http://127.0.0.1:1234/v1.\n"
            f"Default: {DEFAULTS['lmstudio_base_url']}."
        ),
        exposure="user",
        widget="text",
    ),
    _setting(
        key="lmstudio_model",
        label="LM Studio model:",
        tooltip=(
            "Description: Model identifier sent with each summary request.\n"
            "Impact: Model choice changes summary quality, speed, and resource usage.\n"
            "Tip: Use the exact model id exposed by your LM Studio server.\n"
            f"Default: {DEFAULTS['lmstudio_model']}."
        ),
        exposure="user",
        widget="text",
    ),
    _setting(
        key="start_monitoring_on_launch",
        label="Start monitoring on launch:",
        tooltip=(
            "Description: Automatically starts monitoring when the tray app opens.\n"
            "Impact: If enabled, capture begins without manually pressing Start.\n"
            "Safety: Disable if you want explicit control before any capture starts.\n"
            f"Default: {DEFAULTS['start_monitoring_on_launch']}."
        ),
        exposure="user",
        widget="bool",
    ),
    _setting(
        key="foreground_poll_interval_seconds",
        label="Foreground poll interval (s):",
        tooltip=(
            "Description: Frequency of active window/title sampling.\n"
            "Impact: Shorter intervals detect context switches faster with more CPU overhead.\n"
            "Range: Runtime enforces at least 0.2 seconds.\n"
            f"Default: {DEFAULTS['foreground_poll_interval_seconds']} seconds."
        ),
        exposure="advanced",
        widget="float",
        min_value=0.2,
        max_value=5.0,
        step=0.1,
    ),
    _setting(
        key="text_inactivity_gap_seconds",
        label="Text inactivity gap (s):",
        tooltip=(
            "Description: Idle gap that closes one reconstructed typing segment.\n"
            "Impact: Lower values split typing into smaller segments; higher values merge pauses.\n"
            "Range: Tuned in seconds; use cautiously if summaries become too fragmented.\n"
            f"Default: {DEFAULTS['text_inactivity_gap_seconds']} seconds."
        ),
        exposure="advanced",
        widget="float",
        min_value=1.0,
        max_value=60.0,
        step=0.5,
    ),
    _setting(
        key="reconstruction_poll_interval_seconds",
        label="Text reconstruction poll (s):",
        tooltip=(
            "Description: How often buffered key events are converted to text segments.\n"
            "Impact: Lower values make text appear sooner but increase processing frequency.\n"
            "Range: Runtime enforces at least 0.5 seconds.\n"
            f"Default: {DEFAULTS['reconstruction_poll_interval_seconds']} seconds."
        ),
        exposure="advanced",
        widget="float",
        min_value=0.5,
        max_value=10.0,
        step=0.5,
    ),
    _setting(
        key="max_parallel_summary_jobs",
        label="Max parallel summary jobs:",
        tooltip=(
            "Description: Maximum concurrent summary requests per flush cycle.\n"
            "Impact: Higher values increase throughput but can overload local model serving.\n"
            "Range: Minimum is 1.\n"
            f"Default: {DEFAULTS['max_parallel_summary_jobs']}."
        ),
        exposure="advanced",
        widget="int",
        min_value=1,
        max_value=16,
    ),
    _setting(
        key="max_screenshots_per_summary",
        label="Max screenshots per summary:",
        tooltip=(
            "Description: Upper bound for screenshot count attached to each summary batch.\n"
            "Impact: Higher values preserve more visual context but increase prompt size.\n"
            "Range: 0 disables screenshot attachments in summaries.\n"
            f"Default: {DEFAULTS['max_screenshots_per_summary']}."
        ),
        exposure="advanced",
        widget="int",
        min_value=0,
        max_value=10,
    ),
    _setting(
        key="max_text_segments_per_summary",
        label="Max text segments per summary:",
        tooltip=(
            "Description: Maximum reconstructed text segments included in a single summary batch.\n"
            "Impact: Higher values improve context but increase latency and token pressure.\n"
            "Range: Keep high enough to avoid truncating busy sessions.\n"
            f"Default: {DEFAULTS['max_text_segments_per_summary']}."
        ),
        exposure="advanced",
        widget="int",
        min_value=10,
        max_value=2000,
    ),
    _setting(
        key="app_data_dir",
        label="Application data directory:",
        tooltip=(
            "Description: Root folder containing config, database, logs, and screenshots.\n"
            "Practical use: Useful for portable mode layouts and moving data to another drive.\n"
            "Troubleshooting: Lets you redirect storage when diagnosing disk or permission issues.\n"
            f"Default: {DEFAULTS['app_data_dir'] or '<auto-resolved>'}."
        ),
        exposure="advanced",
        widget="text",
        requires_restart=True,
    ),
    _setting(
        key="screenshot_dir",
        label="Screenshot directory:",
        tooltip=(
            "Description: Folder where captured screenshots are stored.\n"
            "Practical use: Move this path to manage disk growth or faster storage.\n"
            "Troubleshooting: Helps isolate screenshot retention issues by redirecting output.\n"
            f"Default: {DEFAULTS['screenshot_dir'] or '<app_data_dir>/screenshots'}."
        ),
        exposure="advanced",
        widget="text",
        requires_restart=True,
    ),
    _setting(
        key="log_dir",
        label="Log directory:",
        tooltip=(
            "Description: Folder used for application logs and diagnostics output.\n"
            "Practical use: Redirect logs for support bundles or centralized troubleshooting.\n"
            "Troubleshooting: Useful when local profile permissions block default logging paths.\n"
            f"Default: {DEFAULTS['log_dir'] or '<app_data_dir>/logs'}."
        ),
        exposure="advanced",
        widget="text",
        requires_restart=True,
    ),
    _setting(
        key="db_path",
        label="SQLite database path:",
        tooltip=(
            "Description: Full file path to the SQLite activity database.\n"
            "Practical use: Move to a larger disk or faster storage for long-term history.\n"
            "Troubleshooting: Useful for backups, recovery, and portability workflows.\n"
            f"Default: {DEFAULTS['db_path'] or '<app_data_dir>/worklog_diary.db'}."
        ),
        exposure="advanced",
        widget="text",
        requires_restart=True,
    ),
    _setting(
        key="request_timeout_seconds",
        label="LM request timeout (s):",
        tooltip=(
            "Description: Maximum wait per LM Studio summary request before timeout.\n"
            "Impact: Higher values tolerate long generations but delay recovery from failures.\n"
            "Range: Balance reliability with responsiveness.\n"
            f"Default: {DEFAULTS['request_timeout_seconds']} seconds."
        ),
        exposure="advanced",
        widget="int",
        min_value=5,
        max_value=3600,
    ),
    _setting(
        key="lmstudio_max_prompt_chars",
        label="LM max prompt chars:",
        tooltip=(
            "Description: Hard cap for prompt size before LM requests are sent.\n"
            "Impact: Lower values trim context; higher values can increase latency or model failures.\n"
            "Experimental: Primarily for live tuning while refining prompt construction.\n"
            f"Default: {DEFAULTS['lmstudio_max_prompt_chars']}."
        ),
        exposure="debug",
        widget="int",
        min_value=2000,
        max_value=200000,
    ),
    _setting(
        key="activity_segment_min_duration_seconds",
        label="Activity segment min duration (s):",
        tooltip="Description: Minimum activity span required before a segment is considered complete.\nImpact: Lower values increase segment count and can create noisy summaries.\nExperimental: Segmenting logic is still actively tuned.\nDefault: 180.0 seconds.",
        exposure="debug",
        widget="float",
        min_value=0.0,
        max_value=3600.0,
        step=5.0,
    ),
    _setting(
        key="activity_segment_max_duration_seconds",
        label="Activity segment max duration (s):",
        tooltip="Description: Maximum span allowed for a segment before forced rollover.\nImpact: Lower values create shorter summaries with less temporal context.\nExperimental: Useful for online tuning while segment heuristics stabilize.\nDefault: 900.0 seconds.",
        exposure="debug",
        widget="float",
        min_value=0.0,
        max_value=7200.0,
        step=5.0,
    ),
    _setting(
        key="activity_segment_idle_gap_seconds",
        label="Activity segment idle gap (s):",
        tooltip="Description: Idle time that can close a running activity segment.\nImpact: Lower values break segments aggressively when you pause.\nExperimental: Tune carefully to avoid over-fragmentation.\nDefault: 20.0 seconds.",
        exposure="debug",
        widget="float",
        min_value=0.0,
        max_value=600.0,
        step=1.0,
    ),
    _setting(
        key="activity_segment_title_similarity_threshold",
        label="Activity title similarity threshold:",
        tooltip="Description: Similarity threshold for deciding whether window-title changes stay in one segment.\nImpact: Lower values merge more context; higher values split more aggressively.\nExperimental: Affects semantic grouping quality.\nDefault: 0.72.",
        exposure="debug",
        widget="float",
        min_value=0.0,
        max_value=1.0,
        step=0.01,
    ),
    _setting(
        key="summary_similarity_suppress_threshold",
        label="Summary suppress similarity threshold:",
        tooltip="Description: Similarity threshold for suppressing near-duplicate summaries.\nImpact: Higher values keep more summaries; lower values suppress aggressively.\nExperimental: Helps tune recap density and repetition.\nDefault: 0.86.",
        exposure="debug",
        widget="float",
        min_value=0.0,
        max_value=1.0,
        step=0.01,
    ),
    _setting(
        key="summary_similarity_merge_threshold",
        label="Summary merge similarity threshold:",
        tooltip="Description: Similarity threshold for merging adjacent related summaries.\nImpact: Lower values merge more often; higher values preserve separate entries.\nExperimental: Must stay at or below suppress threshold after normalization.\nDefault: 0.74.",
        exposure="debug",
        widget="float",
        min_value=0.0,
        max_value=1.0,
        step=0.01,
    ),
    _setting(
        key="summary_cooldown_seconds",
        label="Summary cooldown (s):",
        tooltip="Description: Cooldown window used by summary dedup logic between similar outputs.\nImpact: Higher values reduce repeated summaries; lower values allow denser updates.\nExperimental: Tradeoff between freshness and noise.\nDefault: 240 seconds.",
        exposure="debug",
        widget="int",
        min_value=0,
        max_value=7200,
    ),
    _setting(
        key="recent_summary_compare_count",
        label="Recent summaries to compare:",
        tooltip="Description: Number of recent summaries compared for dedup decisions.\nImpact: Higher values improve duplicate detection but add processing overhead.\nExperimental: Useful when tuning long-session behavior.\nDefault: 5.",
        exposure="debug",
        widget="int",
        min_value=1,
        max_value=100,
    ),
    _setting(
        key="screenshot_dedup_enabled",
        label="Screenshot dedup enabled:",
        tooltip="Description: Master switch for screenshot deduplication during batching.\nImpact: Disabling may increase repeated screenshots and prompt size.\nExperimental: Keep enabled unless validating raw capture behavior.\nDefault: True.",
        exposure="debug",
        widget="bool",
    ),
    _setting(
        key="screenshot_dedup_exact_hash_enabled",
        label="Dedup exact hash enabled:",
        tooltip="Description: Enables exact image hash checks for duplicate screenshots.\nImpact: Fast duplicate detection for byte-identical images.\nExperimental: Usually safe to keep enabled.\nDefault: True.",
        exposure="debug",
        widget="bool",
    ),
    _setting(
        key="screenshot_dedup_perceptual_hash_enabled",
        label="Dedup perceptual hash enabled:",
        tooltip="Description: Enables perceptual hash comparisons for visually similar screenshots.\nImpact: Improves duplicate detection across minor pixel changes.\nExperimental: Can hide small but meaningful UI updates if too aggressive.\nDefault: True.",
        exposure="debug",
        widget="bool",
    ),
    _setting(
        key="screenshot_dedup_phash_threshold",
        label="Dedup pHash threshold:",
        tooltip="Description: Distance threshold for perceptual hash similarity.\nImpact: Higher values treat more screenshots as duplicates.\nExperimental: Affects both capture dedup and activity segmentation heuristics.\nDefault: 6.",
        exposure="debug",
        widget="int",
        min_value=0,
        max_value=64,
    ),
    _setting(
        key="screenshot_dedup_ssim_enabled",
        label="Dedup SSIM enabled:",
        tooltip="Description: Enables structural similarity checks in screenshot dedup.\nImpact: Improves semantic dedup quality at extra compute cost.\nExperimental: Toggle when diagnosing dedup behavior.\nDefault: True.",
        exposure="debug",
        widget="bool",
    ),
    _setting(
        key="screenshot_dedup_ssim_threshold",
        label="Dedup SSIM threshold:",
        tooltip="Description: Similarity threshold used by SSIM dedup checks.\nImpact: Higher values require screenshots to be almost identical to dedup.\nExperimental: Tune with care; small shifts can change retention a lot.\nDefault: 0.985.",
        exposure="debug",
        widget="float",
        min_value=0.0,
        max_value=1.0,
        step=0.001,
    ),
    _setting(
        key="screenshot_dedup_resize_width",
        label="Dedup resize width (px):",
        tooltip="Description: Width used when resizing images for dedup feature extraction.\nImpact: Lower values are faster but less precise; higher values cost more CPU.\nExperimental: Primarily for algorithm tuning.\nDefault: 32.",
        exposure="debug",
        widget="int",
        min_value=8,
        max_value=512,
    ),
    _setting(
        key="screenshot_dedup_compare_recent_count",
        label="Dedup compare recent count:",
        tooltip="Description: Number of recent screenshots considered during dedup comparisons.\nImpact: Higher values catch repeats across longer windows with more compute.\nExperimental: Tune for balance between accuracy and throughput.\nDefault: 8.",
        exposure="debug",
        widget="int",
        min_value=1,
        max_value=100,
    ),
    _setting(
        key="screenshot_min_keep_interval_seconds",
        label="Min keep interval same context (s):",
        tooltip="Description: Minimum time between kept screenshots in nearly identical visual context.\nImpact: Higher values reduce screenshot volume in repetitive scenes.\nExperimental: Useful for storage and prompt-size tuning.\nDefault: 120 seconds.",
        exposure="debug",
        widget="int",
        min_value=0,
        max_value=7200,
    ),
)


UI_SETTINGS_BY_KEY: dict[str, SettingUiMetadata] = {item.key: item for item in EXPOSED_SETTINGS}


USER_SETTINGS: tuple[SettingUiMetadata, ...] = tuple(item for item in EXPOSED_SETTINGS if item.exposure == "user")
ADVANCED_SETTINGS: tuple[SettingUiMetadata, ...] = tuple(item for item in EXPOSED_SETTINGS if item.exposure == "advanced")
DEBUG_SETTINGS: tuple[SettingUiMetadata, ...] = tuple(item for item in EXPOSED_SETTINGS if item.exposure == "debug")


def is_debug_value_modified_from_default(key: str, value: object) -> bool:
    default = DEFAULTS[key]
    if isinstance(default, float):
        return abs(float(value) - default) > 1e-9
    if isinstance(default, list):
        if not isinstance(value, list):
            return True
        normalized = [str(item).strip().lower() for item in value if str(item).strip()]
        return normalized != default
    return value != default


def modified_debug_keys(values: Mapping[str, object]) -> list[str]:
    changed: list[str] = []
    for setting in DEBUG_SETTINGS:
        if setting.key not in values:
            continue
        if is_debug_value_modified_from_default(setting.key, values[setting.key]):
            changed.append(setting.key)
    return changed


def float_step_decimals(step: float) -> int:
    text = f"{step:.9f}".rstrip("0")
    if "." not in text:
        return 0
    return len(text.split(".", 1)[1])
