from __future__ import annotations

from dataclasses import dataclass

from ..core.config import AppConfig


@dataclass(frozen=True, slots=True)
class SettingUiMetadata:
    key: str
    label: str
    tooltip: str


DEFAULTS = AppConfig().to_dict()


EDITABLE_SETTINGS: tuple[SettingUiMetadata, ...] = (
    SettingUiMetadata(
        key="blocked_processes",
        label="Blocked processes:",
        tooltip=(
            "Description: Lists process names that should never have text or screenshots recorded.\n"
            "Impact: Matching windows are treated as private; more entries improve privacy but can hide useful activity.\n"
            "Impact: Names are normalized to lowercase and blank lines are ignored when saved.\n"
            f"Example: Keep '{', '.join(DEFAULTS['blocked_processes'])}' to exclude browsers and Webex by default."
        ),
    ),
    SettingUiMetadata(
        key="screenshot_interval_seconds",
        label="Screenshot interval (s):",
        tooltip=(
            "Description: Sets how often screenshot capture runs while monitoring is active.\n"
            "Impact: Lower values create denser visual history but increase storage and processing load.\n"
            "Impact: Runtime enforces a minimum of 5 seconds even if a lower value is provided elsewhere.\n"
            f"Example: {DEFAULTS['screenshot_interval_seconds']} captures one screenshot per minute."
        ),
    ),
    SettingUiMetadata(
        key="capture_mode",
        label="Screenshot capture mode:",
        tooltip=(
            "Description: Chooses whether captures use the active window only or the full monitor image.\n"
            "Impact: active_window reduces unrelated content; full_screen preserves broader context around the task.\n"
            "Impact: Unknown values are normalized back to active_window during config normalization.\n"
            f"Example: '{DEFAULTS['capture_mode']}' records just the focused window region."
        ),
    ),
    SettingUiMetadata(
        key="foreground_poll_interval_seconds",
        label="Foreground poll interval (s):",
        tooltip=(
            "Description: Controls how frequently the foreground-window tracker samples app/title changes.\n"
            "Impact: Short intervals detect context switches faster but consume more background CPU.\n"
            "Impact: Runtime clamps this to at least 0.2 seconds for stability.\n"
            f"Example: {DEFAULTS['foreground_poll_interval_seconds']} checks the active window once per second."
        ),
    ),
    SettingUiMetadata(
        key="text_inactivity_gap_seconds",
        label="Text inactivity gap (s):",
        tooltip=(
            "Description: Defines idle time that ends one reconstructed typing segment and starts a new one.\n"
            "Impact: Lower values split text into shorter segments; higher values merge pauses into one segment.\n"
            "Impact: This changes summary granularity but does not disable key capture.\n"
            f"Example: {DEFAULTS['text_inactivity_gap_seconds']} means an 8-second pause closes the current segment."
        ),
    ),
    SettingUiMetadata(
        key="reconstruction_poll_interval_seconds",
        label="Text reconstruction poll (s):",
        tooltip=(
            "Description: Sets how often buffered key events are processed into text segments.\n"
            "Impact: Lower values make new text appear in storage sooner but increase processing frequency.\n"
            "Impact: Runtime clamps this to at least 0.5 seconds when applying config.\n"
            f"Example: {DEFAULTS['reconstruction_poll_interval_seconds']} processes keyboard buffers every 2 seconds."
        ),
    ),
    SettingUiMetadata(
        key="flush_interval_seconds",
        label="Flush interval (s):",
        tooltip=(
            "Description: Determines how often pending activity is converted into summaries automatically.\n"
            "Impact: Short intervals produce more frequent summaries with less context per run.\n"
            "Impact: Scheduler and coordinator enforce a minimum effective interval of 30 seconds.\n"
            f"Example: {DEFAULTS['flush_interval_seconds']} runs summary flush roughly every 5 minutes."
        ),
    ),
    SettingUiMetadata(
        key="max_parallel_summary_jobs",
        label="Max parallel summary jobs:",
        tooltip=(
            "Description: Limits concurrent LM Studio summary requests during a flush cycle.\n"
            "Impact: Higher values can improve throughput but increase CPU/network pressure on the model server.\n"
            "Impact: Value is normalized to at least 1 before use.\n"
            f"Example: {DEFAULTS['max_parallel_summary_jobs']} allows up to two summaries to run in parallel."
        ),
    ),
    SettingUiMetadata(
        key="max_screenshots_per_summary",
        label="Max screenshots per summary:",
        tooltip=(
            "Description: Caps how many representative screenshots are attached to each summary batch.\n"
            "Impact: Higher values add visual evidence but increase prompt size and token usage.\n"
            "Impact: 0 disables screenshot attachments for summaries while capture can still continue.\n"
            f"Example: {DEFAULTS['max_screenshots_per_summary']} keeps up to three screenshots per summary."
        ),
    ),
    SettingUiMetadata(
        key="max_text_segments_per_summary",
        label="Max text segments per summary:",
        tooltip=(
            "Description: Sets the maximum number of reconstructed text segments fetched into one summary batch.\n"
            "Impact: Higher limits preserve more detail but increase prompt length and summarization latency.\n"
            "Impact: Very low limits can omit useful context from busy periods.\n"
            f"Example: {DEFAULTS['max_text_segments_per_summary']} includes up to 400 text segments in a batch."
        ),
    ),
    SettingUiMetadata(
        key="lmstudio_base_url",
        label="LM Studio base URL:",
        tooltip=(
            "Description: Base endpoint used for local OpenAI-compatible LM Studio API requests.\n"
            "Impact: Must point to the running LM Studio server or summary generation will fail.\n"
            "Impact: Trailing slashes are trimmed when config is applied.\n"
            f"Example: '{DEFAULTS['lmstudio_base_url']}' targets a local LM Studio server on port 1234."
        ),
    ),
    SettingUiMetadata(
        key="lmstudio_model",
        label="LM Studio model:",
        tooltip=(
            "Description: Names the model identifier sent with each summary request.\n"
            "Impact: Switching models changes summary quality, speed, and hardware requirements.\n"
            "Impact: The value is forwarded directly to LM Studio without app-side validation.\n"
            f"Example: '{DEFAULTS['lmstudio_model']}' uses the default local model name from config."
        ),
    ),
    SettingUiMetadata(
        key="request_timeout_seconds",
        label="LM request timeout (s):",
        tooltip=(
            "Description: Maximum wait time for each LM Studio summary request before timing out.\n"
            "Impact: Higher values tolerate slower generations but keep workers occupied longer on failures.\n"
            "Impact: Lower values fail fast but can interrupt legitimate long-running summaries.\n"
            f"Example: {DEFAULTS['request_timeout_seconds']} allows up to 10 minutes per request."
        ),
    ),
)


READONLY_SETTINGS: tuple[SettingUiMetadata, ...] = (
    SettingUiMetadata(
        key="app_data_dir",
        label="Data directory:",
        tooltip=(
            "Description: Root folder where WorkLog Diary stores config, logs, screenshots, and the database by default.\n"
            "Impact: Determines where runtime artifacts are created and where disk growth occurs.\n"
            "Impact: If unset, it is auto-derived from env override, portable mode, or local app data.\n"
            "Example: In portable mode this resolves next to the executable under a 'data' folder."
        ),
    ),
    SettingUiMetadata(
        key="db_path",
        label="SQLite database:",
        tooltip=(
            "Description: Full file path to the SQLite database that stores captured activity and summaries.\n"
            "Impact: Changing it moves where new records are written and where history is read from.\n"
            "Impact: Parent folders are created automatically during config normalization.\n"
            "Example: Default path is <app_data_dir>/worklog_diary.db."
        ),
    ),
    SettingUiMetadata(
        key="log_dir",
        label="Logs folder:",
        tooltip=(
            "Description: Directory used for application logs and fatal fault reporting output.\n"
            "Impact: Useful for diagnostics; faster growth is expected when verbose errors occur.\n"
            "Impact: Folder is auto-created on startup and when saving config.\n"
            "Example: Default path is <app_data_dir>/logs."
        ),
    ),
    SettingUiMetadata(
        key="screenshot_dir",
        label="Screenshot folder:",
        tooltip=(
            "Description: Location where captured screenshots are written before being referenced in summaries.\n"
            "Impact: Primary source of storage growth when screenshot capture is enabled.\n"
            "Impact: Folder is created automatically if missing.\n"
            "Example: Default path is <app_data_dir>/screenshots."
        ),
    ),
)


UI_SETTINGS_BY_KEY: dict[str, SettingUiMetadata] = {
    item.key: item for item in (*EDITABLE_SETTINGS, *READONLY_SETTINGS)
}
