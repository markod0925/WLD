# WorkLog Diary (Windows MVP)

This file preserves the technical and implementation details from the end-user README.

## Overview

WorkLog Diary is a local-first Windows desktop tray app that captures work context and periodically generates local summaries via LM Studio.

## What it does

- Tracks the active foreground window and process over time intervals.
- Captures screenshots on a configurable interval.
- Supports `full_screen` and `active_window` screenshot capture modes.
- Filters near-duplicate screenshots before summary generation so batches stay more informative.
- Captures raw global keyboard events.
- Reconstructs useful text segments from key events.
- Automatically pauses monitoring when the Windows session is locked, then resumes after unlock.
- Builds summary batches and sends them to a local LM Studio OpenAI-compatible endpoint.
- Stores summary output in SQLite.
- Provides a calendar-based summaries viewer with per-day summary highlighting.
- Supports manual daily recap generation ("summary of summaries") from existing batch summaries.
- Purges raw key/text/screenshot data per successful batch.

## Windows-only scope

This MVP is Windows-focused.

- Foreground window tracking uses Win32 APIs.
- Global keyboard capture uses a global listener suitable for Windows desktop usage.
- The app is intended to run as a tray/background app on Windows 10/11.

## Privacy block rule

If the foreground process is blocked, WorkLog Diary will:

- Not capture screenshots.
- Not capture raw keyboard input.
- Not reconstruct text.
- Only store minimal blocked interval metadata.

When the Windows session is locked, WorkLog Diary enters `Paused (PC locked)` mode:

- no screenshots are captured,
- no raw keyboard events are recorded,
- no non-forced text reconstruction loop work proceeds.

Default blocked processes:

- `chrome.exe`
- `msedge.exe`
- `webex.exe`

You can configure this list in settings or config JSON.

## Project structure

- `src/worklog_diary/core`: config, storage, privacy, tracking, capture, reconstruction, batching, LM Studio client, summarizer, scheduler, service coordination.
- `src/worklog_diary/ui`: tray menu, settings window, summaries window.
- `tests`: core unit tests.

## Requirements

- Python 3.11+
- Windows desktop session
- LM Studio running locally (for summarization)

## Installation

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
```

## Run

Tray app:

```bash
worklog-diary
```

Headless mode (no PySide tray):

```bash
worklog-diary --headless
```

Custom config path:

```bash
worklog-diary --config C:\\path\\to\\config.json
```

## Portable packaging with PyInstaller

The project is set up for a Windows `onedir` bundle built with PyInstaller.

Install PyInstaller into the active Python environment first if it is not already available:

```powershell
pip install pyinstaller
```

Build the portable app from the repository root:

```powershell
pyinstaller --noconfirm --clean WLD.spec
```

If PyInstaller cannot clean `build/WLD/` because Windows has a file lock on `localpycs`, close any running `WLD.exe` instance and retry. If the folder still cannot be removed, use a fresh work directory for that build:

```powershell
pyinstaller --noconfirm --clean --workpath build-pyi --distpath dist-pyi WLD.spec
```

That creates `dist/WLD/` with the executable and bundled runtime files. The main items are:

- `WLD.exe`
- `_internal/`
- bundled `README.md`
- bundled `sample_config.json`

On first launch, the frozen app creates a portable `data/` directory next to `WLD.exe` if it does not already exist:

- `data/config.json`
- `data/worklog_diary.db`
- `data/screenshots/`
- `data/logs/worklog_diary.log`

To test on another Windows PC, copy the entire `dist/WLD/` folder to the target machine and launch `WLD.exe` from that folder. Keep the full folder structure intact so the executable stays adjacent to its `data/` directory.

The bundled app still uses the same LM Studio settings as the dev build. LM Studio must be installed and running on the target PC, and the local OpenAI-compatible endpoint must match the configured `lmstudio_base_url` and `lmstudio_model`.

Because the executable is not code-signed by default, Windows SmartScreen or Defender may show a warning on first launch. That is expected for unsigned local builds. Signing the binary with a trusted code-signing certificate is the standard way to reduce those prompts.

## Configuration

On first launch, a default config is created automatically.

Portable frozen builds use:

- `.\\data\\config.json`

The app keeps the existing dev fallback for normal runs:

- `%LOCALAPPDATA%\\WorkLogDiary\\config.json` (Windows)
- `~/.worklog_diary/config.json` (non-Windows fallback used by the code)

Use `sample_config.json` as a reference.

Important fields:

- `blocked_processes`
- `screenshot_interval_seconds`
- `capture_mode`: `full_screen` or `active_window`
- `foreground_poll_interval_seconds`
- `flush_interval_seconds`
- `max_parallel_summary_jobs`: concurrent summary worker limit (default `2`)
- `max_text_segments_per_summary`
- `max_screenshots_per_summary`
- `screenshot_dedup_enabled`
- `screenshot_dedup_threshold`
- `screenshot_min_keep_interval_seconds`
- `lmstudio_base_url`
- `lmstudio_model`
- `log_dir`
- `db_path`
- `screenshot_dir`

### Screenshot capture mode

- `full_screen`: captures the full virtual desktop.
- `active_window`: captures only the foreground-window bounds, clipped to the visible desktop region.

Active-window mode handles common edge cases:

- Invalid window rectangle: skip screenshot.
- Minimized window: skip screenshot.
- Partially off-screen window: crop to visible region.
- Blocked app: still skips screenshot entirely.

### Screenshot deduplication

Screenshot capture still runs on the normal interval, but summary batches prefer visually distinct images.

The default behavior is conservative:

- `screenshot_dedup_enabled`: `true`
- `screenshot_dedup_threshold`: `6`
- `screenshot_min_keep_interval_seconds`: `120`

The batch builder keeps a screenshot when:

- the active window changes,
- the window title changes,
- the visual fingerprint differs enough,
- enough time has passed since the last kept screenshot.

This is a selection-time filter only. Screenshots are still captured normally and stored in SQLite.

## Flush behavior and backlog draining

### Flush Now (tray and debug CLI)

`Flush Now` is a drain operation, not a single batch:

- repeatedly reconstructs pending text and enqueues summary batches,
- continues until backlog is empty,
- stops on:
  - empty buffer,
  - summary failure (treated as unrecoverable for that drain run),
  - manual cancel (`Stop Flush Drain` in tray).

### Concurrent summary workers

Summary processing uses a bounded queue/worker dispatcher:

- maximum concurrency is controlled by `max_parallel_summary_jobs`,
- batches are reserved before execution to avoid duplicate processing,
- purge runs only for each batch that succeeds,
- failed batches keep raw data retryable.

## Tray backlog indicators

The tray menu and tooltip expose:

- monitoring state:
  - `Monitoring`
  - `Paused`
  - `Paused (PC locked)`
- pending screenshot count,
- pending text segment count,
- pending summary job count,
- summary running state,
- summary job counts (`queued`, `running`, `completed`, `failed`, `cancelled`),
- synthesized buffer state:
  - `Buffer empty`
  - `Buffer pending`
  - `Summarizing`
  - `Summarizing, backlog remaining`
- approximate remaining batch count.

## Summaries viewer and daily recap

The summaries window is day-centric:

- left pane: calendar day selector,
- highlighted days (light blue) when one or more summaries exist,
- right pane: structured summary cards for the selected day.

Each summary card includes:

- time range,
- summary text,
- major activities (when present),
- blocked/unanalyzed notes (when present),
- uncertainty/notes (when present).

Daily recap generation is manual:

- select a day in the summaries window,
- click `Generate Daily Recap`,
- the app aggregates that day's batch summaries and asks LM Studio for a concise recap,
- the recap is stored separately from batch summaries,
- regenerating the same day replaces the existing daily recap for that day.

## LM Studio setup

1. Start LM Studio and load a local model.
2. Enable local server / OpenAI-compatible endpoint.
3. Set config values:
   - `lmstudio_base_url` (default: `http://127.0.0.1:1234/v1`)
   - `lmstudio_model` (must match your loaded model id/name)

The app sends structured activity context and optional screenshot images to `/chat/completions`.

## Data storage and purge behavior

SQLite DB stores:

- Foreground intervals and blocked intervals metadata.
- Temporary raw key events.
- Temporary reconstructed text segments.
- Temporary screenshot records.
- Summary jobs and final summaries.
- Daily recap rows (`daily_summaries`) keyed by calendar day.

After a successful summary batch:

- Raw key events in the summarized range are deleted.
- Reconstructed text segments in the summarized range are deleted.
- Screenshot records in the summarized range are deleted.
- Screenshot files in the summarized range are removed from disk.
- Interval metadata is kept and marked summarized.

If summarization fails:

- Raw data is retained for retry.
- Summary job is marked failed.
- Drain mode stops and leaves remaining backlog pending.

## Tests

Run:

```bash
pytest
```

Included tests cover:

- Privacy engine block behavior.
- Text reconstruction behavior.
- Batch-building behavior.
- Flush drain-until-empty behavior.
- Summary concurrency limit behavior.
- Active-window screenshot region selection logic.
- Session lock/unlock monitoring pause/resume logic.
- Summary day queries and daily recap persistence behavior.
- Summaries day-view model preparation (calendar/day data shaping).

## Diagnostics and validation tooling

CLI diagnostics command:

```bash
worklog-diary-debug pending
```

Optional explicit DB path:

```bash
worklog-diary-debug pending --db C:\\path\\to\\worklog_diary.db
```

Drain buffered data:

```bash
worklog-diary-debug flush-buffered --reason manual-validation
```

The tray menu also includes **Diagnostics Snapshot** for quick runtime counts/ranges.

## Logging and observability

Logs are written to:

- `%LOCALAPPDATA%\\WorkLogDiary\\worklog_diary.log` (Windows default)

Important structured events include:

- `foreground_window_change`
- `privacy_block_transition`
- `key_capture_accepted` / `key_capture_skipped`
- `screenshot_captured` / `screenshot_skipped`
- `text_segment_finalized`
- `summary_job_queued` / `summary_job_started` / `summary_job_completed` / `summary_job_failed`
- `summary_drain_started` / `summary_drain_tick` / `summary_drain_finished`
- `session_locked` / `session_unlocked`
- `monitoring_paused_by_lock` / `monitoring_resumed_after_unlock`
- `calendar_summary_load`
- `daily_recap_generation_started` / `daily_recap_generation_succeeded` / `daily_recap_generation_failed`
- `daily_recap_replaced`
- `purge_actions`

To enable debug-level capture logs:

```bash
set WORKLOG_DIARY_LOG_LEVEL=DEBUG
```

## Manual validation checklist (Windows)

1. Start the app and enable monitoring.
2. Open a non-blocked app (for example `notepad.exe`) and type text with:
   - normal letters
   - `Shift` + letters
   - `Backspace`
   - `Enter`
   - `Ctrl` hotkeys (for example `Ctrl+C`, `Ctrl+V`)
3. Confirm in logs:
   - `key_capture_accepted` events for non-blocked windows
   - `text_segment_finalized` events with expected text/hotkeys
4. Switch to a blocked app (for example `chrome.exe` if blocked).
5. Confirm in logs:
   - `privacy_block_transition ... blocked=true`
   - `key_capture_skipped reason=blocked_process`
   - `screenshot_skipped reason=blocked`
6. Switch back to a non-blocked app and confirm:
   - `privacy_block_transition ... blocked=false`
   - `key_capture_accepted` resumes
   - `screenshot_captured` resumes
7. Lock the Windows session, then unlock it, and confirm:
   - `session_locked` / `session_unlocked`
   - `monitoring_paused_by_lock` then `monitoring_resumed_after_unlock`
   - no new key/screenshot capture events during the lock period
8. Trigger **Flush Now (Drain)** from tray or run:
   - `worklog-diary-debug flush-buffered --reason manual-validation`
9. Confirm summary lifecycle logs:
   - `summary_drain_started`
   - `summary_job_queued`
   - `summary_job_started`
   - `summary_job_completed` (or `summary_job_failed`)
   - `summary_drain_finished`
10. Inspect DB state:
   - `worklog-diary-debug pending`
   - Verify pending raw counts drop after successful summaries
11. Verify screenshot purge:
   - files referenced in summarized ranges are removed from screenshot directory
   - no stale screenshot records remain in `screenshots` table for purged ranges

## Privacy blocking verification

- Ensure sensitive applications are present in `blocked_processes`.
- While one blocked app is foreground:
  - no new key events should be inserted
  - no new screenshots should be inserted
  - no text should be reconstructed
  - only interval/block metadata should advance
- Use `worklog-diary-debug pending` and logs to confirm behavior.

## Flush and purge verification

- Successful summary batch:
  - summary row added
  - summary job marked `succeeded`
  - intervals in summarized range marked summarized
  - raw key/text/screenshot rows in summarized range removed
  - screenshot files removed from disk
- Failed summary batch:
  - summary job marked `failed`
  - raw key/text/screenshot rows remain retryable
  - no purge action runs for that failed job

## MVP notes

- This is a practical first version intended for local iteration.
- Manual drain flush is available from tray menu.
- Scheduled flush runs in the background.
- UI is intentionally minimal and functional.
