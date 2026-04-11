# WorkLog Diary (Windows MVP)

WorkLog Diary is a local-first Windows desktop tray app that captures work context and periodically generates local summaries via LM Studio.

## What it does

- Tracks the active foreground window and process over time intervals.
- Captures screenshots on a configurable interval.
- Supports `full_screen` and `active_window` screenshot capture modes.
- Captures raw global keyboard events.
- Reconstructs useful text segments from key events.
- Builds summary batches and sends them to a local LM Studio OpenAI-compatible endpoint.
- Stores summary output in SQLite.
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
worklog-diary --config C:\path\to\config.json
```

## Configuration

On first launch, a default config is created at:

- `%LOCALAPPDATA%\WorkLogDiary\config.json` (Windows)

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
- `lmstudio_base_url`
- `lmstudio_model`
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

## Diagnostics and validation tooling

CLI diagnostics command:

```bash
worklog-diary-debug pending
```

Optional explicit DB path:

```bash
worklog-diary-debug pending --db C:\path\to\worklog_diary.db
```

Drain buffered data:

```bash
worklog-diary-debug flush-buffered --reason manual-validation
```

The tray menu also includes **Diagnostics Snapshot** for quick runtime counts/ranges.

## Logging and observability

Logs are written to:

- `%LOCALAPPDATA%\WorkLogDiary\worklog_diary.log` (Windows default)

Important structured events include:

- `foreground_window_change`
- `privacy_block_transition`
- `key_capture_accepted` / `key_capture_skipped`
- `screenshot_captured` / `screenshot_skipped`
- `text_segment_finalized`
- `summary_job_queued` / `summary_job_started` / `summary_job_completed` / `summary_job_failed`
- `summary_drain_started` / `summary_drain_tick` / `summary_drain_finished`
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
7. Trigger **Flush Now (Drain)** from tray or run:
   - `worklog-diary-debug flush-buffered --reason manual-validation`
8. Confirm summary lifecycle logs:
   - `summary_drain_started`
   - `summary_job_queued`
   - `summary_job_started`
   - `summary_job_completed` (or `summary_job_failed`)
   - `summary_drain_finished`
9. Inspect DB state:
   - `worklog-diary-debug pending`
   - Verify pending raw counts drop after successful summaries
10. Verify screenshot purge:
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
