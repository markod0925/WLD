# WorkLog Diary (Windows MVP)

WorkLog Diary is a local-first Windows desktop tray app that captures work context and periodically generates local summaries via LM Studio.

## What it does

- Tracks the active foreground window and process over time intervals.
- Captures screenshots on a configurable interval.
- Captures raw global keyboard events.
- Reconstructs useful text segments from key events.
- Builds summary batches and sends them to a local LM Studio OpenAI-compatible endpoint.
- Stores summary output in SQLite.
- Purges raw key/text/screenshot data after successful summary persistence.

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
- `foreground_poll_interval_seconds`
- `flush_interval_seconds`
- `lmstudio_base_url`
- `lmstudio_model`
- `db_path`
- `screenshot_dir`

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

After a successful summary:

- Raw key events in summarized range are deleted.
- Reconstructed text segments in summarized range are deleted.
- Screenshot records in summarized range are deleted.
- Screenshot files in summarized range are removed from disk.
- Interval metadata is kept and marked summarized.

If summarization fails:

- Raw data is retained for retry.
- Summary job is marked failed.

## Tests

Run:

```bash
pytest
```

Included tests cover:

- Privacy engine block behavior.
- Text reconstruction behavior.
- Batch-building behavior.

## MVP notes

- This is a practical first version intended for local iteration.
- Manual flush is available from tray menu.
- Scheduled flush runs in the background.
- UI is intentionally minimal and functional.
