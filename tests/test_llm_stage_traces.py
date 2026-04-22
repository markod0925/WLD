from __future__ import annotations

import logging
from pathlib import Path

import pytest
import requests

from worklog_diary.core.batching import BatchBuilder, SummaryBatch
from worklog_diary.core.lmstudio_client import LMStudioClient
from worklog_diary.core.models import ActiveInterval, ForegroundInfo, ScreenshotRecord, TextSegment
from worklog_diary.core.storage import SQLiteStorage
from worklog_diary.core.summarizer import Summarizer


class FakeResponse:
    def __init__(self, content: str, status_code: int = 200) -> None:
        self._content = content
        self.status_code = status_code
        self.text = content

    def json(self) -> dict:
        return {"choices": [{"message": {"content": self._content}, "finish_reason": "stop"}]}


def _seed_raw_data(storage: SQLiteStorage, screenshot_path: Path | None = None) -> None:
    info = ForegroundInfo(
        timestamp=10.0,
        hwnd=100,
        pid=200,
        process_name="code.exe",
        window_title="Editor",
    )
    interval_id = storage.start_interval(info, blocked=False)
    storage.close_interval(interval_id, end_ts=20.0)

    storage.insert_text_segments(
        [
            TextSegment(
                id=None,
                start_ts=12.0,
                end_ts=13.0,
                process_name="code.exe",
                window_title="Editor",
                text="a" * 32,
                hotkeys=[],
                raw_key_count=1,
            )
        ]
    )

    if screenshot_path is None:
        return

    screenshot_path.parent.mkdir(parents=True, exist_ok=True)
    screenshot_path.write_bytes(b"fake-image")
    storage.insert_screenshot(
        ScreenshotRecord(
            id=None,
            ts=14.0,
            file_path=str(screenshot_path),
            process_name="code.exe",
            window_title="Editor",
            active_interval_id=interval_id,
        )
    )


def _summary_batch(*, screenshot_path: Path | None = None) -> SummaryBatch:
    screenshots: list[ScreenshotRecord] = []
    if screenshot_path is not None:
        screenshots.append(
            ScreenshotRecord(
                id=None,
                ts=14.0,
                file_path=str(screenshot_path),
                process_name="code.exe",
                window_title="Editor",
                active_interval_id=1,
            )
        )
    return SummaryBatch(
        start_ts=10.0,
        end_ts=20.0,
        active_intervals=[
            ActiveInterval(
                id=1,
                start_ts=10.0,
                end_ts=20.0,
                hwnd=100,
                pid=200,
                process_name="code.exe",
                window_title="Editor",
                blocked=False,
                summarized=False,
            )
        ],
        blocked_intervals=[],
        text_segments=[
            TextSegment(
                id=None,
                start_ts=12.0,
                end_ts=13.0,
                process_name="code.exe",
                window_title="Editor",
                text="a" * 32,
                hotkeys=[],
                raw_key_count=1,
            )
        ],
        screenshots=screenshots,
    )


def _build_summarizer(tmp_path: Path, *, with_screenshot: bool = False) -> tuple[Summarizer, SQLiteStorage]:
    storage = SQLiteStorage(str(tmp_path / "worklog.db"))
    shot_path = tmp_path / "screens" / "shot.png" if with_screenshot else None
    _seed_raw_data(storage, shot_path)
    summarizer = Summarizer(
        storage=storage,
        batch_builder=BatchBuilder(storage=storage, max_text_segments=200, max_screenshots=3),
        lm_client=LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5),
    )
    return summarizer, storage


def _stage_lines(caplog: pytest.LogCaptureFixture) -> str:
    return "\n".join(record.getMessage() for record in caplog.records if record.getMessage().startswith("[LLM]"))


def test_llm_trace_success(tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    summarizer, storage = _build_summarizer(tmp_path, with_screenshot=True)
    caplog.set_level(logging.INFO)
    shot_path = tmp_path / "screens" / "shot.png"

    monkeypatch.setattr(
        requests,
        "post",
        lambda *_args, **_kwargs: FakeResponse(
            '{"summary_text":"done","key_points":["a"],"blocked_activity":[],"metadata":{}}'
        ),
    )
    monkeypatch.setattr(
        summarizer.batch_builder,
        "build_pending_batch",
        lambda **_kwargs: _summary_batch(screenshot_path=shot_path),
    )

    try:
        assert summarizer.flush_pending(reason="test") is not None
        assert summarizer.storage.get_summary_job_status_counts()["succeeded"] == 1
        output = _stage_lines(caplog)
        assert "stage=job_created status=ok" in output
        assert "stage=submission_decision status=proceed" in output
        assert "stage=payload_build status=start" in output
        assert "stage=payload_build status=ok" in output
        assert "stage=request_submit status=start" in output
        assert "stage=request_success status=ok" in output
        assert "stage=response_parse status=start" in output
        assert "stage=response_parse status=ok" in output
        assert "stage=summary_store status=start" in output
        assert "stage=summary_store status=ok" in output
        assert "stage=summary_job_started status=ok" in output
        assert "stage=summary_job_completed status=ok" in output
    finally:
        summarizer.stop()
        storage.close()


def test_llm_trace_connection_failure(tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    summarizer, storage = _build_summarizer(tmp_path)
    caplog.set_level(logging.INFO)

    def fake_post(*_args: object, **_kwargs: object) -> object:
        raise requests.ConnectionError("connection refused")

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(summarizer.batch_builder, "build_pending_batch", lambda **_kwargs: _summary_batch())

    try:
        assert summarizer.flush_pending(reason="test") is None
        assert summarizer.storage.get_summary_job_status_counts()["failed"] == 1
        output = _stage_lines(caplog)
        assert "stage=job_created status=ok" in output
        assert "stage=submission_decision status=proceed" in output
        assert "stage=payload_build status=ok" in output
        assert "stage=request_submit status=start" in output
        assert "stage=http_response status=error" in output
        assert "error_type=ConnectionError" in output
        assert "stage=summary_job_failed status=error" in output
        assert "failed_stage=http_response" in output
    finally:
        summarizer.stop()
        storage.close()


def test_llm_trace_timeout(tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    summarizer, storage = _build_summarizer(tmp_path)
    caplog.set_level(logging.INFO)

    def fake_post(*_args: object, **_kwargs: object) -> object:
        raise requests.Timeout("timed out")

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(summarizer.batch_builder, "build_pending_batch", lambda **_kwargs: _summary_batch())

    try:
        assert summarizer.flush_pending(reason="test") is None
        assert summarizer.storage.get_summary_job_status_counts()["timed_out"] == 1
        output = _stage_lines(caplog)
        assert "stage=request_submit status=start" in output
        assert "stage=http_response status=error" in output
        assert "error_type=Timeout" in output
        assert "stage=summary_job_failed status=error" in output
        assert "failed_stage=http_response" in output
    finally:
        summarizer.stop()
        storage.close()


def test_llm_trace_parse_failure(tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    summarizer, storage = _build_summarizer(tmp_path)
    caplog.set_level(logging.INFO)

    monkeypatch.setattr(requests, "post", lambda *_args, **_kwargs: FakeResponse("not json"))
    monkeypatch.setattr(summarizer.batch_builder, "build_pending_batch", lambda **_kwargs: _summary_batch())

    try:
        assert summarizer.flush_pending(reason="test") is None
        assert summarizer.storage.get_summary_job_status_counts()["failed"] == 1
        output = _stage_lines(caplog)
        assert "stage=request_success status=ok" in output
        assert "stage=response_parse status=start" in output
        assert "stage=response_parse status=error" in output
        assert "stage=summary_job_failed status=error" in output
        assert "failed_stage=response_parse" in output
    finally:
        summarizer.stop()
        storage.close()
