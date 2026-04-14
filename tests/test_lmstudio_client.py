from __future__ import annotations

from datetime import date

import pytest
import requests

from worklog_diary.core.batching import SummaryBatch
from worklog_diary.core.lmstudio_client import LMStudioClient
from worklog_diary.core.lmstudio_prompt import LMStudioPromptBuilder
from worklog_diary.core.models import ActiveInterval, ScreenshotRecord, SummaryRecord, TextSegment


class FakeResponse:
    def __init__(self, content: str) -> None:
        self._content = content

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {"choices": [{"message": {"content": self._content}}]}


def _summary_batch() -> SummaryBatch:
    return SummaryBatch(
        start_ts=1.0,
        end_ts=2.0,
        active_intervals=[
            ActiveInterval(
                id=1,
                start_ts=1.0,
                end_ts=2.0,
                hwnd=1,
                pid=2,
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
                start_ts=1.0,
                end_ts=1.1,
                process_name="code.exe",
                window_title="Editor",
                text="x" * 2000,
                hotkeys=[],
                raw_key_count=1,
            ),
            TextSegment(
                id=None,
                start_ts=1.2,
                end_ts=1.3,
                process_name="code.exe",
                window_title="Editor",
                text="second",
                hotkeys=[],
                raw_key_count=1,
            ),
        ],
        screenshots=[
            ScreenshotRecord(
                id=None,
                ts=1.5,
                file_path="missing.png",
                process_name="code.exe",
                window_title="Editor",
                active_interval_id=1,
            ),
            ScreenshotRecord(
                id=None,
                ts=1.6,
                file_path="missing-2.png",
                process_name="code.exe",
                window_title="Editor",
                active_interval_id=1,
            ),
        ],
    )


def test_prompt_builder_truncates_input_and_adds_metadata() -> None:
    builder = LMStudioPromptBuilder(
        max_summary_text_segments=2,
        max_summary_screenshots=2,
        max_daily_summaries=2,
        max_text_chars=12,
    )

    result = builder.build_summary_prompt(_summary_batch())

    assert result.metadata["response_kind"] == "summary"
    assert result.metadata["truncated"] is True
    assert result.metadata["included_counts"]["text_segments"] == 2
    assert "Prompt metadata" in result.prompt_text
    assert "xxxxxxxxxxxx..." in result.prompt_text


def test_lmstudio_client_retries_malformed_response_then_parses(monkeypatch: pytest.MonkeyPatch) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    calls: list[dict] = []
    responses = iter(
        [
            FakeResponse("not json"),
            FakeResponse('{"summary_text":"done","key_points":["a"],"blocked_activity":[],"metadata":{}}'),
        ]
    )

    def fake_post(*_args: object, **kwargs: object) -> FakeResponse:
        calls.append(kwargs["json"])
        return next(responses)

    monkeypatch.setattr(requests, "post", fake_post)

    summary_text, parsed = client.summarize_batch(_summary_batch())

    assert summary_text == "done"
    assert parsed["summary_text"] == "done"
    assert parsed["key_points"] == ["a"]
    assert parsed["metadata"]["parse_status"] == "validated"
    assert len(calls) == 2
    assert "invalid" in calls[1]["messages"][1]["content"].lower()


def test_lmstudio_client_falls_back_after_malformed_responses(monkeypatch: pytest.MonkeyPatch) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    responses = iter([FakeResponse("still bad"), FakeResponse("still bad again")])

    def fake_post(*_args: object, **_kwargs: object) -> FakeResponse:
        return next(responses)

    monkeypatch.setattr(requests, "post", fake_post)

    summary_text, parsed = client.summarize_batch(_summary_batch())

    assert summary_text == "still bad again"
    assert parsed["metadata"]["parse_status"] == "fallback"
    assert parsed["metadata"]["attempts"] == 2
    assert parsed["key_points"] == []
    assert parsed["blocked_activity"] == []


def test_lmstudio_client_daily_recap_uses_structured_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    response = FakeResponse('{"summary_text":"daily recap","key_points":["one"],"blocked_activity":[],"metadata":{}}')

    monkeypatch.setattr(requests, "post", lambda *_args, **_kwargs: response)

    recap_text, parsed = client.summarize_daily_recap(
        day=date(2026, 4, 14),
        summaries=[
            SummaryRecord(
                id=1,
                job_id=1,
                start_ts=1.0,
                end_ts=2.0,
                summary_text="worked",
                summary_json={"summary_text": "worked", "key_points": []},
                created_ts=3.0,
            )
        ],
    )

    assert recap_text == "daily recap"
    assert parsed["summary_text"] == "daily recap"
    assert parsed["metadata"]["response_kind"] == "daily_recap"
