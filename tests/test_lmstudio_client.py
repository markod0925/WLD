from __future__ import annotations

from datetime import date

import pytest
import requests

from worklog_diary.core.batching import SummaryBatch
from worklog_diary.core.errors import LMStudioServiceUnavailableError
from worklog_diary.core.lmstudio_client import LMStudioClient
from worklog_diary.core.lmstudio_prompt import LMStudioPromptBuilder
from worklog_diary.core.models import ActiveInterval, ScreenshotRecord, SummaryRecord, TextSegment


class FakeResponse:
    def __init__(self, content: str, status_code: int = 200) -> None:
        self._content = content
        self.status_code = status_code
        self.text = content

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


def test_lmstudio_client_raises_on_malformed_responses_after_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    responses = iter([FakeResponse("still bad"), FakeResponse("still bad again")])

    def fake_post(*_args: object, **_kwargs: object) -> FakeResponse:
        return next(responses)

    monkeypatch.setattr(requests, "post", fake_post)

    with pytest.raises(LMStudioServiceUnavailableError) as exc_info:
        client.summarize_batch(_summary_batch())

    assert getattr(exc_info.value, "failed_stage", None) == "response_parse"


def test_lmstudio_client_raises_on_non_object_json(monkeypatch: pytest.MonkeyPatch) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    responses = iter([FakeResponse("[1, 2, 3]"), FakeResponse("[1, 2, 3]")])

    def fake_post(*_args: object, **_kwargs: object) -> FakeResponse:
        return next(responses)

    monkeypatch.setattr(requests, "post", fake_post)

    with pytest.raises(LMStudioServiceUnavailableError) as exc_info:
        client.summarize_batch(_summary_batch())

    assert getattr(exc_info.value, "failed_stage", None) == "response_parse"


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


def test_prompt_builder_limits_daily_recap_prompt_budget() -> None:
    builder = LMStudioPromptBuilder(
        max_daily_summaries=20,
        max_text_chars=500,
        max_prompt_chars=2000,
    )
    summaries = [
        SummaryRecord(
            id=i,
            job_id=i,
            start_ts=float(i),
            end_ts=float(i + 1),
            summary_text=("x" * 350) + str(i),
            summary_json={"summary_text": "x" * 350},
            created_ts=float(i + 2),
        )
        for i in range(8)
    ]

    result = builder.build_daily_recap_prompt(day=date(2026, 4, 20), summaries=summaries)

    assert len(result.prompt_text) > 2000
    assert result.metadata["max_prompt_chars"] == 2000


def test_lmstudio_client_daily_recap_splits_large_input_into_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    builder = LMStudioPromptBuilder(max_text_chars=500, max_prompt_chars=1800, max_daily_summaries=50)
    client = LMStudioClient(
        base_url="http://localhost:1234/v1",
        model="test-model",
        timeout_seconds=5,
        prompt_builder=builder,
    )
    calls: list[dict] = []

    def fake_post(*_args: object, **kwargs: object) -> FakeResponse:
        calls.append(kwargs["json"])
        call_index = len(calls)
        return FakeResponse(
            f'{{"summary_text":"response-{call_index}","key_points":[],"blocked_activity":[],"metadata":{{}}}}'
        )

    monkeypatch.setattr(requests, "post", fake_post)
    summaries = [
        SummaryRecord(
            id=i,
            job_id=i,
            start_ts=float(i),
            end_ts=float(i + 1),
            summary_text=("x" * 450) + str(i),
            summary_json={"summary_text": "x" * 450},
            created_ts=float(i + 2),
        )
        for i in range(6)
    ]

    chunks = client._split_daily_recap_chunks(day=date(2026, 4, 20), summaries=summaries)
    recap_text, parsed = client.summarize_daily_recap(day=date(2026, 4, 20), summaries=summaries)

    assert len(chunks) > 1
    assert parsed["metadata"]["intermediate_chunk_count"] == len(chunks)
    if parsed["metadata"].get("aggregation_fallback") == "local_merge_no_progress":
        assert recap_text == parsed["summary_text"]
        assert len(calls) == len(chunks)
    else:
        assert recap_text == f"response-{len(chunks) + 1}"
        assert parsed["summary_text"] == f"response-{len(chunks) + 1}"
        assert len(calls) == len(chunks) + 1


def test_lmstudio_client_daily_recap_rechunks_aggregation_when_needed(monkeypatch: pytest.MonkeyPatch) -> None:
    builder = LMStudioPromptBuilder(max_text_chars=500, max_prompt_chars=1800, max_daily_summaries=50)
    client = LMStudioClient(
        base_url="http://localhost:1234/v1",
        model="test-model",
        timeout_seconds=5,
        prompt_builder=builder,
    )
    calls: list[dict] = []

    def fake_post(*_args: object, **kwargs: object) -> FakeResponse:
        calls.append(kwargs["json"])
        call_index = len(calls)
        # Force long intermediate outputs so aggregated prompts also need chunking.
        summary_text = ("y" * 600) + str(call_index)
        return FakeResponse(
            f'{{"summary_text":"{summary_text}","key_points":[],"blocked_activity":[],"metadata":{{}}}}'
        )

    monkeypatch.setattr(requests, "post", fake_post)
    summaries = [
        SummaryRecord(
            id=i,
            job_id=i,
            start_ts=float(i),
            end_ts=float(i + 1),
            summary_text=("x" * 450) + str(i),
            summary_json={"summary_text": "x" * 450},
            created_ts=float(i + 2),
        )
        for i in range(12)
    ]

    recap_text, parsed = client.summarize_daily_recap(day=date(2026, 4, 20), summaries=summaries)

    assert recap_text
    assert parsed["metadata"]["intermediate_chunk_count"] > 1
    assert parsed["metadata"]["aggregation_fallback"] == "local_merge_no_progress"
    assert parsed["metadata"]["aggregation_rounds"] == 0
    assert len(calls) == parsed["metadata"]["intermediate_chunk_count"]


def test_lmstudio_client_wraps_chunk_planning_prompt_errors() -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)

    def raise_prompt_error(*_args: object, **_kwargs: object) -> None:
        raise TypeError("bad prompt input")

    client.prompt_builder.build_daily_recap_prompt = raise_prompt_error  # type: ignore[method-assign]
    summaries = [
        SummaryRecord(
            id=1,
            job_id=1,
            start_ts=1.0,
            end_ts=2.0,
            summary_text="worked",
            summary_json={"summary_text": "worked"},
            created_ts=3.0,
        )
    ]

    with pytest.raises(LMStudioServiceUnavailableError) as exc_info:
        client.summarize_daily_recap(day=date(2026, 4, 20), summaries=summaries)

    assert getattr(exc_info.value, "failed_stage", None) == "payload_build"
