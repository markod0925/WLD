from __future__ import annotations

import re
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


class StructuredClient:
    def summarize_batch(self, *_args: object, **_kwargs: object) -> tuple[str, dict]:
        _emit_started(_kwargs)
        payload = {
            "summary_text": "Edited Input_cases.m in MATLAB.",
            "primary_activity": [{"text": "editing a MATLAB input file", "confidence": 0.93}],
            "programs_used": [{"name": "MATLAB", "confidence": 0.98}],
            "files": [
                {
                    "entity_type": "file_path",
                    "entity_value": "U:\\PROJ1\\XXX\\data\\Input_cases.m",
                    "entity_normalized": "u:/proj1/xxx/data/input_cases.m",
                    "evidence_kind": "observed",
                }
            ],
            "conversations": [],
            "task_candidates": [{"text": "review input data shape", "confidence": 0.66}],
            "outcomes": [{"text": "updated MATLAB input handling", "confidence": 0.84}],
            "follow_ups": [{"text": "verify downstream test coverage", "confidence": 0.5}],
            "blocked_activity": [],
            "unknowns": [],
            "evidence_quality": {
                "overall_confidence": 0.91,
                "confidence_notes": ["direct file evidence from window title"],
                "field_confidence": {"files": 0.98},
            },
            "metadata": {"parse_status": "validated"},
        }
        return payload["summary_text"], payload

    def summarize_daily_recap(self, *_args: object, **_kwargs: object) -> tuple[str, dict]:
        _emit_started(_kwargs)
        payload = {
            "executive_summary": "Prepared a focused work recap from structured event summaries.",
            "program_activity_breakdown": [{"program": "MATLAB", "confidence": 0.98}],
            "tasks_advanced": [{"text": "review input data shape", "confidence": 0.66}],
            "files_observed": [{"path": "U:\\PROJ1\\XXX\\data\\Input_cases.m", "confidence": 0.98}],
            "files_likely_modified": [],
            "conversations_or_meetings": [],
            "decisions": [{"text": "keep the MATLAB path as the source of truth", "confidence": 0.7}],
            "blockers": [],
            "follow_ups": [{"text": "verify downstream test coverage", "confidence": 0.5}],
            "jira_update_candidates": [{"text": "summarize MATLAB input handling", "confidence": 0.55}],
            "open_questions": [],
            "confidence_notes": ["structured event payloads were available"],
            "metadata": {"parse_status": "validated"},
        }
        return payload["executive_summary"], payload


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


def test_lmstudio_client_degrades_on_malformed_responses_after_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    responses = iter([FakeResponse("still bad"), FakeResponse("still bad again")])

    def fake_post(*_args: object, **_kwargs: object) -> FakeResponse:
        return next(responses)

    monkeypatch.setattr(requests, "post", fake_post)

    summary_text, parsed = client.summarize_batch(_summary_batch())

    assert summary_text == "still bad again"
    assert parsed["summary_text"] == "still bad again"
    assert parsed["unknowns"] == ["insufficient evidence"]
    assert parsed["metadata"]["parse_status"] == "degraded"
    assert parsed["metadata"]["parse_error"] == "Malformed JSON response"
    assert parsed["raw_response"] == "still bad again"


def test_lmstudio_client_degrades_on_non_object_json(monkeypatch: pytest.MonkeyPatch) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    responses = iter([FakeResponse("[1, 2, 3]"), FakeResponse("[1, 2, 3]")])

    def fake_post(*_args: object, **_kwargs: object) -> FakeResponse:
        return next(responses)

    monkeypatch.setattr(requests, "post", fake_post)

    summary_text, parsed = client.summarize_batch(_summary_batch())

    assert summary_text == "[1, 2, 3]"
    assert parsed["summary_text"] == "[1, 2, 3]"
    assert parsed["unknowns"] == ["insufficient evidence"]
    assert parsed["metadata"]["parse_status"] == "degraded"
    assert parsed["raw_response"] == "[1, 2, 3]"


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


def test_lmstudio_client_daily_recap_accepts_task_file_first_sections(monkeypatch: pytest.MonkeyPatch) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    response = FakeResponse(
        '{"executive_summary":"daily recap",'
        '"workstreams_or_task_candidates":[{"text":"Finalize export notes"}],'
        '"files_and_documents":[{"path/name":"ginopino.pdf","status":"read_or_viewed"}],'
        '"conversations_meetings_and_references":[{"reference":"Backend review - Webex"}],'
        '"program_activity_breakdown":[{"program":"code.exe"}],'
        '"outcomes":[{"text":"Prepared update draft"}],'
        '"follow_ups_or_jira_candidates":[{"text":"Update WLD-7"}],'
        '"evidence_limits_and_unknowns":["Blocked content unavailable"],'
        '"metadata":{}}'
    )
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
    assert parsed["workstreams_or_task_candidates"]
    assert parsed["files_and_documents"][0]["path/name"] == "ginopino.pdf"
    assert parsed["evidence_limits_and_unknowns"] == ["Blocked content unavailable"]


def test_daily_recap_prompt_requests_short_highlight_list() -> None:
    builder = LMStudioPromptBuilder(max_daily_summaries=5, max_summary_text_segments=20)
    summaries = [
        SummaryRecord(
            id=1,
            job_id=1,
            start_ts=1.0,
            end_ts=2.0,
            summary_text="worked on recap formatting and review flow",
            summary_json={"summary_text": "worked on recap formatting and review flow"},
            created_ts=3.0,
        )
    ]

    result = builder.build_daily_recap_prompt(day=date(2026, 4, 14), summaries=summaries)

    assert "daily evidence aggregate" in result.prompt_text
    assert "summary_index" in result.prompt_text
    assert "confidence_notes" in result.prompt_text
    assert "executive_summary" in result.prompt_text
    assert "workstreams_or_task_candidates" in result.prompt_text
    assert "files_and_documents" in result.prompt_text
    assert "Do not use phrases like 'the user', 'interacting with', or 'activity log covers'." in result.prompt_text
    assert "Never include raw Unix timestamps in generated prose" in result.prompt_text


def test_summary_prompt_keeps_blocked_intervals_as_unknown_or_blocked() -> None:
    batch = SummaryBatch(
        start_ts=1.0,
        end_ts=2.0,
        active_intervals=[
            ActiveInterval(
                id=1,
                start_ts=1.0,
                end_ts=2.0,
                hwnd=1,
                pid=2,
                process_name="explorer.exe",
                window_title="Downloads",
                blocked=True,
                summarized=False,
            )
        ],
        blocked_intervals=[
            ActiveInterval(
                id=2,
                start_ts=2.0,
                end_ts=3.0,
                hwnd=3,
                pid=4,
                process_name="explorer.exe",
                window_title="Downloads",
                blocked=True,
                summarized=False,
            )
        ],
        text_segments=[],
        screenshots=[],
    )
    builder = LMStudioPromptBuilder()

    result = builder.build_summary_prompt(batch)

    assert "blocked content" in result.prompt_text
    assert "window-title metadata" in result.prompt_text
    assert "blocked_intervals" in result.prompt_text


def test_summary_prompt_prioritizes_task_and_file_sections_before_program_prose() -> None:
    builder = LMStudioPromptBuilder()
    result = builder.build_summary_prompt(_summary_batch())

    assert "task_candidates, files_and_documents, conversations_or_references, programs_used" in result.prompt_text
    assert "Do not lead with generic program prose unless no better evidence exists." in result.prompt_text
    assert "blocked_observed_references" in result.prompt_text
    assert "Do not use phrases like 'the user', 'interacting with', or 'activity log covers'." in result.prompt_text
    assert "Never include raw Unix timestamps in generated prose" in result.prompt_text


def test_lmstudio_client_degrades_generic_event_to_unknowns(monkeypatch: pytest.MonkeyPatch) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    response = FakeResponse(
        '{"summary_text":"Worked on something","primary_activity":[],"programs_used":[],"files":[],"conversations":[],"task_candidates":[],"outcomes":[],"follow_ups":[],"blocked_activity":[],"unknowns":[],"evidence_quality":{"overall_confidence":0.1,"confidence_notes":["thin evidence"],"field_confidence":{}},"metadata":{}}'
    )

    monkeypatch.setattr(requests, "post", lambda *_args, **_kwargs: response)

    summary_text, parsed = client.summarize_batch(_summary_batch())

    assert summary_text == "Worked on something"
    assert parsed["unknowns"] == ["insufficient evidence"]
    assert parsed["evidence_quality"]["overall_confidence"] == 0.1
    assert parsed["files"] == []
    assert parsed["task_candidates"] == []


def test_lmstudio_client_accepts_new_event_structured_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    response = FakeResponse(
        '{"summary_text":"task-focused","task_candidates":[{"text":"Prepare JIRA update"}],'
        '"files_and_documents":[{"path/name":"ginopino.pdf","status":"read_or_viewed"}],'
        '"conversations_or_references":[{"reference":"Backend review - Webex"}],'
        '"blocked_observed_references":[{"process":"chrome.exe","window_title":"ginopino.pdf - Google Chrome","inferred_reference_type":"pdf","content_captured":false,"metadata_used":true}],'
        '"jira_update_candidates":[{"text":"Update WLD-101"}],'
        '"unknowns_and_privacy_limits":["Blocked browser content unavailable"],'
        '"programs_used":[{"name":"chrome.exe"}],'
        '"outcomes":[{"text":"Collected references"}],'
        '"follow_ups":[{"text":"Post update"}],'
        '"evidence_quality":{"overall_confidence":0.8,"confidence_notes":[],"field_confidence":{}},'
        '"metadata":{}}'
    )
    monkeypatch.setattr(requests, "post", lambda *_args, **_kwargs: response)

    _summary_text, parsed = client.summarize_batch(_summary_batch())

    assert parsed["task_candidates"]
    assert parsed["files_and_documents"][0]["path/name"] == "ginopino.pdf"
    assert parsed["blocked_observed_references"][0]["content_captured"] is False
    assert parsed["unknowns_and_privacy_limits"] == ["Blocked browser content unavailable"]


def test_lmstudio_client_program_only_event_keeps_generic_summary_with_unknowns(monkeypatch: pytest.MonkeyPatch) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    response = FakeResponse(
        '{"summary_text":"Worked in Chrome","programs_used":[{"name":"chrome.exe"}],'
        '"task_candidates":[],"files":[],"conversations":[],"outcomes":[],"follow_ups":[],"unknowns":[],"blocked_activity":[],'
        '"evidence_quality":{"overall_confidence":0.2,"confidence_notes":["program-only evidence"],"field_confidence":{}},'
        '"metadata":{}}'
    )
    monkeypatch.setattr(requests, "post", lambda *_args, **_kwargs: response)

    summary_text, parsed = client.summarize_batch(_summary_batch())

    assert summary_text == "Worked in Chrome"
    assert parsed["programs_used"] == [{"name": "chrome.exe"}]
    assert parsed["unknowns"] == ["insufficient evidence"]


def test_lmstudio_client_sanitizes_timestamp_and_template_phrasing(monkeypatch: pytest.MonkeyPatch) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    response = FakeResponse(
        '{"summary_text":"activity log covers 1715606400.0 while interacting with MATLAB","task_candidates":[],"files":[],"conversations":[],"outcomes":[],"follow_ups":[],"unknowns":[],"blocked_activity":[],"evidence_quality":{"overall_confidence":0.6,"confidence_notes":[],"field_confidence":{}},"metadata":{}}'
    )

    monkeypatch.setattr(requests, "post", lambda *_args, **_kwargs: response)

    summary_text, parsed = client.summarize_batch(_summary_batch())

    assert "1715606400.0" not in summary_text
    assert "activity log covers" not in summary_text.lower()
    assert "interacting with" not in summary_text.lower()
    assert re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", summary_text)
    assert parsed["summary_text"] == summary_text


def test_lmstudio_client_sanitizes_nested_generated_prose_without_touching_file_identifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    response = FakeResponse(
        '{"summary_text":"ok",'
        '"outcomes":[{"text":"activity log covers 1715606400 while interacting with parser"}],'
        '"unknowns_and_privacy_limits":["activity log covers 1715606401"],'
        '"files_and_documents":[{"path/name":"1715606400.log"}],'
        '"task_candidates":[{"text":"SLG-487"}],'
        '"blocked_activity":[],"follow_ups":[],"programs_used":[],"conversations":[],"unknowns":[],"metadata":{}}'
    )

    monkeypatch.setattr(requests, "post", lambda *_args, **_kwargs: response)

    _summary_text, parsed = client.summarize_batch(_summary_batch())

    assert parsed["outcomes"][0]["text"].startswith("Observed activity covers ")
    assert "interacting with" not in parsed["outcomes"][0]["text"].lower()
    assert parsed["unknowns_and_privacy_limits"][0].startswith("Observed activity covers ")
    assert parsed["files_and_documents"][0]["path/name"] == "1715606400.log"
    assert parsed["task_candidates"][0]["text"] == "SLG-487"


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
    naive_payload = {
        "schema": "worklog.lmstudio.daily_recap.v1",
        "day": date(2026, 4, 20).isoformat(),
        "structured_event_outputs": [
            {
                "summary_id": item.id,
                "summary_text": item.summary_text,
                "task_candidates": [],
                "files_and_documents": [],
                "programs_used": [],
                "activity_entities": [],
                "parser_coverage": [],
            }
            for item in summaries
        ],
        "confidence_notes": [],
    }
    naive_prompt = builder._render_prompt(  # noqa: SLF001
        title="Create a short daily recap for 2026-04-20 from the following batch summaries.",
        instructions="Return only strict JSON.",
        payload=naive_payload,
        metadata={"response_kind": "daily_recap"},
    )

    assert len(result.prompt_text) < len(naive_prompt)
    assert result.metadata["max_prompt_chars"] == 2000


def test_daily_recap_prompt_aggregate_preserves_program_file_and_task_evidence() -> None:
    builder = LMStudioPromptBuilder(max_daily_summaries=10, max_text_chars=500, max_prompt_chars=4000)
    summaries = [
        SummaryRecord(
            id=1,
            job_id=1,
            start_ts=10.0,
            end_ts=20.0,
            summary_text="Worked on SLG-487 in MATLAB",
            summary_json={
                "summary_text": "Worked on SLG-487 in MATLAB",
                "programs_used": [{"name": "matlab.exe"}],
                "files_and_documents": [{"path/name": "buildImplicitHeliModel.m"}],
                "task_candidates": [{"text": "SLG-487"}],
                "source_context": {"window_title": "buildImplicitHeliModel.m - MATLAB"},
                "activity_entities": [
                    {"entity_type": "file_path", "entity_value": r"U:\\PROJ1\\buildImplicitHeliModel.m", "entity_normalized": "u:/proj1/buildimplicithelimodel.m"},
                    {"entity_type": "task_candidate", "entity_value": "SLG-487", "entity_normalized": "slg-487"},
                    {"entity_type": "program", "entity_value": "matlab.exe", "entity_normalized": "matlab.exe"},
                ],
            },
            created_ts=30.0,
        )
    ]

    result = builder.build_daily_recap_prompt(day=date(2026, 4, 20), summaries=summaries)

    assert "matlab.exe" in result.prompt_text
    assert "buildImplicitHeliModel.m" in result.prompt_text
    assert "SLG-487" in result.prompt_text


def test_daily_recap_prompt_preserves_meaningful_false_flags_in_summary_index() -> None:
    builder = LMStudioPromptBuilder(max_daily_summaries=10, max_text_chars=500, max_prompt_chars=4000)
    summaries = [
        SummaryRecord(
            id=1,
            job_id=1,
            start_ts=10.0,
            end_ts=20.0,
            summary_text="Worked on parser cleanup",
            summary_json={
                "summary_text": "Worked on parser cleanup",
                "programs_used": [{"name": "code.exe"}],
                "activity_entities": [],
            },
            created_ts=30.0,
        )
    ]

    result = builder.build_daily_recap_prompt(day=date(2026, 4, 20), summaries=summaries)

    assert '"blocked": false' in result.prompt_text


def test_prompt_builder_derives_text_char_limit_from_text_segment_budget() -> None:
    default_builder = LMStudioPromptBuilder(max_summary_text_segments=400)
    smaller_builder = LMStudioPromptBuilder(max_summary_text_segments=100)

    assert default_builder.max_text_chars == 2000
    assert smaller_builder.max_text_chars == 500


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


def test_lmstudio_client_logs_timeout_category(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=1)
    caplog.set_level("INFO")

    def fake_post(*_args: object, **_kwargs: object) -> FakeResponse:
        raise requests.Timeout("slow")

    monkeypatch.setattr(requests, "post", fake_post)
    with pytest.raises(Exception):
        client.summarize_batch(_summary_batch())

    assert any("event=lmstudio_request_timeout" in rec.message and "category=timeout" in rec.message for rec in caplog.records)


def test_lmstudio_client_backs_off_between_malformed_response_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    responses = iter([FakeResponse("still bad"), FakeResponse("still bad again")])
    sleeps: list[float] = []

    monkeypatch.setattr(requests, "post", lambda *_args, **_kwargs: next(responses))
    monkeypatch.setattr("worklog_diary.core.lmstudio_client.time.sleep", lambda seconds: sleeps.append(seconds))

    client.summarize_batch(_summary_batch())

    assert sleeps == [0.25]


def test_lmstudio_request_id_correlates_start_and_success(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    caplog.set_level("INFO")
    monkeypatch.setattr(
        requests,
        "post",
        lambda *_args, **_kwargs: FakeResponse('{"summary_text":"ok","key_points":[],"blocked_activity":[],"metadata":{}}'),
    )

    client.summarize_batch(_summary_batch(), job_id="job-42")
    start_line = next(rec.message for rec in caplog.records if "event=lmstudio_request_start" in rec.message)
    success_line = next(rec.message for rec in caplog.records if "event=lmstudio_request_success" in rec.message)
    start_id = start_line.split("lm_request_id=")[1].split()[0]
    success_id = success_line.split("lm_request_id=")[1].split()[0]
    assert start_id == success_id
    assert "summary_job_id=job-42" in start_line
    assert "summary_job_id=job-42" in success_line
