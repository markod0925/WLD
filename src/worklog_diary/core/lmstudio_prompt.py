from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

from .batching import SummaryBatch
from .models import SummaryRecord

TEXT_CHARS_PER_SUMMARY_SEGMENT = 5


@dataclass(slots=True)
class PromptBuildResult:
    prompt_text: str
    metadata: dict[str, Any]


class LMStudioPromptBuilder:
    def __init__(
        self,
        *,
        max_summary_text_segments: int = 120,
        max_summary_screenshots: int = 3,
        max_daily_summaries: int = 120,
        max_text_chars: int | None = None,
        max_prompt_chars: int = 20000,
    ) -> None:
        self.max_summary_text_segments = max(1, int(max_summary_text_segments))
        self.max_summary_screenshots = max(1, int(max_summary_screenshots))
        self.max_daily_summaries = max(1, int(max_daily_summaries))
        self.max_prompt_chars = max(2000, int(max_prompt_chars))
        derived_max_text_chars = self.max_summary_text_segments * TEXT_CHARS_PER_SUMMARY_SEGMENT
        self.max_text_chars = max(1, int(max_text_chars if max_text_chars is not None else derived_max_text_chars))

    def build_summary_prompt(self, batch: SummaryBatch) -> PromptBuildResult:
        payload, metadata = self._build_summary_payload(batch)
        prompt_text = self._render_prompt(
            title="Summarize the following WorkLog Diary activity batch.",
            instructions=(
                "Return only valid JSON with keys summary_text, key_points, blocked_activity, metadata. "
                "Treat blocked intervals as intentionally redacted privacy windows."
            ),
            payload=payload,
            metadata=metadata,
        )
        return PromptBuildResult(prompt_text=prompt_text, metadata=metadata)

    def build_daily_recap_prompt(self, day: date, summaries: list[SummaryRecord]) -> PromptBuildResult:
        payload, metadata = self._build_daily_recap_payload(day=day, summaries=summaries)
        prompt_text = self._render_prompt(
            title=f"Create a short daily recap for {day.isoformat()} from the following batch summaries.",
            instructions=(
                "Return only valid JSON with keys summary_text, key_points, blocked_activity, metadata. "
                "Keep summary_text short, ideally 2 to 4 sentences. "
                "Make key_points a compact highlights list with 3 to 6 entries, each using the format "
                "\"Category: short description\". "
                "Prefer categories such as \"Programma/Attività\", \"Decisioni\", \"Blocchi\", and \"Follow-up\". "
                "Use key_points to capture the most useful actions and outcomes from the day, not generic prose. "
                "Keep blocked_activity brief and only mention meaningful privacy or blocked-work notes."
            ),
            payload=payload,
            metadata=metadata,
        )
        return PromptBuildResult(prompt_text=prompt_text, metadata=metadata)

    def _build_summary_payload(self, batch: SummaryBatch) -> tuple[dict[str, Any], dict[str, Any]]:
        source = batch.to_dict()
        text_segments: list[dict[str, Any]] = []
        text_truncated = False
        for item in source["text_segments"][: self.max_summary_text_segments]:
            sanitized, truncated = self._truncate_structure(item)
            text_segments.append(sanitized)
            text_truncated = text_truncated or truncated

        screenshots: list[dict[str, Any]] = []
        screenshots_truncated = False
        for item in source["screenshots"][: self.max_summary_screenshots]:
            sanitized, truncated = self._truncate_structure(item)
            screenshots.append(sanitized)
            screenshots_truncated = screenshots_truncated or truncated

        activity_segments: list[dict[str, Any]] = []
        activity_truncated = False
        for item in source.get("activity_segments", []):
            sanitized, truncated = self._truncate_structure(item)
            activity_segments.append(sanitized)
            activity_truncated = activity_truncated or truncated

        active_intervals: list[dict[str, Any]] = []
        active_truncated = False
        for item in source["active_intervals"]:
            sanitized, truncated = self._truncate_structure(item)
            active_intervals.append(sanitized)
            active_truncated = active_truncated or truncated

        blocked_intervals: list[dict[str, Any]] = []
        blocked_truncated = False
        for item in source["blocked_intervals"]:
            sanitized, truncated = self._truncate_structure(item)
            blocked_intervals.append(sanitized)
            blocked_truncated = blocked_truncated or truncated

        payload = {
            "schema": "worklog.lmstudio.summary_batch.v1",
            "batch": {
                "start_ts": source["start_ts"],
                "end_ts": source["end_ts"],
                "activity_segments": activity_segments,
                "active_intervals": active_intervals,
                "blocked_intervals": blocked_intervals,
                "text_segments": text_segments,
                "screenshots": screenshots,
            },
        }
        metadata = self._payload_metadata(
            response_kind="summary",
            original_counts={
                "active_intervals": len(source["active_intervals"]),
                "blocked_intervals": len(source["blocked_intervals"]),
                "text_segments": len(source["text_segments"]),
                "screenshots": len(source["screenshots"]),
                "activity_segments": len(source.get("activity_segments", [])),
            },
            included_counts={
                "active_intervals": len(active_intervals),
                "blocked_intervals": len(blocked_intervals),
                "text_segments": len(text_segments),
                "screenshots": len(screenshots),
                "activity_segments": len(activity_segments),
            },
            structure_truncated=(
                text_truncated
                or screenshots_truncated
                or active_truncated
                or blocked_truncated
                or activity_truncated
            ),
        )
        return payload, metadata

    def _build_daily_recap_payload(self, day: date, summaries: list[SummaryRecord]) -> tuple[dict[str, Any], dict[str, Any]]:
        included = summaries[: self.max_daily_summaries]
        recap_items: list[dict[str, Any]] = []
        recap_truncated = False
        for item in included:
            sanitized, truncated = self._truncate_structure(
                {
                    "time_range": {"start_ts": item.start_ts, "end_ts": item.end_ts},
                    "summary_text": item.summary_text,
                    "summary_json": item.summary_json,
                }
            )
            recap_items.append(sanitized)
            recap_truncated = recap_truncated or truncated

        payload = {
            "schema": "worklog.lmstudio.daily_recap.v1",
            "day": day.isoformat(),
            "summaries": recap_items,
        }
        metadata = self._payload_metadata(
            response_kind="daily_recap",
            original_counts={"summaries": len(summaries)},
            included_counts={"summaries": len(included)},
            structure_truncated=recap_truncated,
        )
        return payload, metadata

    def _render_prompt(self, *, title: str, instructions: str, payload: dict[str, Any], metadata: dict[str, Any]) -> str:
        return (
            f"{title}\n"
            f"{instructions}\n\n"
            "Prompt metadata:\n"
            f"{json.dumps(metadata, ensure_ascii=False, indent=2)}\n\n"
            "Payload:\n"
            f"{json.dumps(payload, ensure_ascii=False, indent=2)}"
        )

    def _payload_metadata(
        self,
        *,
        response_kind: str,
        original_counts: dict[str, int],
        included_counts: dict[str, int],
        structure_truncated: bool = False,
    ) -> dict[str, Any]:
        truncated = structure_truncated or any(
            included_counts.get(key, 0) != original_counts.get(key, 0) for key in original_counts
        )
        return {
            "schema": "worklog.lmstudio.prompt.v1",
            "response_kind": response_kind,
            "truncated": truncated,
            "max_summary_text_segments": self.max_summary_text_segments,
            "max_summary_screenshots": self.max_summary_screenshots,
            "max_daily_summaries": self.max_daily_summaries,
            "max_text_chars": self.max_text_chars,
            "max_prompt_chars": self.max_prompt_chars,
            "original_counts": original_counts,
            "included_counts": included_counts,
        }

    def _truncate_structure(self, value: Any) -> tuple[Any, bool]:
        if isinstance(value, str):
            if len(value) <= self.max_text_chars:
                return value, False
            return value[: self.max_text_chars] + "...", True
        if isinstance(value, list):
            truncated_any = False
            items: list[Any] = []
            for item in value:
                sanitized, truncated = self._truncate_structure(item)
                items.append(sanitized)
                truncated_any = truncated_any or truncated
            return items, truncated_any
        if isinstance(value, dict):
            truncated_any = False
            items: dict[str, Any] = {}
            for key, item in value.items():
                sanitized, truncated = self._truncate_structure(item)
                items[key] = sanitized
                truncated_any = truncated_any or truncated
            return items, truncated_any
        return value, False
