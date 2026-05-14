from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

from .batching import SummaryBatch
from .models import SummaryRecord
from .internal_artifacts import is_internal_artifact_path

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

    def update_limits(self, *, max_prompt_chars: int, max_summary_text_segments: int) -> None:
        self.max_prompt_chars = max(2000, int(max_prompt_chars))
        self.max_summary_text_segments = max(1, int(max_summary_text_segments))
        self.max_text_chars = self.max_summary_text_segments * TEXT_CHARS_PER_SUMMARY_SEGMENT

    def build_summary_prompt(self, batch: SummaryBatch) -> PromptBuildResult:
        payload, metadata = self._build_summary_payload(batch)
        prompt_text = self._render_prompt(
            title="Summarize the following WorkLog Diary activity batch.",
            instructions=(
                "Return only strict JSON with top-level keys summary_text, task_candidates, files_and_documents, "
                "conversations_or_references, programs_used, outcomes, follow_ups, jira_update_candidates, "
                "unknowns_and_privacy_limits, blocked_observed_references, primary_activity, files, conversations, "
                "blocked_activity, unknowns, evidence_quality, metadata. "
                "Prioritize sections in this order: task_candidates, files_and_documents, conversations_or_references, "
                "programs_used, outcomes, follow_ups, jira_update_candidates, unknowns_and_privacy_limits. "
                "Do not lead with generic program prose unless no better evidence exists. "
                "Use activity_entities and other observed evidence as the source of truth. Observed facts must stay separate from inference. "
                "Do not invent file modifications or task names. Preserve exact file paths and names. "
                "For blocked apps, distinguish blocked content (screenshot/text unavailable) from observable metadata "
                "(process_name, window_title, timestamps). Do not infer hidden blocked content. "
                "Treat blocked intervals as blocked or unknown content, not visible content. "
                "Use window-title metadata conservatively: preserve PDF/file/document titles as read_or_viewed references; "
                "preserve Outlook/Webex/Teams subjects or titles as references with unknown content. "
                "Never claim blocked items were modified, sent, or discussed unless explicit evidence supports it. "
                "Use explicit confidence values in evidence_quality. Avoid generic filler unless evidence is genuinely weak. "
                "Write concise work-focused statements, not narration about the system or user. "
                "Do not use phrases like 'the user', 'interacting with', or 'activity log covers'. "
                "Never include raw Unix timestamps in generated prose; if time context matters, use plain-language time references instead."
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
                "Return only strict JSON with top-level keys executive_summary, workstreams_or_task_candidates, "
                "files_and_documents, conversations_meetings_and_references, program_activity_breakdown, outcomes, "
                "follow_ups_or_jira_candidates, evidence_limits_and_unknowns, tasks_advanced, files_observed, "
                "files_likely_modified, conversations_or_meetings, decisions, blockers, follow_ups, "
                "jira_update_candidates, open_questions, confidence_notes, metadata. "
                "Daily recap sections must prioritize this order: Workstreams/task candidates, Files/documents, "
                "Conversations/meetings/references, Program activity breakdown, Outcomes, Follow-ups/JIRA candidates, "
                "Evidence limits and unknowns. "
                "Base the recap on the deterministic daily evidence aggregate and summary index. "
                "Do not invent file modifications or task names. Do not hallucinate blocked content. "
                "Include blocked-app metadata-derived references with caveats that content was not captured. "
                "Keep the recap concise and fact-oriented. Use confidence_notes to explain low-confidence or ambiguous evidence. "
                "Preserve exact file, task, and program evidence when present, but do not repeat raw event summaries unnecessarily. "
                "Write concise work-focused statements, not narration about the system or user. "
                "Do not use phrases like 'the user', 'interacting with', or 'activity log covers'. "
                "Never include raw Unix timestamps in generated prose; if time context matters, use plain-language time references instead."
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

        activity_entities: list[dict[str, Any]] = []
        activity_entities_truncated = False
        for item in source.get("activity_entities", []):
            sanitized, truncated = self._truncate_structure(item)
            activity_entities.append(sanitized)
            activity_entities_truncated = activity_entities_truncated or truncated

        parser_coverage: list[dict[str, Any]] = []
        parser_coverage_truncated = False
        for item in source.get("parser_coverage", []):
            sanitized, truncated = self._truncate_structure(item)
            parser_coverage.append(sanitized)
            parser_coverage_truncated = parser_coverage_truncated or truncated

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
                "activity_entities": activity_entities,
                "parser_coverage": parser_coverage,
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
                "activity_entities": len(source.get("activity_entities", [])),
                "parser_coverage": len(source.get("parser_coverage", [])),
            },
            included_counts={
                "active_intervals": len(active_intervals),
                "blocked_intervals": len(blocked_intervals),
                "text_segments": len(text_segments),
                "screenshots": len(screenshots),
                "activity_segments": len(activity_segments),
                "activity_entities": len(activity_entities),
                "parser_coverage": len(parser_coverage),
            },
            structure_truncated=(
                text_truncated
                or screenshots_truncated
                or active_truncated
                or blocked_truncated
                or activity_truncated
                or activity_entities_truncated
                or parser_coverage_truncated
            ),
        )
        return payload, metadata

    def _build_daily_recap_payload(self, day: date, summaries: list[SummaryRecord]) -> tuple[dict[str, Any], dict[str, Any]]:
        included = summaries[: self.max_daily_summaries]
        recap_truncated = False
        confidence_notes: list[str] = []
        summary_index: list[dict[str, Any]] = []
        aggregates = {
            "programs": {},
            "files_and_documents": {},
            "folders_or_workstreams": {},
            "task_candidates": {},
            "canonical_window_titles": {},
            "evidence_caveats": {},
        }
        for item in included:
            structured = item.summary_json if isinstance(item.summary_json, dict) else {}
            evidence_quality = structured.get("evidence_quality") if isinstance(structured.get("evidence_quality"), dict) else {}
            activity_entities = structured.get("activity_entities") if isinstance(structured.get("activity_entities"), list) else []
            summary_text = _summary_text_excerpt(structured.get("summary_text") or item.summary_text)
            summary_index.append(
                _prune_empty_structure(
                    {
                        "summary_id": item.id,
                        "time_range": {"start_ts": item.start_ts, "end_ts": item.end_ts},
                        "summary_text": summary_text,
                        "programs": _collect_summary_labels(structured, activity_entities, "programs"),
                        "files": _collect_summary_labels(structured, activity_entities, "files"),
                        "tasks": _collect_summary_labels(structured, activity_entities, "tasks"),
                        "window_title": _canonical_window_title(structured),
                        "blocked": bool(_collect_summary_labels(structured, activity_entities, "caveats")),
                    }
                )
            )
            _aggregate_bucket(
                aggregates["programs"],
                _collect_summary_labels(structured, activity_entities, "programs"),
                item,
            )
            _aggregate_bucket(
                aggregates["files_and_documents"],
                _collect_summary_labels(structured, activity_entities, "files"),
                item,
            )
            _aggregate_bucket(
                aggregates["folders_or_workstreams"],
                _collect_summary_labels(structured, activity_entities, "folders"),
                item,
            )
            _aggregate_bucket(
                aggregates["task_candidates"],
                _collect_summary_labels(structured, activity_entities, "tasks"),
                item,
            )
            window_title = _canonical_window_title(structured)
            if window_title:
                _aggregate_bucket(aggregates["canonical_window_titles"], [window_title], item)
            _aggregate_bucket(
                aggregates["evidence_caveats"],
                _collect_summary_labels(structured, activity_entities, "caveats"),
                item,
            )
            if isinstance(evidence_quality.get("confidence_notes"), list):
                confidence_notes.extend(str(note) for note in evidence_quality["confidence_notes"] if str(note).strip())

        aggregate_programs = _finalize_aggregate_entries(aggregates["programs"])
        aggregate_files = _finalize_aggregate_entries(aggregates["files_and_documents"])
        aggregate_folders = _finalize_aggregate_entries(aggregates["folders_or_workstreams"])
        aggregate_tasks = _finalize_aggregate_entries(aggregates["task_candidates"])
        aggregate_windows = _finalize_aggregate_entries(aggregates["canonical_window_titles"])
        aggregate_caveats = _finalize_aggregate_entries(aggregates["evidence_caveats"])
        aggregate_payload = _prune_empty_structure(
            {
                "time_range": {
                    "start_ts": min((item.start_ts for item in included), default=0.0),
                    "end_ts": max((item.end_ts for item in included), default=0.0),
                },
                "summary_count": len(included),
                "programs": aggregate_programs,
                "files_and_documents": aggregate_files,
                "folders_or_workstreams": aggregate_folders,
                "task_candidates": aggregate_tasks,
                "canonical_window_titles": aggregate_windows,
                "evidence_caveats": aggregate_caveats,
            }
        )
        sanitized_summary_index, truncated = self._truncate_structure(summary_index)
        recap_truncated = recap_truncated or truncated
        sanitized_aggregate, truncated = self._truncate_structure(aggregate_payload)
        recap_truncated = recap_truncated or truncated
        payload = {
            "schema": "worklog.lmstudio.daily_recap.v2",
            "day": day.isoformat(),
            "daily_evidence_aggregate": sanitized_aggregate,
            "summary_index": sanitized_summary_index,
            "confidence_notes": _dedupe_preserve_order(confidence_notes)[:20],
        }
        metadata = self._payload_metadata(
            response_kind="daily_recap",
            original_counts={"summaries": len(summaries)},
            included_counts={
                "summaries": len(included),
                "summary_index": len(summary_index),
                "aggregate_programs": len(aggregate_programs),
                "aggregate_files": len(aggregate_files),
                "aggregate_tasks": len(aggregate_tasks),
            },
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


def _flatten_prompt_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned and is_internal_artifact_path(cleaned):
            return []
        return [cleaned] if cleaned else []
    if isinstance(value, list):
        flattened: list[str] = []
        for item in value:
            flattened.extend(_flatten_prompt_values(item))
        return [item for item in flattened if item]
    if isinstance(value, dict):
        for key in ("text", "path/name", "path", "name", "reference", "title"):
            text = value.get(key)
            if isinstance(text, str) and text.strip():
                return [text.strip()]
        return []
    cleaned = str(value).strip()
    return [cleaned] if cleaned else []


def _summary_text_excerpt(value: Any, limit: int = 80) -> str:
    text = " ".join(_flatten_prompt_values(value))
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _canonical_window_title(structured: dict[str, Any]) -> str:
    source_context = structured.get("source_context")
    if not isinstance(source_context, dict):
        return ""
    title = str(source_context.get("window_title") or "").strip()
    if not title:
        return ""
    for suffix in (" - Google Chrome", " - Chromium", " - Microsoft Edge", " - Edge"):
        if title.endswith(suffix):
            title = title[: -len(suffix)].strip()
            break
    return title


def _collect_entity_values(activity_entities: list[dict[str, Any]], entity_types: set[str]) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for item in activity_entities:
        if not isinstance(item, dict):
            continue
        entity_type = str(item.get("entity_type") or "").strip().lower()
        if entity_type not in entity_types:
            continue
        value = str(item.get("entity_value") or "").strip()
        if not value:
            continue
        if entity_type in {"file_path", "file_name", "folder_path", "project_candidate"} and is_internal_artifact_path(value):
            continue
        normalized = str(item.get("entity_normalized") or value).strip().lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        values.append(value)
    return values


def _collect_summary_labels(structured: dict[str, Any], activity_entities: list[dict[str, Any]], kind: str) -> list[str]:
    if kind == "programs":
        return _dedupe_preserve_order(
            _flatten_prompt_values(structured.get("programs_used"))
            + _collect_entity_values(activity_entities, {"program"})
        )[:8]
    if kind == "files":
        return _dedupe_preserve_order(
            _flatten_prompt_values(structured.get("files_and_documents") or structured.get("files"))
            + _collect_entity_values(activity_entities, {"file_path", "file_name"})
        )[:10]
    if kind == "folders":
        return _dedupe_preserve_order(
            _flatten_prompt_values(structured.get("task_candidates"))
            + _collect_entity_values(activity_entities, {"folder_path", "project_candidate", "task_candidate"})
        )[:10]
    if kind == "tasks":
        return _dedupe_preserve_order(
            _flatten_prompt_values(structured.get("task_candidates") or structured.get("jira_update_candidates"))
            + _collect_entity_values(activity_entities, {"task_candidate"})
        )[:10]
    if kind == "caveats":
        return _dedupe_preserve_order(
            _flatten_prompt_values(structured.get("unknowns_and_privacy_limits") or structured.get("unknowns"))
            + _flatten_prompt_values(structured.get("blocked_activity"))
            + _flatten_prompt_values(structured.get("blocked_observed_references"))
        )[:10]
    return []


def _aggregate_bucket(bucket: dict[str, dict[str, Any]], values: list[str], summary: SummaryRecord) -> None:
    for value in values:
        label = value.strip()
        if not label:
            continue
        key = label.lower()
        entry = bucket.setdefault(
            key,
            {
                "label": label,
                "count": 0,
                "summary_ids": [],
                "start_ts": summary.start_ts,
                "end_ts": summary.end_ts,
            },
        )
        entry["count"] = int(entry["count"]) + 1
        if summary.id not in entry["summary_ids"]:
            entry["summary_ids"].append(summary.id)
        entry["start_ts"] = min(float(entry["start_ts"]), summary.start_ts)
        entry["end_ts"] = max(float(entry["end_ts"]), summary.end_ts)


def _finalize_aggregate_entries(bucket: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        {
            "label": str(entry["label"]),
            "count": int(entry["count"]),
            "time_range": {"start_ts": float(entry["start_ts"]), "end_ts": float(entry["end_ts"])},
            "summary_ids": list(entry["summary_ids"]),
        }
        for entry in bucket.values()
    ]
    rows.sort(key=lambda row: (-int(row["count"]), str(row["label"]).lower(), float(row["time_range"]["start_ts"])))
    return rows


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = value.strip()
        if not cleaned:
            continue
        normalized = cleaned.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(cleaned)
    return result


def _prune_empty_structure(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, item in value.items():
            pruned = _prune_empty_structure(item)
            if pruned in (None, "", [], {}):
                continue
            cleaned[key] = pruned
        return cleaned
    if isinstance(value, list):
        cleaned_list = [_prune_empty_structure(item) for item in value]
        return [item for item in cleaned_list if item not in (None, "", [], {})]
    return value
