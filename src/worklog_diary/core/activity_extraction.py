from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import PureWindowsPath
from typing import Any, Iterable


_WINDOWS_PATH_RE = re.compile(
    r"""
    (?<!\w)
    (?:
        [A-Za-z]:[\\/](?:[^\\/:*?"<>|\r\n]+[\\/])*[^\\/:*?"<>|\r\n]+
        |
        (?:\\\\|//)(?:[^\\/:*?"<>|\r\n]+[\\/])+[^\\/:*?"<>|\r\n]+
    )
    """,
    re.VERBOSE,
)
_TICKET_RE = re.compile(r"\b[A-Z][A-Z0-9]+-\d+\b", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")
_TRAILING_DIRTY_MARKER_RE = re.compile(r"\s*\*\s*$")

_GENERIC_PROJECT_SEGMENTS = {
    "appdata",
    "desktop",
    "documents",
    "downloads",
    "home",
    "local",
    "program files",
    "program files (x86)",
    "projects",
    "source",
    "src",
    "temp",
    "tmp",
    "users",
    "workspace",
}

_SOURCE_KIND_PRECEDENCE = {
    "window_title": 0,
    "text_segment": 1,
    "process_name": 2,
    "llm_inference": 3,
}

_EVIDENCE_KIND_PRECEDENCE = {
    "observed": 0,
    "likely": 1,
    "inferred": 2,
    "unknown": 3,
}


@dataclass(slots=True)
class ActivityEntityDraft:
    entity_type: str
    entity_value: str
    entity_normalized: str
    source_kind: str
    source_ref: str
    evidence_kind: str
    confidence: float
    attributes: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class ActivityParserCoverageDraft:
    process_name: str
    normalized_process_name: str
    window_title: str
    normalized_window_title: str
    matched_parser_names: list[str]
    used_generic_parser: bool
    used_specialized_parser: bool
    extracted_entity_count: int
    unclassified_evidence_count: int
    parser_confidence: float
    unknown_app: bool
    start_ts: float
    end_ts: float


def extract_activity_entities(
    *,
    start_ts: float,
    end_ts: float,
    process_name: str,
    window_title: str,
    text_segments: Iterable[Any] | None = None,
    screenshot_refs: Iterable[Any] | None = None,
) -> list[ActivityEntityDraft]:
    entities, _coverage = extract_activity_entities_with_coverage(
        start_ts=start_ts,
        end_ts=end_ts,
        process_name=process_name,
        window_title=window_title,
        text_segments=text_segments,
        screenshot_refs=screenshot_refs,
    )
    return entities


def extract_activity_entities_with_coverage(
    *,
    start_ts: float,
    end_ts: float,
    process_name: str,
    window_title: str,
    text_segments: Iterable[Any] | None = None,
    screenshot_refs: Iterable[Any] | None = None,
) -> tuple[list[ActivityEntityDraft], dict[str, object]]:
    drafts: dict[tuple[str, str], ActivityEntityDraft] = {}
    coverage = {
        "process_name": process_name.strip(),
        "normalized_process_name": _normalize_text(process_name),
        "window_title": window_title.strip(),
        "normalized_window_title": _normalize_text(window_title),
        "matched_parser_names": set(),
        "used_generic_parser": False,
        "used_specialized_parser": False,
        "extracted_entity_count": 0,
        "unclassified_evidence_count": 0,
        "parser_confidence": 0.0,
        "unknown_app": False,
        "start_ts": start_ts,
        "end_ts": end_ts,
    }

    process_name = process_name.strip()
    window_title = window_title.strip()
    text_sources = list(text_segments or [])
    screenshot_list = [str(item).strip() for item in (screenshot_refs or []) if str(item).strip()]
    has_dirty_marker = bool(window_title) and bool(_TRAILING_DIRTY_MARKER_RE.search(window_title))

    if process_name:
        _merge_draft(
            drafts,
            ActivityEntityDraft(
                entity_type="program",
                entity_value=process_name,
                entity_normalized=_normalize_text(process_name),
                source_kind="process_name",
                source_ref=process_name,
                evidence_kind="observed",
                confidence=1.0,
                attributes={"start_ts": start_ts, "end_ts": end_ts},
            ),
        )

    if window_title:
        title_attributes: dict[str, object] = {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "observed": True,
        }
        if screenshot_list:
            title_attributes["screenshot_refs"] = screenshot_list
        if has_dirty_marker:
            title_attributes["dirty_marker"] = True
            title_attributes["likely_edited"] = True
        _merge_draft(
            drafts,
            ActivityEntityDraft(
                entity_type="window_title",
                entity_value=window_title,
                entity_normalized=_normalize_text(window_title),
                source_kind="window_title",
                source_ref=window_title,
                evidence_kind="observed",
                confidence=1.0,
                attributes=title_attributes,
            ),
        )

        specialized_parser_names = _extract_title_entities(
            drafts,
            window_title=window_title,
            process_name=process_name,
            source_ref=window_title,
            start_ts=start_ts,
            end_ts=end_ts,
            coverage=coverage,
        )
        if has_dirty_marker:
            for item in drafts.values():
                if item.source_kind == "window_title" and item.entity_type in {"file_path", "file_name", "folder_path"}:
                    item.attributes["dirty_marker"] = True
                    item.attributes["likely_edited"] = True
        coverage["used_specialized_parser"] = bool(specialized_parser_names)
        if not specialized_parser_names:
            coverage["unknown_app"] = True
            coverage["unclassified_evidence_count"] = 1
            _merge_draft(
                drafts,
                ActivityEntityDraft(
                    entity_type="unclassified_window_title",
                    entity_value=window_title,
                    entity_normalized=_normalize_text(window_title),
                    source_kind="window_title",
                    source_ref=window_title,
                    evidence_kind="observed",
                    confidence=1.0,
                    attributes={
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                        "unknown_app": True,
                        "matched_parser_names": [],
                    },
                ),
            )

    for index, item in enumerate(text_sources):
        text = _coerce_text(item)
        if not text:
            continue
        _extract_text_entities(
            drafts,
            text=text,
            source_kind="text_segment",
            source_ref=f"text_segment[{index}]",
            start_ts=start_ts,
            end_ts=end_ts,
            coverage=coverage,
        )

    coverage["matched_parser_names"] = sorted(coverage["matched_parser_names"])
    coverage["used_generic_parser"] = True
    coverage["extracted_entity_count"] = len(drafts)
    coverage["parser_confidence"] = _estimate_parser_confidence(
        used_specialized_parser=bool(coverage["used_specialized_parser"]),
        extracted_entity_count=len(drafts),
        unclassified_evidence_count=int(coverage["unclassified_evidence_count"]),
    )

    return sorted(
        drafts.values(),
        key=lambda item: (item.entity_type, item.source_kind, item.entity_normalized, item.source_ref),
    ), coverage


def _extract_title_entities(
    drafts: dict[tuple[str, str], ActivityEntityDraft],
    *,
    window_title: str,
    process_name: str,
    source_ref: str,
    start_ts: float,
    end_ts: float,
    coverage: dict[str, object],
) -> set[str]:
    lower_process = process_name.lower()
    lower_title = window_title.lower()
    specialized_parser_names: set[str] = set()

    for path, path_kind in _iter_windows_paths(window_title):
        _add_path_entities(
            drafts,
            path=path,
            source_kind="window_title",
            source_ref=source_ref,
            start_ts=start_ts,
            end_ts=end_ts,
            dirty_marker=bool(_TRAILING_DIRTY_MARKER_RE.search(window_title)),
            path_kind=path_kind,
        )

    generic_parser_names = _extract_generic_entities(
        drafts,
        text=window_title,
        source_kind="window_title",
        source_ref=source_ref,
        start_ts=start_ts,
        end_ts=end_ts,
        dirty_marker=bool(_TRAILING_DIRTY_MARKER_RE.search(window_title)),
    )
    coverage["matched_parser_names"].update(generic_parser_names)

    if _is_browser_title(lower_process, lower_title):
        page_title = _strip_browser_suffix(window_title)
        if page_title and page_title != window_title:
            _merge_draft(
                drafts,
                ActivityEntityDraft(
                    entity_type="web_page_title",
                    entity_value=page_title,
                    entity_normalized=_normalize_text(page_title),
                    source_kind="window_title",
                    source_ref=source_ref,
                    evidence_kind="observed",
                    confidence=0.93,
                    attributes={"start_ts": start_ts, "end_ts": end_ts, "browser_process": process_name},
            ),
        )
            specialized_parser_names.add("browser_title")
            coverage["matched_parser_names"].add("browser_title")

    if _is_outlook_title(lower_process, lower_title):
        subject = _strip_mail_suffix(window_title)
        if subject:
            _merge_draft(
                drafts,
                ActivityEntityDraft(
                    entity_type="mail_subject",
                    entity_value=subject,
                    entity_normalized=_normalize_text(subject),
                    source_kind="window_title",
                    source_ref=source_ref,
                    evidence_kind="observed",
                    confidence=0.95,
                    attributes={"start_ts": start_ts, "end_ts": end_ts, "mail_process": process_name},
            ),
        )
            specialized_parser_names.add("mail_subject")
            coverage["matched_parser_names"].add("mail_subject")

    if _is_conversation_title(lower_process, lower_title):
        subject = _strip_conversation_suffix(window_title)
        if subject:
            _merge_draft(
                drafts,
                ActivityEntityDraft(
                    entity_type="conversation_subject",
                    entity_value=subject,
                    entity_normalized=_normalize_text(subject),
                    source_kind="window_title",
                    source_ref=source_ref,
                    evidence_kind="observed",
                    confidence=0.9,
                    attributes={"start_ts": start_ts, "end_ts": end_ts, "conversation_process": process_name},
            ),
        )
            specialized_parser_names.add("conversation_subject")
            coverage["matched_parser_names"].add("conversation_subject")

    for ticket in _extract_tickets(window_title):
        _merge_draft(
            drafts,
            ActivityEntityDraft(
                entity_type="task_candidate",
                entity_value=ticket,
                entity_normalized=ticket.upper(),
                source_kind="window_title",
                source_ref=source_ref,
                evidence_kind="observed",
                confidence=0.95,
                attributes={"start_ts": start_ts, "end_ts": end_ts},
            ),
        )
        coverage["matched_parser_names"].add("ticket_label")

    return specialized_parser_names


def _extract_text_entities(
    drafts: dict[tuple[str, str], ActivityEntityDraft],
    *,
    text: str,
    source_kind: str,
    source_ref: str,
    start_ts: float,
    end_ts: float,
    coverage: dict[str, object],
) -> None:
    generic_parser_names = _extract_generic_entities(
        drafts,
        text=text,
        source_kind=source_kind,
        source_ref=source_ref,
        start_ts=start_ts,
        end_ts=end_ts,
        dirty_marker=False,
    )
    coverage["matched_parser_names"].update(generic_parser_names)
    coverage["used_generic_parser"] = True


def _extract_generic_entities(
    drafts: dict[tuple[str, str], ActivityEntityDraft],
    *,
    text: str,
    source_kind: str,
    source_ref: str,
    start_ts: float,
    end_ts: float,
    dirty_marker: bool,
) -> set[str]:
    matched_parser_names: set[str] = set()
    for path, path_kind in _iter_windows_paths(text):
        _add_path_entities(
            drafts,
            path=path,
            source_kind=source_kind,
            source_ref=source_ref,
            start_ts=start_ts,
            end_ts=end_ts,
            dirty_marker=False,
            path_kind=path_kind,
        )
        matched_parser_names.add("windows_path")

    for token in _extract_file_like_tokens(text):
        _merge_draft(
            drafts,
            ActivityEntityDraft(
                entity_type="file_name",
                entity_value=token,
                entity_normalized=token.lower(),
                source_kind=source_kind,
                source_ref=source_ref,
                evidence_kind="observed",
                confidence=0.88,
                attributes={"start_ts": start_ts, "end_ts": end_ts, "dirty_marker": dirty_marker},
            ),
        )
        matched_parser_names.add("file_token")

    for ticket in _extract_tickets(text):
        _merge_draft(
            drafts,
            ActivityEntityDraft(
                entity_type="task_candidate",
                entity_value=ticket,
                entity_normalized=ticket.upper(),
                source_kind=source_kind,
                source_ref=source_ref,
                evidence_kind="observed",
                confidence=0.9,
                attributes={"start_ts": start_ts, "end_ts": end_ts},
            ),
        )
        matched_parser_names.add("ticket_label")

    for repo in _extract_repository_candidates(text):
        _merge_draft(
            drafts,
            ActivityEntityDraft(
                entity_type="repository",
                entity_value=repo,
                entity_normalized=repo.lower(),
                source_kind=source_kind,
                source_ref=source_ref,
                evidence_kind="inferred",
                confidence=0.7,
                attributes={"start_ts": start_ts, "end_ts": end_ts},
            ),
        )
        matched_parser_names.add("repository_hint")

    for branch in _extract_git_branch_candidates(text):
        _merge_draft(
            drafts,
            ActivityEntityDraft(
                entity_type="git_branch",
                entity_value=branch,
                entity_normalized=branch.lower(),
                source_kind=source_kind,
                source_ref=source_ref,
                evidence_kind="inferred",
                confidence=0.68,
                attributes={"start_ts": start_ts, "end_ts": end_ts},
            ),
        )
        matched_parser_names.add("git_branch_hint")

    for task_candidate in _extract_task_candidates(text):
        _merge_draft(
            drafts,
            ActivityEntityDraft(
                entity_type="task_candidate",
                entity_value=task_candidate,
                entity_normalized=_normalize_text(task_candidate),
                source_kind=source_kind,
                source_ref=source_ref,
                evidence_kind="observed",
                confidence=0.82,
                attributes={"start_ts": start_ts, "end_ts": end_ts, "explicit_label": True},
            ),
        )
        matched_parser_names.add("explicit_task_label")

    return matched_parser_names


def _add_path_entities(
    drafts: dict[tuple[str, str], ActivityEntityDraft],
    *,
    path: str,
    source_kind: str,
    source_ref: str,
    start_ts: float,
    end_ts: float,
    dirty_marker: bool,
    path_kind: str,
) -> None:
    normalized_path = _normalize_path(path)
    if not normalized_path:
        return

    path = _trim_path(path)
    if not path:
        return

    path_obj = PureWindowsPath(path)
    is_file = path_kind == "file"
    if is_file:
        _merge_draft(
            drafts,
            ActivityEntityDraft(
                entity_type="file_path",
                entity_value=path,
                entity_normalized=normalized_path,
                source_kind=source_kind,
                source_ref=source_ref,
                evidence_kind="observed",
                confidence=0.98,
                attributes={"start_ts": start_ts, "end_ts": end_ts, "dirty_marker": dirty_marker, "likely_edited": dirty_marker},
            ),
        )
        filename = path_obj.name
        if filename:
            _merge_draft(
                drafts,
                ActivityEntityDraft(
                    entity_type="file_name",
                    entity_value=filename,
                    entity_normalized=filename.lower(),
                    source_kind=source_kind,
                    source_ref=source_ref,
                    evidence_kind="observed",
                    confidence=0.97,
                    attributes={"start_ts": start_ts, "end_ts": end_ts, "dirty_marker": dirty_marker},
                ),
            )
        folder = str(path_obj.parent)
        if folder and folder not in {path, "."}:
            _merge_draft(
                drafts,
                ActivityEntityDraft(
                    entity_type="folder_path",
                    entity_value=folder,
                    entity_normalized=_normalize_path(folder),
                    source_kind=source_kind,
                    source_ref=source_ref,
                    evidence_kind="observed",
                    confidence=0.9,
                    attributes={"start_ts": start_ts, "end_ts": end_ts, "dirty_marker": dirty_marker},
                ),
            )
    else:
        _merge_draft(
            drafts,
            ActivityEntityDraft(
                entity_type="folder_path",
                entity_value=path,
                entity_normalized=normalized_path,
                source_kind=source_kind,
                source_ref=source_ref,
                evidence_kind="observed",
                confidence=0.9,
                attributes={"start_ts": start_ts, "end_ts": end_ts, "dirty_marker": dirty_marker},
            ),
        )

    project_candidate = _project_candidate_from_path(path_obj)
    if project_candidate is not None:
        _merge_draft(
            drafts,
            ActivityEntityDraft(
                entity_type="project_candidate",
                entity_value=project_candidate,
                entity_normalized=_normalize_text(project_candidate),
                source_kind=source_kind,
                source_ref=source_ref,
                evidence_kind="likely",
                confidence=0.72,
                attributes={"start_ts": start_ts, "end_ts": end_ts, "derived_from_path": True},
            ),
        )


def _merge_draft(
    drafts: dict[tuple[str, str], ActivityEntityDraft],
    draft: ActivityEntityDraft,
) -> None:
    key = (draft.entity_type, draft.entity_normalized)
    existing = drafts.get(key)
    if existing is None:
        drafts[key] = draft
        return

    if _SOURCE_KIND_PRECEDENCE.get(draft.source_kind, 99) < _SOURCE_KIND_PRECEDENCE.get(existing.source_kind, 99):
        existing.source_kind = draft.source_kind
        existing.source_ref = draft.source_ref
    elif (
        _SOURCE_KIND_PRECEDENCE.get(draft.source_kind, 99) == _SOURCE_KIND_PRECEDENCE.get(existing.source_kind, 99)
        and draft.confidence > existing.confidence
    ):
        existing.source_ref = draft.source_ref

    if _EVIDENCE_KIND_PRECEDENCE.get(draft.evidence_kind, 99) < _EVIDENCE_KIND_PRECEDENCE.get(existing.evidence_kind, 99):
        existing.evidence_kind = draft.evidence_kind
    existing.confidence = max(existing.confidence, draft.confidence)
    existing.attributes.update(draft.attributes)


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        text = value.get("text")
        return str(text).strip() if text is not None else ""
    return str(value).strip()


def _extract_tickets(text: str) -> list[str]:
    tickets: list[str] = []
    seen: set[str] = set()
    for match in _TICKET_RE.finditer(text):
        ticket = match.group(0).upper()
        if ticket in seen:
            continue
        seen.add(ticket)
        tickets.append(ticket)
    return tickets


def _extract_repository_candidates(text: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"\b([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)\b", text):
        value = match.group(1)
        if "." in value.split("/", 1)[0]:
            continue
        normalized = value.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(value)
    return candidates


def _extract_git_branch_candidates(text: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    patterns = (
        r"\bbranch[:\s]+([A-Za-z0-9._/\-]+)\b",
        r"\bon\s+([A-Za-z0-9._/\-]+)\s+branch\b",
        r"\brefs/heads/([A-Za-z0-9._/\-]+)\b",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            value = match.group(1)
            normalized = value.lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            candidates.append(value)
    return candidates


def _iter_windows_paths(text: str) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    seen: set[str] = set()
    for match in _WINDOWS_PATH_RE.finditer(text):
        raw = _trim_path(match.group(0))
        normalized = _normalize_path(raw)
        if not raw or not normalized or normalized in seen:
            continue
        if not _looks_like_windows_path(raw):
            continue
        seen.add(normalized)
        candidates.append((raw, _classify_path(raw)))
    return candidates


def _extract_file_like_tokens(text: str) -> list[str]:
    tokens: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"(?<![\w/\\])([A-Za-z0-9][A-Za-z0-9._-]{0,127}\.[A-Za-z0-9]{1,10})(?![\w/\\])", text):
        value = _trim_path(match.group(1))
        if not value or value.endswith("."):
            continue
        if _looks_like_windows_path(value):
            continue
        normalized = value.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        tokens.append(value)
    return tokens


def _extract_task_candidates(text: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    patterns = (
        r"\b(?:task|case|job|run|build|ticket|issue|story)[:\s#-]+([A-Za-z0-9._/-]{2,})\b",
        r"\b(?:analysis for|work on|follow up on)\s+([A-Z0-9][A-Z0-9._-]{1,})\b",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            value = match.group(1).strip().strip("-_ ")
            if not value:
                continue
            normalized = value.lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            candidates.append(value)
    return candidates


def _looks_like_windows_path(value: str) -> bool:
    if len(value) < 3:
        return False
    if value.startswith(("\\\\", "//")):
        return True
    return len(value) >= 3 and value[1] == ":" and value[2] in {"\\", "/"}


def _classify_path(path: str) -> str:
    path_obj = PureWindowsPath(path)
    if path_obj.suffix:
        return "file"
    return "folder"


def _project_candidate_from_path(path: PureWindowsPath) -> str | None:
    parts = list(path.parts)
    if (path.drive or path.root) and parts:
        parts = parts[1:]
    if not parts:
        return None
    for part in parts[:-1] if path.suffix else parts:
        normalized = part.strip().lower()
        if not normalized or normalized in _GENERIC_PROJECT_SEGMENTS:
            continue
        if _is_noise_segment(normalized):
            continue
        return part.strip()
    first = parts[0].strip()
    return first or None


def _is_noise_segment(value: str) -> bool:
    return value in {"bin", "build", "dist", "obj", "debug", "release", "src", "source"}


def _strip_browser_suffix(title: str) -> str:
    suffixes = (
        " - Google Chrome",
        " - Chromium",
        " - Microsoft Edge",
        " - Edge",
    )
    for suffix in suffixes:
        if title.endswith(suffix):
            return title[: -len(suffix)].strip()
    return title.strip()


def _strip_mail_suffix(title: str) -> str:
    suffixes = (
        " - Outlook",
        " | Outlook",
        " - Microsoft Outlook",
        " - Message",
        " - Message (HTML)",
    )
    stripped = title.strip()
    changed = True
    while changed:
        changed = False
        for suffix in suffixes:
            if stripped.endswith(suffix):
                stripped = stripped[: -len(suffix)].strip()
                changed = True
    return stripped


def _strip_conversation_suffix(title: str) -> str:
    suffixes = (
        " - Microsoft Teams",
        " | Microsoft Teams",
        " - Teams",
        " - Slack",
        " | Slack",
        " - Zoom",
        " | Zoom",
        " - Webex",
        " | Webex",
        " - Google Meet",
        " | Google Meet",
    )
    stripped = title.strip()
    for suffix in suffixes:
        if stripped.endswith(suffix):
            return stripped[: -len(suffix)].strip()
    return stripped


def _is_browser_title(lower_process: str, lower_title: str) -> bool:
    return any(
        token in lower_process or token in lower_title
        for token in ("chrome", "msedge", "edge", "chromium")
    )


def _is_outlook_title(lower_process: str, lower_title: str) -> bool:
    return "outlook" in lower_process or "outlook" in lower_title


def _is_conversation_title(lower_process: str, lower_title: str) -> bool:
    return any(
        token in lower_process or token in lower_title
        for token in ("teams", "slack", "zoom", "webex", "meet")
    )


def _trim_path(value: str) -> str:
    trimmed = value.strip().strip("\"'[](){}<>,;")
    trimmed = _TRAILING_DIRTY_MARKER_RE.sub("", trimmed).strip()
    while trimmed.endswith(("\\", "/")):
        trimmed = trimmed[:-1].strip()
    return trimmed


def _normalize_text(value: str) -> str:
    return _WHITESPACE_RE.sub(" ", value.strip()).lower()


def _normalize_path(value: str) -> str:
    path = _trim_path(value)
    if not path:
        return ""
    try:
        normalized = PureWindowsPath(path).as_posix()
    except Exception:
        normalized = path.replace("\\", "/")
    return normalized.lower()


def _estimate_parser_confidence(
    *,
    used_specialized_parser: bool,
    extracted_entity_count: int,
    unclassified_evidence_count: int,
) -> float:
    if used_specialized_parser:
        return 0.9 if extracted_entity_count >= 3 else 0.84
    if extracted_entity_count > unclassified_evidence_count + 2:
        return 0.62
    if extracted_entity_count > unclassified_evidence_count:
        return 0.48
    return 0.32
