from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import PureWindowsPath
from statistics import mean
from typing import Any

BUCKET_ORDER = ('excellent', 'good', 'weak', 'poor')
ENTITY_FILE_TYPES = {'file_path', 'file_name', 'folder_path'}
CONVERSATION_ENTITY_TYPES = {'conversation_subject', 'mail_subject', 'web_page_title', 'title_subject_candidate'}
TITLE_ONLY_ENTITY_TYPES = {'conversation_subject', 'mail_subject', 'web_page_title', 'title_subject_candidate'}
TICKET_LABEL_RE = re.compile(r'[A-Z]{2,}[A-Z0-9]*-\d+')
WINDOW_SUFFIX_RE = re.compile(
    r'\s*-\s*(?:Microsoft\s+Edge|Google\s+Chrome|Outlook|Message\s+\(HTML\)|'
    r'File\s+Explorer|Notepad\+\+|Word|Webex|Teams|LM\s+Studio|Profile\s+\d+)\s*$',
    re.IGNORECASE,
)
WINDOW_PREFIX_RE = re.compile(r'^\s*(?:re|fw|fwd)\s*:\s*', re.IGNORECASE)


@dataclass(slots=True)
class DailyReadinessRow:
    day: str
    event_summary_count: int
    daily_summary_present: bool
    average_evidence_quality: float
    evidence_bucket_counts: dict[str, int]
    top_programs: list[tuple[str, int]]
    top_files: list[tuple[str, int, str]]
    top_task_candidates: list[tuple[str, int, str]]
    top_conversation_subjects: list[tuple[str, int, str]]
    unknown_evidence_count: int
    unclassified_evidence_count: int
    blocked_or_privacy_heavy_count: int
    classification: str
    diary_useful: bool
    rationale: list[str]


@dataclass(slots=True)
class ProgramBreakdownRow:
    program: str
    summary_count: int
    day_count: int
    first_seen: str | None
    last_seen: str | None
    linked_files: list[tuple[str, int, str]]
    linked_task_candidates: list[tuple[str, int, str]]
    linked_conversations: list[tuple[str, int, str]]
    evidence_bucket_counts: dict[str, int]
    unknown_rows: int
    unclassified_evidence_count: int
    unknown_ratio: float
    role: str
    confidence: float


@dataclass(slots=True)
class FileEvidenceRow:
    entity_type: str
    label: str
    first_seen: str | None
    last_seen: str | None
    days_seen: int
    linked_processes: list[str]
    linked_summaries: list[int]
    status: str
    confidence: float
    supporting_evidence_sources: list[str]


@dataclass(slots=True)
class ConversationEvidenceRow:
    entity_type: str
    title: str
    app_process: str
    first_seen: str | None
    last_seen: str | None
    linked_files: list[str]
    linked_tasks: list[str]
    content_known: bool
    confidence: float
    note: str


@dataclass(slots=True)
class TaskClusterRow:
    task_label: str
    related_programs: list[str]
    related_files: list[str]
    related_conversations: list[str]
    related_days: list[str]
    suggested_diary_interpretation: str
    confidence: float
    evidence_refs: list[str]


@dataclass(slots=True)
class JiraCandidateRow:
    project_or_program: str
    task_label: str
    suggested_update_text: str
    suggested_subtasks: list[str]
    supporting_evidence: list[str]
    confidence: str
    caveats: list[str]


@dataclass(slots=True)
class WorkDiaryReadinessMetrics:
    day_reports: list[DailyReadinessRow]
    program_reports: list[ProgramBreakdownRow]
    file_reports: list[FileEvidenceRow]
    conversation_reports: list[ConversationEvidenceRow]
    task_clusters: list[TaskClusterRow]
    jira_candidates: list[JiraCandidateRow]
    jira_ready_day_count: int
    partially_ready_day_count: int
    weak_day_count: int
    not_ready_day_count: int
    file_evidence_group_count: int
    likely_edited_file_group_count: int
    task_cluster_count: int
    conversation_group_count: int
    unknown_unclassified_count: int
    missing_daily_recap_count: int
    degraded_payload_count: int
    parser_coverage_gap_count: int
    average_evidence_quality_score: float



def _safe_str(value: Any) -> str:
    if value is None:
        return ''
    return str(value).strip()



def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default



def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default



def _unique_strings(values: list[str], *, limit: int | None = None) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        label = _safe_str(value)
        if not label:
            continue
        key = label.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(label)
        if limit is not None and len(result) >= limit:
            break
    return result



def _format_number(value: int) -> str:
    return f'{value:,}'



def _format_ranked_items(items: list[tuple[str, int]], *, limit: int = 5) -> str:
    if not items:
        return 'none'
    selected = items[:limit]
    return '; '.join(f'`{label}` ({_format_number(count)})' for label, count in selected)



def _format_ranked_items_with_type(items: list[tuple[str, int, str]], *, limit: int = 5) -> str:
    if not items:
        return 'none'
    selected = items[:limit]
    return '; '.join(f'`{label}` ({_format_number(count)}) [{item_type}]' for label, count, item_type in selected)



def _format_list(values: list[str], *, limit: int = 5) -> str:
    if not values:
        return 'none'
    selected = values[:limit]
    return ', '.join(f'`{item}`' for item in selected)



def _format_time_range(first_seen: str | None, last_seen: str | None) -> str:
    if first_seen and last_seen:
        if first_seen == last_seen:
            return f'`{first_seen}`'
        return f'`{first_seen}` to `{last_seen}`'
    if first_seen:
        return f'`{first_seen}`'
    if last_seen:
        return f'`{last_seen}`'
    return 'n/a'



def _format_summary_ids(summary_ids: list[int], *, limit: int = 6) -> str:
    if not summary_ids:
        return 'none'
    selected = summary_ids[:limit]
    return ', '.join(f'`{summary_id}`' for summary_id in selected)



def _normalize_cluster_label(value: str) -> str:
    return _safe_str(value).casefold()



def _canonical_title(value: str) -> str:
    text = _safe_str(value)
    if not text:
        return ''
    text = WINDOW_PREFIX_RE.sub('', text)
    text = WINDOW_SUFFIX_RE.sub('', text)
    text = text.strip(' -')
    return text.strip()



def _is_generic_task_label(value: str) -> bool:
    label = _safe_str(value)
    if not label:
        return True
    lowered = label.casefold()
    if lowered in {'file', 'folder', 'project', 'projects', 'untitled', 'webex', 'microsoft outlook', 'outlook', 'program manager'}:
        return True
    if lowered.isdigit():
        return True
    return len(lowered) < 3



def _clean_path_label(path_value: str) -> list[str]:
    labels: list[str] = []
    path = _safe_str(path_value)
    if not path:
        return labels
    path_obj = PureWindowsPath(path)
    if path_obj.stem and not _is_generic_task_label(path_obj.stem):
        labels.append(path_obj.stem)
    if path_obj.name and not _is_generic_task_label(path_obj.name):
        labels.append(path_obj.name)
    if path_obj.parent and str(path_obj.parent) not in {path, '.'}:
        parent_name = path_obj.parent.name
        if parent_name and not _is_generic_task_label(parent_name):
            labels.append(parent_name)
    for match in TICKET_LABEL_RE.findall(path):
        labels.append(match.upper())
    return list(dict.fromkeys(labels))



def _clean_title_label(title: str) -> list[str]:
    labels: list[str] = []
    clean = _canonical_title(title)
    if not clean:
        return labels
    for match in TICKET_LABEL_RE.findall(clean):
        labels.append(match.upper())
    if not _is_generic_task_label(clean):
        labels.append(clean)
    return list(dict.fromkeys(labels))



def _parse_attributes_json(row: dict[str, Any]) -> dict[str, Any]:
    attributes = row.get('attributes_json')
    if isinstance(attributes, dict):
        return attributes
    if isinstance(attributes, str) and attributes.strip():
        try:
            parsed = json.loads(attributes)
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}



def _row_day(row: dict[str, Any]) -> str:
    return _safe_str(row.get('day'))



def _row_summary_id(row: dict[str, Any]) -> int | None:
    summary_id = row.get('summary_id')
    if summary_id is None:
        return None
    try:
        return int(summary_id)
    except (TypeError, ValueError):
        return None



def _row_start_ts(row: dict[str, Any]) -> float:
    return _safe_float(row.get('start_ts'), default=0.0)



def _row_end_ts(row: dict[str, Any]) -> float:
    return _safe_float(row.get('end_ts'), default=0.0)



def _row_confidence(row: dict[str, Any]) -> float:
    return _safe_float(row.get('confidence'), default=0.0)



def _row_entity_type(row: dict[str, Any]) -> str:
    return _safe_str(row.get('entity_type'))



def _row_entity_value(row: dict[str, Any]) -> str:
    return _safe_str(row.get('entity_value') or row.get('entity_normalized'))



def _row_process_name(row: dict[str, Any]) -> str:
    return _safe_str(row.get('process_name') or row.get('normalized_process_name'))



def _row_source_kind(row: dict[str, Any]) -> str:
    return _safe_str(row.get('source_kind'))



def _row_source_ref(row: dict[str, Any]) -> str:
    return _safe_str(row.get('source_ref'))



def _summaries_by_day(summaries: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in summaries:
        day = _row_day(row)
        if day:
            grouped[day].append(row)
    for rows in grouped.values():
        rows.sort(key=lambda item: (_row_start_ts(item), _row_summary_id(item) or 0))
    return grouped



def _quality_rows_by_day(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        day = _row_day(row)
        if day and _safe_str(row.get('summary_kind') or 'event') == 'event':
            grouped[day].append(row)
    for items in grouped.values():
        items.sort(key=lambda item: (_row_start_ts(item), _row_summary_id(item) or 0))
    return grouped



def _parser_rows_by_day(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        day = _row_day(row)
        if day:
            grouped[day].append(row)
    for items in grouped.values():
        items.sort(key=lambda item: (_row_start_ts(item), _row_summary_id(item) or 0))
    return grouped



def _activity_entities_by_day(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        day = _row_day(row)
        if day:
            grouped[day].append(row)
    for items in grouped.values():
        items.sort(key=lambda item: (_row_start_ts(item), _row_summary_id(item) or 0, _row_entity_type(item), _row_entity_value(item)))
    return grouped



def _extract_cluster_labels_from_entity(row: dict[str, Any]) -> list[str]:
    entity_type = _row_entity_type(row)
    value = _row_entity_value(row)
    if not value:
        return []
    labels: list[str] = []
    if entity_type in {'task_candidate', 'project_candidate'}:
        if not _is_generic_task_label(value):
            labels.append(value)
    elif entity_type in ENTITY_FILE_TYPES:
        labels.extend(_clean_path_label(value))
    elif entity_type in CONVERSATION_ENTITY_TYPES:
        labels.extend(_clean_title_label(value))
    else:
        labels.extend(_clean_title_label(value))
    return _unique_strings(labels)



def _score_day_readiness(
    *,
    average_score: float,
    daily_summary_present: bool,
    has_file_evidence: bool,
    has_task_evidence: bool,
    has_conversation_evidence: bool,
    unknown_count: int,
    unclassified_count: int,
    blocked_or_privacy_heavy_count: int,
    bucket_counts: dict[str, int],
) -> tuple[str, bool, list[str], float]:
    total_rows = sum(bucket_counts.get(bucket, 0) for bucket in BUCKET_ORDER)
    weak_or_poor = bucket_counts.get('weak', 0) + bucket_counts.get('poor', 0)
    unknown_pressure = (unknown_count + unclassified_count) / max(1, total_rows)
    support_score = average_score
    if daily_summary_present:
        support_score += 0.06
    if has_file_evidence:
        support_score += 0.05
    if has_task_evidence:
        support_score += 0.05
    if has_conversation_evidence:
        support_score += 0.04
    if blocked_or_privacy_heavy_count:
        support_score -= min(0.08, blocked_or_privacy_heavy_count / max(1, total_rows) * 0.08)
    support_score -= min(0.2, unknown_pressure * 0.2)
    support_score = max(0.0, min(1.0, support_score))

    rationale: list[str] = []
    rationale.append('daily summary present' if daily_summary_present else 'daily summary missing')
    if has_file_evidence:
        rationale.append('file evidence present')
    if has_task_evidence:
        rationale.append('task evidence present')
    if has_conversation_evidence:
        rationale.append('conversation evidence present')
    if blocked_or_privacy_heavy_count:
        rationale.append(f'{blocked_or_privacy_heavy_count} blocked/privacy-heavy summaries')
    if unknown_count or unclassified_count:
        rationale.append(f'unknown/unclassified evidence {unknown_count + unclassified_count}')
    if weak_or_poor:
        rationale.append(f'{weak_or_poor} weak/poor summaries')

    classification = 'not_ready'
    diary_useful = False
    if total_rows == 0:
        return classification, diary_useful, rationale, support_score
    if daily_summary_present and support_score >= 0.72 and (has_file_evidence or has_task_evidence or has_conversation_evidence) and weak_or_poor <= max(1, total_rows // 5):
        classification = 'jira_ready'
        diary_useful = True
    elif support_score >= 0.5 and (has_file_evidence or has_task_evidence or has_conversation_evidence):
        classification = 'partially_ready'
        diary_useful = True
    elif average_score >= 0.3 or has_file_evidence or has_task_evidence or has_conversation_evidence:
        classification = 'weak'
    return classification, diary_useful, rationale, support_score



def _role_for_program(program: str, *, linked_files: list[tuple[str, int, str]], linked_tasks: list[tuple[str, int, str]], linked_conversations: list[tuple[str, int, str]]) -> str:
    lowered = program.casefold()
    if lowered in {'outlook.exe', 'ciscollabhost.exe'} or len(linked_conversations) > len(linked_files):
        return 'communication_tool'
    if lowered == 'explorer.exe' or (linked_files and len(linked_files) >= len(linked_tasks) + len(linked_conversations)):
        return 'navigation_file_management'
    if linked_files or linked_tasks or linked_conversations:
        return 'primary_work_tool'
    return 'unknown_tool'



def _file_status_for_group(entity_type: str, attributes: list[dict[str, Any]], source_kinds: set[str]) -> tuple[str, float]:
    explicit_saved = any(
        bool(attr.get('saved_or_modified'))
        or bool(attr.get('saved'))
        or bool(attr.get('modified'))
        or bool(attr.get('was_saved'))
        for attr in attributes
    )
    dirty_marker = any(bool(attr.get('dirty_marker')) for attr in attributes)
    likely_edited = any(bool(attr.get('likely_edited')) for attr in attributes)
    if explicit_saved:
        return 'saved_or_modified', 0.95
    if dirty_marker or likely_edited:
        return 'likely_edited', 0.9 if entity_type == 'file_path' else 0.85
    if entity_type == 'file_path':
        return 'observed', 0.78
    if entity_type == 'folder_path':
        return 'referenced', 0.65
    if entity_type == 'file_name':
        return 'referenced', 0.62
    if source_kinds:
        return 'observed', 0.55
    return 'unknown', 0.4



def _confidence_label(value: float) -> str:
    if value >= 0.8:
        return 'high'
    if value >= 0.55:
        return 'medium'
    return 'low'



def _timestamp_to_local_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts).astimezone().isoformat(timespec='seconds')



def build_work_diary_readiness(data: Any) -> WorkDiaryReadinessMetrics:
    summaries_by_day = _summaries_by_day(getattr(data, 'event_summaries', []))
    daily_by_day = {day: row for day, row in ((_row_day(item), item) for item in getattr(data, 'daily_summaries', [])) if day}
    quality_by_day = _quality_rows_by_day(getattr(data, 'evidence_quality_rows', []))
    parser_by_day = _parser_rows_by_day(getattr(data, 'parser_coverage', []))
    activity_by_day = _activity_entities_by_day(getattr(data, 'activity_entities', []))
    entities_by_summary_id: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in getattr(data, 'activity_entities', []):
        summary_id = _row_summary_id(row)
        if summary_id is not None:
            entities_by_summary_id[summary_id].append(row)

    file_buckets: dict[tuple[str, str], dict[str, Any]] = {}
    conversation_buckets: dict[tuple[str, str], dict[str, Any]] = {}
    task_buckets: dict[str, dict[str, Any]] = {}
    program_buckets: dict[str, dict[str, Any]] = {}

    for summary in getattr(data, 'event_summaries', []):
        summary_id = _row_summary_id(summary)
        if summary_id is None:
            continue
        process_name = _row_process_name(summary)
        day = _row_day(summary)
        start_ts = _row_start_ts(summary)
        end_ts = _row_end_ts(summary)
        summary_entities = entities_by_summary_id.get(summary_id, [])
        summary_quality_rows = quality_by_day.get(day, [])
        parser_rows = [row for row in parser_by_day.get(day, []) if _row_summary_id(row) == summary_id]

        if process_name:
            program_bucket = program_buckets.setdefault(
                process_name.casefold(),
                {
                    'program': process_name,
                    'summary_ids': set(),
                    'days': set(),
                    'first_seen': start_ts,
                    'last_seen': end_ts,
                    'linked_rows': [],
                    'quality_rows': [],
                    'parser_rows': [],
                },
            )
            program_bucket['summary_ids'].add(summary_id)
            program_bucket['days'].add(day)
            program_bucket['first_seen'] = min(float(program_bucket['first_seen']), start_ts)
            program_bucket['last_seen'] = max(float(program_bucket['last_seen']), end_ts)
            program_bucket['linked_rows'].extend(summary_entities)
            program_bucket['quality_rows'].extend(summary_quality_rows)
            program_bucket['parser_rows'].extend(parser_rows)

        for entity in summary_entities:
            entity_type = _row_entity_type(entity)
            entity_value = _row_entity_value(entity)
            attrs = _parse_attributes_json(entity)
            if entity_type in ENTITY_FILE_TYPES and entity_value:
                bucket_key = (entity_type, _safe_str(entity.get('entity_normalized')))
                bucket = file_buckets.setdefault(
                    bucket_key,
                    {
                        'entity_type': entity_type,
                        'label': entity_value,
                        'normalized': _safe_str(entity.get('entity_normalized')),
                        'summary_ids': set(),
                        'days': set(),
                        'first_seen': _row_start_ts(entity),
                        'last_seen': _row_end_ts(entity),
                        'linked_processes': [],
                        'attributes': [],
                        'source_kinds': set(),
                        'source_refs': [],
                        'confidences': [],
                    },
                )
                bucket['summary_ids'].add(summary_id)
                bucket['days'].add(day)
                bucket['first_seen'] = min(float(bucket['first_seen']), _row_start_ts(entity))
                bucket['last_seen'] = max(float(bucket['last_seen']), _row_end_ts(entity))
                bucket['linked_processes'].append(process_name)
                bucket['attributes'].append(attrs)
                bucket['source_kinds'].add(_row_source_kind(entity))
                bucket['source_refs'].append(_row_source_ref(entity))
                bucket['confidences'].append(_row_confidence(entity))
            if entity_type in CONVERSATION_ENTITY_TYPES and entity_value:
                canonical = _canonical_title(entity_value)
                key = (entity_type, _normalize_cluster_label(canonical))
                bucket = conversation_buckets.setdefault(
                    key,
                    {
                        'entity_type': entity_type,
                        'title': canonical,
                        'summary_ids': set(),
                        'days': set(),
                        'first_seen': _row_start_ts(entity),
                        'last_seen': _row_end_ts(entity),
                        'source_processes': [],
                        'source_kinds': set(),
                        'source_refs': [],
                        'entity_types': set(),
                        'confidences': [],
                    },
                )
                bucket['summary_ids'].add(summary_id)
                bucket['days'].add(day)
                bucket['first_seen'] = min(float(bucket['first_seen']), _row_start_ts(entity))
                bucket['last_seen'] = max(float(bucket['last_seen']), _row_end_ts(entity))
                bucket['source_processes'].append(_safe_str(attrs.get('mail_process') or attrs.get('browser_process') or attrs.get('conversation_process') or process_name))
                bucket['source_kinds'].add(_row_source_kind(entity))
                bucket['source_refs'].append(_row_source_ref(entity))
                bucket['entity_types'].add(entity_type)
                bucket['confidences'].append(_row_confidence(entity))
            for label in _extract_cluster_labels_from_entity(entity):
                normalized_label = _normalize_cluster_label(label)
                bucket = task_buckets.setdefault(
                    normalized_label,
                    {
                        'label': label,
                        'summary_ids': set(),
                        'days': set(),
                        'programs': [],
                        'files': [],
                        'conversations': [],
                        'evidence_refs': [],
                        'signal_types': set(),
                    },
                )
                bucket['summary_ids'].add(summary_id)
                bucket['days'].add(day)
                bucket['programs'].append(process_name)
                if entity_type in ENTITY_FILE_TYPES:
                    bucket['files'].append(entity_value)
                    bucket['evidence_refs'].append(f'{entity_type}:{entity_value}')
                elif entity_type in CONVERSATION_ENTITY_TYPES:
                    bucket['conversations'].append(entity_value)
                    bucket['evidence_refs'].append(f'{entity_type}:{entity_value}')
                else:
                    bucket['evidence_refs'].append(f'summary {summary_id}')
                bucket['signal_types'].add(entity_type)

    day_reports: list[DailyReadinessRow] = []
    for day, day_summaries in summaries_by_day.items():
        day_entities = activity_by_day.get(day, [])
        day_quality_rows = quality_by_day.get(day, [])
        day_parser_rows = parser_by_day.get(day, [])
        daily_summary_present = day in daily_by_day
        avg_score = round(mean(_safe_float(row.get('score'), default=0.0) for row in day_quality_rows), 3) if day_quality_rows else 0.0
        bucket_counts = Counter(_safe_str(row.get('bucket') or 'unknown') for row in day_quality_rows)
        for bucket_name in BUCKET_ORDER:
            bucket_counts.setdefault(bucket_name, 0)
        top_programs = Counter(_row_entity_value(row) for row in day_entities if _row_entity_type(row) == 'program').most_common(5)
        top_files = Counter(( _row_entity_value(row), _row_entity_type(row) ) for row in day_entities if _row_entity_type(row) in ENTITY_FILE_TYPES).most_common(5)
        top_tasks = Counter(( _row_entity_value(row), _row_entity_type(row) ) for row in day_entities if _row_entity_type(row) in {'task_candidate', 'project_candidate'}).most_common(5)
        top_conversations = Counter(( _canonical_title(_row_entity_value(row)), _row_entity_type(row) ) for row in day_entities if _row_entity_type(row) in CONVERSATION_ENTITY_TYPES).most_common(5)
        top_programs_fmt = [(label, count) for (label, count) in top_programs]
        top_files_fmt = [(label, count, item_type) for ((label, item_type), count) in top_files]
        top_tasks_fmt = [(label, count, item_type) for ((label, item_type), count) in top_tasks]
        top_conversations_fmt = [(label, count, item_type) for ((label, item_type), count) in top_conversations]
        unknown_count = sum(1 for row in day_parser_rows if bool(row.get('unknown_app')))
        unclassified_count = sum(_safe_int(row.get('unclassified_evidence_count'), default=0) for row in day_parser_rows)
        blocked_or_privacy_heavy_count = sum(1 for row in day_quality_rows if bool(row.get('blocked_or_privacy_heavy')))
        classification, diary_useful, rationale, _support_score = _score_day_readiness(
            average_score=avg_score,
            daily_summary_present=daily_summary_present,
            has_file_evidence=bool(top_files_fmt),
            has_task_evidence=bool(top_tasks_fmt),
            has_conversation_evidence=bool(top_conversations_fmt),
            unknown_count=unknown_count,
            unclassified_count=unclassified_count,
            blocked_or_privacy_heavy_count=blocked_or_privacy_heavy_count,
            bucket_counts=dict(bucket_counts),
        )
        day_reports.append(
            DailyReadinessRow(
                day=day,
                event_summary_count=len(day_summaries),
                daily_summary_present=daily_summary_present,
                average_evidence_quality=avg_score,
                evidence_bucket_counts=dict(bucket_counts),
                top_programs=top_programs_fmt,
                top_files=top_files_fmt,
                top_task_candidates=top_tasks_fmt,
                top_conversation_subjects=top_conversations_fmt,
                unknown_evidence_count=unknown_count,
                unclassified_evidence_count=unclassified_count,
                blocked_or_privacy_heavy_count=blocked_or_privacy_heavy_count,
                classification=classification,
                diary_useful=diary_useful,
                rationale=rationale,
            )
        )

    program_reports: list[ProgramBreakdownRow] = []
    for bucket in program_buckets.values():
        summary_ids = sorted(int(item) for item in bucket['summary_ids'])
        linked_rows = bucket['linked_rows']
        linked_files = Counter(( _row_entity_value(row), _row_entity_type(row) ) for row in linked_rows if _row_entity_type(row) in ENTITY_FILE_TYPES).most_common(5)
        linked_tasks = Counter(( _row_entity_value(row), _row_entity_type(row) ) for row in linked_rows if _row_entity_type(row) in {'task_candidate', 'project_candidate'}).most_common(5)
        linked_conversations = Counter(( _canonical_title(_row_entity_value(row)), _row_entity_type(row) ) for row in linked_rows if _row_entity_type(row) in CONVERSATION_ENTITY_TYPES).most_common(5)
        linked_files_fmt = [(label, count, item_type) for ((label, item_type), count) in linked_files]
        linked_tasks_fmt = [(label, count, item_type) for ((label, item_type), count) in linked_tasks]
        linked_conversations_fmt = [(label, count, item_type) for ((label, item_type), count) in linked_conversations]
        quality_rows = bucket['quality_rows']
        bucket_counts = Counter(_safe_str(row.get('bucket') or 'unknown') for row in quality_rows)
        for bucket_name in BUCKET_ORDER:
            bucket_counts.setdefault(bucket_name, 0)
        parser_rows = bucket['parser_rows']
        unknown_rows = sum(1 for row in parser_rows if bool(row.get('unknown_app')))
        unclassified_count = sum(_safe_int(row.get('unclassified_evidence_count'), default=0) for row in parser_rows)
        unknown_ratio = round(unknown_rows / max(1, len(parser_rows)), 3)
        role = _role_for_program(bucket['program'], linked_files=linked_files_fmt, linked_tasks=linked_tasks_fmt, linked_conversations=linked_conversations_fmt)
        quality_score = round(mean(_safe_float(row.get('score'), default=0.0) for row in quality_rows), 3) if quality_rows else 0.0
        confidence = round(min(0.99, max(0.1, quality_score * 0.82 + (1.0 - unknown_ratio) * 0.18)), 2)
        program_reports.append(
            ProgramBreakdownRow(
                program=bucket['program'],
                summary_count=len(summary_ids),
                day_count=len(bucket['days']),
                first_seen=_timestamp_to_local_iso(float(bucket['first_seen'])),
                last_seen=_timestamp_to_local_iso(float(bucket['last_seen'])),
                linked_files=linked_files_fmt,
                linked_task_candidates=linked_tasks_fmt,
                linked_conversations=linked_conversations_fmt,
                evidence_bucket_counts=dict(bucket_counts),
                unknown_rows=unknown_rows,
                unclassified_evidence_count=unclassified_count,
                unknown_ratio=unknown_ratio,
                role=role,
                confidence=confidence,
            )
        )

    file_reports: list[FileEvidenceRow] = []
    for bucket in file_buckets.values():
        summary_ids = sorted(int(item) for item in bucket['summary_ids'])
        linked_processes = _unique_strings(bucket['linked_processes'], limit=5)
        status, status_confidence = _file_status_for_group(bucket['entity_type'], bucket['attributes'], bucket['source_kinds'])
        confidence = round(max(status_confidence, mean(bucket['confidences']) if bucket['confidences'] else 0.0), 2)
        supporting_sources = _unique_strings(
            [
                *[f'summary {summary_id}' for summary_id in summary_ids],
                *[f'process {process}' for process in linked_processes],
                *[str(source_kind) for source_kind in bucket['source_kinds']],
                *['dirty_marker' for attr in bucket['attributes'] if bool(attr.get('dirty_marker'))],
                *['likely_edited' for attr in bucket['attributes'] if bool(attr.get('likely_edited'))],
                *['explicit_saved' for attr in bucket['attributes'] if bool(attr.get('saved_or_modified')) or bool(attr.get('saved')) or bool(attr.get('modified')) or bool(attr.get('was_saved'))],
            ],
            limit=8,
        )
        file_reports.append(
            FileEvidenceRow(
                entity_type=bucket['entity_type'],
                label=bucket['label'],
                first_seen=_timestamp_to_local_iso(float(bucket['first_seen'])),
                last_seen=_timestamp_to_local_iso(float(bucket['last_seen'])),
                days_seen=len(bucket['days']),
                linked_processes=linked_processes,
                linked_summaries=summary_ids,
                status=status,
                confidence=confidence,
                supporting_evidence_sources=supporting_sources,
            )
        )

    conversation_reports: list[ConversationEvidenceRow] = []
    for bucket in conversation_buckets.values():
        summary_ids = sorted(int(item) for item in bucket['summary_ids'])
        linked_files = _unique_strings([
            _row_entity_value(entity)
            for summary_id in summary_ids
            for entity in entities_by_summary_id.get(summary_id, [])
            if _row_entity_type(entity) in ENTITY_FILE_TYPES
        ], limit=5)
        linked_tasks = _unique_strings([
            _row_entity_value(entity)
            for summary_id in summary_ids
            for entity in entities_by_summary_id.get(summary_id, [])
            if _row_entity_type(entity) in {'task_candidate', 'project_candidate'}
        ], limit=5)
        app_process = _unique_strings(bucket['source_processes'], limit=3)
        confidence = round(mean(bucket['confidences']) if bucket['confidences'] else 0.0, 2)
        conversation_reports.append(
            ConversationEvidenceRow(
                entity_type=bucket['entity_type'],
                title=bucket['title'],
                app_process=' / '.join(app_process) if app_process else 'unknown',
                first_seen=_timestamp_to_local_iso(float(bucket['first_seen'])),
                last_seen=_timestamp_to_local_iso(float(bucket['last_seen'])),
                linked_files=linked_files,
                linked_tasks=linked_tasks,
                content_known=False,
                confidence=confidence,
                note='title_only',
            )
        )

    task_clusters: list[TaskClusterRow] = []
    for bucket in task_buckets.values():
        summary_ids = sorted(int(item) for item in bucket['summary_ids'])
        if not summary_ids:
            continue
        programs = _unique_strings(bucket['programs'], limit=5)
        files = _unique_strings(bucket['files'], limit=5)
        conversations = _unique_strings(bucket['conversations'], limit=5)
        days = sorted(_unique_strings(list(bucket['days']), limit=10))
        signal_types = bucket['signal_types']
        ticket_like = bool(TICKET_LABEL_RE.search(bucket['label']))
        diversity = len({*programs, *files, *conversations})
        confidence_score = 0.35 + 0.12 * min(4, len(signal_types)) + 0.08 * min(4, len(days)) + 0.05 * min(3, diversity)
        if ticket_like:
            confidence_score += 0.12
        confidence_score = max(0.1, min(0.95, confidence_score))
        if ticket_like and files and conversations:
            interpretation = f"Likely task work around `{bucket['label']}` with file and discussion evidence."
        elif files and conversations:
            interpretation = f"Likely work item around `{bucket['label']}` grounded in file and conversation evidence."
        elif files:
            interpretation = f"Likely file-centric work around `{bucket['label']}`."
        elif conversations:
            interpretation = f"Likely discussion-first work around `{bucket['label']}`; content remains title-only."
        else:
            interpretation = f"Possible task/topic around `{bucket['label']}` with limited supporting evidence."
        evidence_refs = _unique_strings([
            *[f'summary {summary_id}' for summary_id in summary_ids[:8]],
            *[f'program {program}' for program in programs[:5]],
            *[f'file {file_label}' for file_label in files[:5]],
            *[f'conversation {conversation}' for conversation in conversations[:5]],
        ], limit=12)
        task_clusters.append(
            TaskClusterRow(
                task_label=bucket['label'],
                related_programs=programs,
                related_files=files,
                related_conversations=conversations,
                related_days=days,
                suggested_diary_interpretation=interpretation,
                confidence=round(confidence_score, 2),
                evidence_refs=evidence_refs,
            )
        )

    task_clusters.sort(key=lambda item: (-item.confidence, -len(item.related_programs), -len(item.related_files), -len(item.related_conversations), item.task_label.casefold()))

    jira_candidates: list[JiraCandidateRow] = []
    for cluster in task_clusters:
        if cluster.confidence < 0.45 and not TICKET_LABEL_RE.search(cluster.task_label):
            continue
        project_or_program = cluster.related_programs[0] if cluster.related_programs else 'unknown'
        if cluster.related_files and project_or_program == 'unknown':
            project_or_program = cluster.related_files[0]
        if cluster.related_conversations and project_or_program == 'unknown':
            project_or_program = cluster.related_conversations[0]
        if cluster.confidence >= 0.8:
            confidence_label = 'high'
        elif cluster.confidence >= 0.55:
            confidence_label = 'medium'
        else:
            confidence_label = 'low'
        if confidence_label == 'high':
            update_text = (
                f"Observed repeated work on `{cluster.task_label}` with supporting file and conversation evidence. "
                f"Relevant programs include {_format_list(cluster.related_programs, limit=3)}."
            )
        elif confidence_label == 'medium':
            update_text = (
                f"Observed repeated activity related to `{cluster.task_label}`. "
                f"Evidence links the work to {_format_list(cluster.related_programs, limit=3)} and {_format_list(cluster.related_files, limit=3)}."
            )
        else:
            update_text = (
                f"Possible JIRA update candidate around `{cluster.task_label}`. "
                f"Evidence is limited and should be verified before writing an issue update."
            )
        subtasks: list[str] = []
        if cluster.related_files:
            subtasks.append(f"Review {cluster.related_files[0]}")
        if len(cluster.related_files) > 1:
            subtasks.append(f"Compare {cluster.related_files[0]} with {cluster.related_files[1]}")
        if cluster.related_conversations:
            subtasks.append(f"Summarize the related discussion for {cluster.task_label}")
        supporting_evidence = cluster.evidence_refs[:8]
        caveats: list[str] = []
        if not cluster.related_files:
            caveats.append('No direct file evidence')
        if not cluster.related_conversations:
            caveats.append('No linked conversation evidence')
        if cluster.confidence < 0.7:
            caveats.append('Confidence is below the high-confidence threshold')
        jira_candidates.append(
            JiraCandidateRow(
                project_or_program=project_or_program,
                task_label=cluster.task_label,
                suggested_update_text=update_text,
                suggested_subtasks=subtasks[:3],
                supporting_evidence=supporting_evidence,
                confidence=confidence_label,
                caveats=caveats,
            )
        )

    jira_candidates.sort(key=lambda item: ({'high': 0, 'medium': 1, 'low': 2}.get(item.confidence, 3), item.task_label.casefold()))

    file_evidence_group_count = len(file_reports)
    likely_edited_file_group_count = sum(1 for item in file_reports if item.status in {'likely_edited', 'saved_or_modified'})
    task_cluster_count = len(task_clusters)
    conversation_group_count = len(conversation_reports)
    unknown_unclassified_count = sum(item.unknown_evidence_count + item.unclassified_evidence_count for item in day_reports)
    missing_daily_recap_count = sum(1 for item in day_reports if not item.daily_summary_present)
    degraded_payload_count = sum(1 for row in getattr(data, 'evidence_quality_rows', [])) and sum(1 for row in getattr(data, 'evidence_quality_rows', []) if bool(row.get('degraded_payload')))
    parser_coverage_gap_count = sum(1 for row in getattr(data, 'parser_coverage', []) if bool(row.get('unknown_app')))
    average_evidence_quality_score = round(mean(_safe_float(row.get('score'), default=0.0) for row in getattr(data, 'evidence_quality_rows', [])), 3) if getattr(data, 'evidence_quality_rows', []) else 0.0

    jira_ready_day_count = sum(1 for item in day_reports if item.classification == 'jira_ready')
    partially_ready_day_count = sum(1 for item in day_reports if item.classification == 'partially_ready')
    weak_day_count = sum(1 for item in day_reports if item.classification == 'weak')
    not_ready_day_count = sum(1 for item in day_reports if item.classification == 'not_ready')

    return WorkDiaryReadinessMetrics(
        day_reports=day_reports,
        program_reports=sorted(program_reports, key=lambda item: (-item.summary_count, item.program.casefold())),
        file_reports=sorted(file_reports, key=lambda item: (-item.days_seen, -item.confidence, item.label.casefold())),
        conversation_reports=sorted(conversation_reports, key=lambda item: (-item.confidence, item.title.casefold())),
        task_clusters=task_clusters,
        jira_candidates=jira_candidates[:12],
        jira_ready_day_count=jira_ready_day_count,
        partially_ready_day_count=partially_ready_day_count,
        weak_day_count=weak_day_count,
        not_ready_day_count=not_ready_day_count,
        file_evidence_group_count=file_evidence_group_count,
        likely_edited_file_group_count=likely_edited_file_group_count,
        task_cluster_count=task_cluster_count,
        conversation_group_count=conversation_group_count,
        unknown_unclassified_count=unknown_unclassified_count,
        missing_daily_recap_count=missing_daily_recap_count,
        degraded_payload_count=degraded_payload_count,
        parser_coverage_gap_count=parser_coverage_gap_count,
        average_evidence_quality_score=average_evidence_quality_score,
    )



def render_work_diary_section(metrics: WorkDiaryReadinessMetrics) -> list[str]:
    sections = [
        '## Work Diary / JIRA Readiness',
        '',
        '### Daily Work Reconstruction',
    ]
    for row in metrics.day_reports:
        bucket_text = ', '.join(f'{bucket} {_format_number(count)}' for bucket, count in row.evidence_bucket_counts.items())
        sections.append(
            f"- {row.day}: class `{row.classification}`; event summaries `{_format_number(row.event_summary_count)}`; "
            f"daily summary {'present' if row.daily_summary_present else 'missing'}; avg evidence `{row.average_evidence_quality:.3f}`; "
            f"buckets {bucket_text}; top programs {_format_ranked_items(row.top_programs)}; top files {_format_ranked_items_with_type(row.top_files)}; "
            f"top tasks {_format_ranked_items_with_type(row.top_task_candidates)}; top conversations {_format_ranked_items_with_type(row.top_conversation_subjects)}; "
            f"unknown/unclassified `{row.unknown_evidence_count}` / `{row.unclassified_evidence_count}`; diary useful `{'yes' if row.diary_useful else 'no'}`"
        )
        if row.rationale:
            sections.append(f"- {row.day} rationale: {', '.join(row.rationale)}")
    sections.extend(['', '### Program Activity Breakdown'])
    for row in metrics.program_reports:
        sections.append(
            f"- `{row.program}`: summaries `{row.summary_count}`, days `{row.day_count}`, range {_format_time_range(row.first_seen, row.last_seen)}, "
            f"linked files {_format_ranked_items_with_type(row.linked_files)}, linked tasks {_format_ranked_items_with_type(row.linked_task_candidates)}, "
            f"linked conversations {_format_ranked_items_with_type(row.linked_conversations)}, buckets {', '.join(f'{bucket} {_format_number(count)}' for bucket, count in row.evidence_bucket_counts.items())}, "
            f"unknown ratio `{row.unknown_ratio:.3f}`; role `{row.role}`; confidence `{row.confidence:.2f}`"
        )
    sections.extend(['', '### File Evidence Analysis'])
    for row in metrics.file_reports:
        sections.append(
            f"- `{row.entity_type}` `{row.label}`: range {_format_time_range(row.first_seen, row.last_seen)}, days seen `{row.days_seen}`, "
            f"linked processes {_format_list(row.linked_processes)}, linked summaries {_format_summary_ids(row.linked_summaries)}, status `{row.status}`, "
            f"confidence `{row.confidence:.2f}`, evidence {', '.join(row.supporting_evidence_sources) if row.supporting_evidence_sources else 'none'}"
        )
    sections.extend(['', '### Conversation / Mail / Chat Analysis'])
    for row in metrics.conversation_reports:
        sections.append(
            f"- `{row.entity_type}` `{row.title}`: app/process `{row.app_process}`, range {_format_time_range(row.first_seen, row.last_seen)}, "
            f"linked files {_format_list(row.linked_files)}, linked tasks {_format_list(row.linked_tasks)}, content_known `{str(row.content_known).lower()}` ({row.note}), confidence `{row.confidence:.2f}`"
        )
    sections.extend(['', '### Task / Job Candidate Clustering'])
    for row in metrics.task_clusters[:12]:
        sections.append(
            f"- `{row.task_label}`: programs {_format_list(row.related_programs)}, files {_format_list(row.related_files)}, conversations {_format_list(row.related_conversations)}, "
            f"days {_format_list(row.related_days)}, interpretation {row.suggested_diary_interpretation}, confidence `{row.confidence:.2f}`, refs {', '.join(row.evidence_refs) if row.evidence_refs else 'none'}"
        )
    sections.extend(['', '### JIRA Update Candidates'])
    for row in metrics.jira_candidates:
        subtasks = '; '.join(row.suggested_subtasks) if row.suggested_subtasks else 'none'
        caveats = '; '.join(row.caveats) if row.caveats else 'none'
        sections.append(
            f"- `{row.task_label}` / `{row.project_or_program}`: confidence `{row.confidence}`, update `{row.suggested_update_text}`, subtasks {subtasks}, evidence {', '.join(row.supporting_evidence) if row.supporting_evidence else 'none'}, caveats {caveats}"
        )
    sections.extend(['', '### Gap Analysis'])
    sections.extend([
        f"- Missing daily summaries: `{metrics.missing_daily_recap_count}`",
        f"- Days that are not ready or only weakly reconstructable: `{metrics.weak_day_count + metrics.not_ready_day_count}`",
        f"- File evidence groups without explicit modification evidence: `{max(0, metrics.file_evidence_group_count - metrics.likely_edited_file_group_count)}`",
        f"- Title-only conversation groups: `{metrics.conversation_group_count}`",
        f"- Low-confidence task clusters: `{sum(1 for row in metrics.task_clusters if row.confidence < 0.55)}`",
        f"- Degraded structured payloads: `{metrics.degraded_payload_count}`",
        f"- Parser coverage gaps / unknown apps: `{metrics.parser_coverage_gap_count}`",
        f"- Unknown/unclassified evidence count: `{metrics.unknown_unclassified_count}`",
    ])
    if metrics.day_reports:
        weak_days = ', '.join(f'`{row.day}`' for row in metrics.day_reports if row.classification in {'weak', 'not_ready'}) or 'none'
        sections.append(f'- Example weak days: {weak_days}')
    sections.append('')
    sections.append('### Recommendations')
    sections.extend(render_work_diary_recommendations(metrics))
    return sections



def render_work_diary_recommendations(metrics: WorkDiaryReadinessMetrics) -> list[str]:
    recommendations: list[str] = []
    if metrics.missing_daily_recap_count:
        recommendations.append('Improve the daily recap lifecycle so event days consistently roll into a daily diary entry.')
    if metrics.parser_coverage_gap_count:
        top_program = metrics.program_reports[0].program if metrics.program_reports else None
        if top_program:
            recommendations.append(f'Add or tune parsers for `{top_program}` because it still produces the most unknown coverage.')
        else:
            recommendations.append('Add parsers for the most common unknown apps/window patterns.')
    if metrics.file_evidence_group_count > metrics.likely_edited_file_group_count:
        recommendations.append('Add explicit filesystem or save/modify evidence because many files are observed but not classifiable as modified.')
    if metrics.task_cluster_count and sum(1 for row in metrics.task_clusters if row.confidence >= 0.55) < max(1, metrics.task_cluster_count // 3):
        recommendations.append('Improve task clustering and entity linking because file evidence is stronger than task/JIRA evidence.')
    if metrics.conversation_group_count and all(not row.content_known for row in metrics.conversation_reports):
        recommendations.append('Add richer conversation/message parsing so title-only subjects can be promoted to content-backed discussion evidence.')
    if metrics.jira_ready_day_count or metrics.partially_ready_day_count:
        recommendations.append('Proceed to Phase 3 entity search on the strongest days, but keep the low-confidence cases out of automatic JIRA updates.')
    if metrics.average_evidence_quality_score >= 0.7 and metrics.jira_ready_day_count:
        recommendations.append('The export is JIRA-ready enough for several days; use the ready days to validate the update template in a real issue.')
    if not recommendations:
        recommendations.append('No obvious blockers surfaced for diary/JIRA usefulness.')
    return [f'- {item}' for item in recommendations]



def render_work_diary_compare_deltas(current: WorkDiaryReadinessMetrics, baseline: WorkDiaryReadinessMetrics) -> list[str]:
    return [
        f"- jira_ready day count delta: `{current.jira_ready_day_count - baseline.jira_ready_day_count:+d}`",
        f"- partially_ready day count delta: `{current.partially_ready_day_count - baseline.partially_ready_day_count:+d}`",
        f"- average evidence quality delta: `{current.average_evidence_quality_score - baseline.average_evidence_quality_score:+.3f}`",
        f"- file evidence count delta: `{current.file_evidence_group_count - baseline.file_evidence_group_count:+d}`",
        f"- likely_edited count delta: `{current.likely_edited_file_group_count - baseline.likely_edited_file_group_count:+d}`",
        f"- task candidate count delta: `{current.task_cluster_count - baseline.task_cluster_count:+d}`",
        f"- conversation subject count delta: `{current.conversation_group_count - baseline.conversation_group_count:+d}`",
        f"- unknown/unclassified evidence delta: `{current.unknown_unclassified_count - baseline.unknown_unclassified_count:+d}`",
        f"- missing daily recap delta: `{current.missing_daily_recap_count - baseline.missing_daily_recap_count:+d}`",
    ]
