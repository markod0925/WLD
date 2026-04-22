from __future__ import annotations

import contextvars
import json
import logging
import re
from contextlib import contextmanager
from typing import Any

_llm_job_context: contextvars.ContextVar[dict[str, object | None] | None] = contextvars.ContextVar(
    "llm_job_context",
    default=None,
)


@contextmanager
def llm_job_context(job_id: object | None, **metadata: object | None):
    payload = {"job_id": job_id}
    payload.update(metadata)
    token = _llm_job_context.set(payload)
    try:
        yield
    finally:
        _llm_job_context.reset(token)


def get_llm_job_id(default: object | None = None) -> object | None:
    context = _llm_job_context.get()
    job_id = None if context is None else context.get("job_id")
    return default if job_id is None else job_id


def set_failed_stage(exc: BaseException, stage: str) -> BaseException:
    setattr(exc, "failed_stage", stage)
    return exc


def get_failed_stage(exc: BaseException, default: str = "unknown") -> str:
    failed_stage = getattr(exc, "failed_stage", None)
    return str(failed_stage) if failed_stage else default


def safe_preview(text: object | None, max_len: int = 300) -> str:
    if text is None:
        return ""
    value = str(text)
    value = re.sub(r"\s+", " ", value).strip()
    if len(value) <= max_len:
        return value
    return value[:max_len] + "..."


def safe_response_preview(text: object | None, max_len: int = 400) -> str:
    return safe_preview(text, max_len=max_len)


def safe_error(exc: BaseException | None, max_len: int = 300) -> str:
    if exc is None:
        return ""
    return safe_preview(str(exc), max_len=max_len)


def log_llm_stage(
    logger: logging.Logger,
    stage: str,
    status: str,
    *,
    level: int = logging.INFO,
    exc_info: bool | BaseException = False,
    job_id: object | None = None,
    **fields: Any,
) -> None:
    parts = ["[LLM]", f"stage={stage}", f"status={status}"]
    context = _llm_job_context.get() or {}
    current_job_id = job_id if job_id is not None else context.get("job_id")
    current_job_type = context.get("job_type")
    if current_job_id is not None:
        parts.append(f"job_id={_format_value(current_job_id)}")
    else:
        parts.append("job_id=none")

    for key in ("job_type", "timeout_s", "attempt", "input_chars", "input_token_estimate", "queue_size", "queue_wait_s"):
        value = fields.pop(key, context.get(key))
        if value is None:
            continue
        parts.append(f"{key}={_format_value(value)}")

    if current_job_type is not None and not any(part.startswith("job_type=") for part in parts):
        parts.append(f"job_type={_format_value(current_job_type)}")

    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={_format_value(value)}")
    logger.log(level, " ".join(parts), exc_info=exc_info)


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return f"{value:.3f}"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (dict, list, tuple)):
        try:
            value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        except TypeError:
            value = str(value)
    return safe_preview(value, max_len=400)
