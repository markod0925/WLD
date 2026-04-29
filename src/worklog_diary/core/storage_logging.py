from __future__ import annotations

import logging
import time


def log_db_query_timing(logger: logging.Logger, operation: str, started_at: float, *, rows: int | None = None) -> None:
    duration_ms = (time.perf_counter() - started_at) * 1000.0
    if rows is None:
        logger.info("event=db_query_timing operation=%s duration_ms=%.3f", operation, duration_ms)
    else:
        logger.info(
            "event=db_query_timing operation=%s duration_ms=%.3f rows=%s",
            operation,
            duration_ms,
            rows,
        )
