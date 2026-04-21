from __future__ import annotations

from datetime import datetime

from .models import SummaryRecord


def build_canonical_embedding_text(summary: SummaryRecord) -> str:
    context = summary.summary_json.get("source_context") if isinstance(summary.summary_json, dict) else {}
    process_name = ""
    window_title = ""
    if isinstance(context, dict):
        process_name = str(context.get("process_name", "")).strip()
        window_title = str(context.get("window_title", "")).strip()

    start_label = datetime.fromtimestamp(summary.start_ts).strftime("%H:%M")
    end_label = datetime.fromtimestamp(summary.end_ts).strftime("%H:%M")
    parts = [
        f"app={process_name or 'unknown'}",
        f"window={window_title or 'unknown'}",
        f"timespan={start_label}-{end_label}",
        f"summary={summary.summary_text.strip()}",
    ]
    return "\n".join(parts)
