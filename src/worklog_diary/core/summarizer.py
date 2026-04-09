from __future__ import annotations

import logging

from .batching import BatchBuilder
from .lmstudio_client import LMStudioClient
from .storage import SQLiteStorage


class Summarizer:
    def __init__(
        self,
        storage: SQLiteStorage,
        batch_builder: BatchBuilder,
        lm_client: LMStudioClient,
    ) -> None:
        self.storage = storage
        self.batch_builder = batch_builder
        self.lm_client = lm_client
        self.logger = logging.getLogger(__name__)

    def flush_pending(self, reason: str = "manual") -> int | None:
        batch = self.batch_builder.build_pending_batch()
        if batch is None:
            self.logger.info("No pending data to summarize (%s)", reason)
            return None

        job_id = self.storage.create_summary_job(batch.start_ts, batch.end_ts, status="running")
        try:
            summary_text, summary_json = self.lm_client.summarize_batch(batch)
            summary_id = self.storage.insert_summary(
                job_id=job_id,
                start_ts=batch.start_ts,
                end_ts=batch.end_ts,
                summary_text=summary_text,
                summary_json=summary_json,
            )

            self.storage.mark_intervals_summarized(batch.start_ts, batch.end_ts)
            self.storage.purge_raw_data(batch.start_ts, batch.end_ts)
            self.storage.update_summary_job(job_id, status="succeeded")
            self.logger.info("Summary created for %.2f -> %.2f", batch.start_ts, batch.end_ts)
            return summary_id
        except Exception as exc:
            self.storage.update_summary_job(job_id, status="failed", error=str(exc))
            self.logger.exception("Summary job failed: %s", exc)
            return None
