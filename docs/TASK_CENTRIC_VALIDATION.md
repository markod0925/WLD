# Task-centric validation quick guide

1. Generate a daily recap for a target day in WLD (manual recap trigger or backfill run).
2. Export an audit bundle for that day using the audit export tool/UI.
3. Inspect:
   - `task_clusters.jsonl`
   - `summary_task_links.jsonl`
   - `activity_entities.jsonl`
   - `daily_summaries.jsonl`
   - `summaries.jsonl`
   - `manifest.json`

Healthy output signs:
- A few meaningful task clusters (not dozens of micro-clusters).
- Support apps (Notepad/Excel/Chrome) folded into real tasks via evidence.
- `daily_summaries.jsonl` rows with `generated_from_task_clusters=true` and structured payloads.
- `ignored_noise` counts present for blocked/unknown/searchhost without dominating task narrative.
- Main tasks / Minor tasks / Ignored noise sections visible in daily summary output.
