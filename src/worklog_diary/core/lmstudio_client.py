from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import requests

from .batching import SummaryBatch


class LMStudioClient:
    def __init__(self, base_url: str, model: str, timeout_seconds: int = 60) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds

    def summarize_batch(self, batch: SummaryBatch) -> tuple[str, dict[str, Any]]:
        prompt_text = _build_summary_prompt(batch)

        content: list[dict[str, Any]] = [{"type": "text", "text": prompt_text}]
        for screenshot in batch.screenshots:
            image_data = _file_to_data_uri(screenshot.file_path)
            if image_data:
                content.append({"type": "image_url", "image_url": {"url": image_data}})

        payload = {
            "model": self.model,
            "temperature": 0.2,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You summarize desktop work activity. "
                        "Respond in JSON with keys: summary_text, key_points, blocked_activity."
                    ),
                },
                {"role": "user", "content": content},
            ],
        }

        response = requests.post(
            f"{self.base_url}/chat/completions",
            json=payload,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

        data = response.json()
        raw_content = data["choices"][0]["message"]["content"]
        if isinstance(raw_content, list):
            text_content = "\n".join(part.get("text", "") for part in raw_content if isinstance(part, dict))
        else:
            text_content = str(raw_content)

        parsed = _parse_model_response(text_content)
        summary_text = parsed.get("summary_text") or parsed.get("summary") or text_content
        return str(summary_text), parsed



def _build_summary_prompt(batch: SummaryBatch) -> str:
    batch_json = json.dumps(batch.to_dict(), indent=2)
    return (
        "Summarize the following WorkLog Diary activity batch. "
        "Focus on meaningful tasks, context switches, and likely progress. "
        "Treat blocked intervals as intentionally redacted privacy windows.\n\n"
        f"{batch_json}"
    )



def _file_to_data_uri(path: str) -> str | None:
    file_path = Path(path)
    if not file_path.exists():
        return None
    raw = file_path.read_bytes()
    b64 = base64.b64encode(raw).decode("ascii")
    suffix = file_path.suffix.lower()
    mime = "image/png" if suffix == ".png" else "image/jpeg"
    return f"data:{mime};base64,{b64}"



def _parse_model_response(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        if cleaned.startswith("json"):
            cleaned = cleaned[4:].strip()

    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    return {"summary_text": text, "raw_response": text}
