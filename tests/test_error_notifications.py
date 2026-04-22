from __future__ import annotations

from pathlib import Path

import pytest
import requests

from worklog_diary.core.batching import BatchBuilder, SummaryBatch
from worklog_diary.core.error_notifications import ErrorNotificationManager
from worklog_diary.core.errors import LMStudioConnectionError, LMStudioTimeoutError
from worklog_diary.core.lmstudio_client import LMStudioClient
from worklog_diary.core.models import ForegroundInfo, KeyEvent, TextSegment
from worklog_diary.core.storage import SQLiteStorage
from worklog_diary.core.summarizer import Summarizer


class SuccessfulClient:
    def summarize_batch(self, *_args: object, **_kwargs: object) -> tuple[str, dict]:
        return "done", {"summary_text": "done", "key_points": [], "blocked_activity": []}


class FailingConnectionClient:
    def summarize_batch(self, *_args: object, **_kwargs: object) -> tuple[str, dict]:
        raise LMStudioConnectionError("Connection error: Unable to reach LM Studio. Check that it is running.")


def _seed_raw_data(storage: SQLiteStorage) -> None:
    info = ForegroundInfo(
        timestamp=10.0,
        hwnd=100,
        pid=200,
        process_name="code.exe",
        window_title="Editor",
    )
    interval_id = storage.start_interval(info, blocked=False)
    storage.close_interval(interval_id, end_ts=20.0)

    storage.insert_key_event(
        KeyEvent(
            id=None,
            ts=12.0,
            key="a",
            event_type="down",
            modifiers=[],
            process_name="code.exe",
            window_title="Editor",
            hwnd=100,
            active_interval_id=interval_id,
            processed=False,
        )
    )

    storage.insert_text_segments(
        [
            TextSegment(
                id=None,
                start_ts=12.0,
                end_ts=13.0,
                process_name="code.exe",
                window_title="Editor",
                text="a",
                hotkeys=[],
                raw_key_count=1,
            )
        ]
    )


def test_error_notification_manager_deduplicates_until_resolved() -> None:
    notifications: list[tuple[str, str]] = []
    manager = ErrorNotificationManager(lambda category, message: notifications.append((category, message)))

    assert manager.notify("lmstudio_connection", "Connection error", key="same") is True
    assert manager.notify("lmstudio_connection", "Connection error", key="same") is False
    assert notifications == [("lmstudio_connection", "Connection error")]

    manager.resolve("lmstudio_connection")

    assert manager.notify("lmstudio_connection", "Connection error", key="same") is True
    assert notifications == [
        ("lmstudio_connection", "Connection error"),
        ("lmstudio_connection", "Connection error"),
    ]


def test_summarizer_reports_lmstudio_connection_error_once(tmp_path: Path) -> None:
    db_path = tmp_path / "worklog.db"
    storage = SQLiteStorage(str(db_path))
    _seed_raw_data(storage)

    notifications: list[tuple[str, str]] = []
    notifier = ErrorNotificationManager(lambda category, message: notifications.append((category, message)))

    summarizer = Summarizer(
        storage=storage,
        batch_builder=BatchBuilder(storage=storage, max_text_segments=200, max_screenshots=3),
        lm_client=FailingConnectionClient(),
        error_notifier=notifier,
    )

    try:
        assert summarizer.flush_pending(reason="test") is None
        assert summarizer.flush_pending(reason="test") is None
        assert notifications == [
            (
                "lmstudio_connection",
                "Connection error: Unable to reach LM Studio. Check that it is running.",
            )
        ]

        summarizer.lm_client = SuccessfulClient()
        assert summarizer.flush_pending(reason="test") is not None

        _seed_raw_data(storage)
        summarizer.lm_client = FailingConnectionClient()
        assert summarizer.flush_pending(reason="test") is None
        assert len(notifications) == 2
    finally:
        summarizer.stop()
        storage.close()


def test_lmstudio_client_maps_timeout_to_connection_error(monkeypatch: pytest.MonkeyPatch) -> None:
    client = LMStudioClient(base_url="http://localhost:1234/v1", model="test-model", timeout_seconds=5)
    batch = SummaryBatch(
        start_ts=1.0,
        end_ts=2.0,
        active_intervals=[],
        blocked_intervals=[],
        text_segments=[],
        screenshots=[],
    )

    def fake_post(*_args: object, **_kwargs: object) -> object:
        raise requests.Timeout("timed out")

    monkeypatch.setattr(requests, "post", fake_post)

    with pytest.raises(LMStudioTimeoutError) as exc_info:
        client.summarize_batch(batch)

    assert "timed out" in str(exc_info.value)
