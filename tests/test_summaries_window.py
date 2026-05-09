from __future__ import annotations

import threading
import time
from datetime import date
from types import SimpleNamespace

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QLabel

from qt_test_utils import require_qt
from worklog_diary.core.models import DailySummaryRecord, SummaryRecord

pytestmark = pytest.mark.qt


@pytest.fixture(autouse=True)
def _require_qt_runtime() -> None:
    require_qt()


class FakeStorage:
    def __init__(self, day: date) -> None:
        self.day = day
        self._daily_summary: DailySummaryRecord | None = None
        self._summaries = [
            SummaryRecord(
                id=1,
                job_id=1,
                start_ts=10.0,
                end_ts=20.0,
                summary_text="worked on recap UI",
                summary_json={},
                created_ts=30.0,
            )
        ]

    def count_batch_summaries_for_day(self, day: date) -> int:
        return 1 if day == self.day else 0

    def list_summary_days(self, limit: int = 3660) -> list[date]:
        return [self.day] if self._summaries else []

    def list_summaries_for_day(self, day: date, limit: int = 1000) -> list[SummaryRecord]:
        return list(self._summaries) if day == self.day else []

    def list_effective_summaries_for_day(self, day: date, *, use_coalesced: bool = False) -> list[SummaryRecord]:
        return self.list_summaries_for_day(day)

    def get_daily_summary_for_day(self, day: date) -> DailySummaryRecord | None:
        return self._daily_summary if day == self.day else None

    def list_semantic_merge_diagnostics(
        self,
        day: date,
        *,
        decision: str | None = None,
        limit: int = 1000,
    ) -> list[object]:
        return []


class FakeServices:
    def __init__(self, storage: FakeStorage) -> None:
        self.config = SimpleNamespace(semantic_coalescing_enabled=False)
        self.storage = storage
        self.started = threading.Event()
        self.release = threading.Event()
        self.is_drain_active = False

    def cancel_flush_drain(self) -> bool:
        return False

    def flush_now(self, reason: str = "manual") -> None:
        return None

    def generate_daily_recap(self, day: date) -> dict[str, int | str | bool]:
        self.storage._daily_summary = DailySummaryRecord(
            id=9,
            day=day,
            recap_text="daily recap ready",
            recap_json={"major_activities": ["worked on recap UI"]},
            source_batch_count=1,
            created_ts=40.0,
        )
        self.started.set()
        self.release.wait(timeout=5)
        return {
            "day": day.isoformat(),
            "daily_summary_id": 9,
            "source_batch_count": 1,
            "replaced": False,
        }


def _get_app():
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_daily_recap_generation_resets_button_after_background_completion() -> None:
    from worklog_diary.ui.summaries_window import SummariesWindow

    app = _get_app()
    target_day = date(2026, 4, 10)
    storage = FakeStorage(target_day)
    services = FakeServices(storage)
    window = SummariesWindow(services)

    try:
        window._load_day(target_day)
        window._generate_daily_recap()

        assert services.started.wait(timeout=2)
        assert window.generate_recap_button.text() == "Generating..."
        assert window.generate_recap_button.isEnabled() is False

        services.release.set()

        deadline = time.time() + 2
        while time.time() < deadline:
            app.processEvents()
            if (
                window.generate_recap_button.text() == "Generate Daily Recap"
                and window.generate_recap_button.isEnabled() is True
            ):
                break
            time.sleep(0.01)

        assert window.generate_recap_button.text() == "Generate Daily Recap"
        assert window.generate_recap_button.isEnabled() is True
    finally:
        window.close()
        window.deleteLater()
        app.processEvents()


def test_summaries_window_auto_refreshes_when_storage_changes() -> None:
    from worklog_diary.ui.summaries_window import SummariesWindow

    app = _get_app()
    target_day = date(2026, 4, 10)
    storage = FakeStorage(target_day)
    services = FakeServices(storage)
    window = SummariesWindow(services)

    try:
        window.show()
        app.processEvents()
        window._load_day(target_day)
        assert window.summary_cards_layout.count() == 2

        storage._summaries.append(
            SummaryRecord(
                id=2,
                job_id=2,
                start_ts=30.0,
                end_ts=40.0,
                summary_text="new item",
                summary_json={},
                created_ts=50.0,
            )
        )

        window._maybe_refresh()
        app.processEvents()

        assert window.summary_cards_layout.count() == 3
        assert window.selected_date_label.text() == f"Selected date: {target_day.isoformat()}"
    finally:
        window.close()
        window.deleteLater()
        app.processEvents()


def test_summary_card_body_is_selectable_and_uses_explicit_text_formats() -> None:
    from worklog_diary.ui.summaries_view_model import SummaryCardView
    from worklog_diary.ui.summaries_window import _build_summary_card_widget

    card = SummaryCardView(
        summary_id=1,
        time_range="10:00:00 - 10:15:00",
        summary_text="Investigated <parser> issue and fixed naïve matching.",
        major_activities=["Opened Caffè plan.md"],
        blocked_notes=[],
        uncertainty_notes=[],
    )

    widget = _build_summary_card_widget(card, "naïve")
    labels = widget.findChildren(QLabel)
    assert len(labels) >= 3

    header_label = labels[0]
    summary_label = labels[1]
    details_label = labels[2]
    selectable_flags = (
        Qt.TextInteractionFlag.TextSelectableByMouse | Qt.TextInteractionFlag.TextSelectableByKeyboard
    )

    try:
        assert header_label.textFormat() == Qt.TextFormat.PlainText
        assert summary_label.textFormat() == Qt.TextFormat.RichText
        assert summary_label.textInteractionFlags() == selectable_flags
        assert "background-color: #fff176" in summary_label.text()
        assert "&lt;parser&gt;" in summary_label.text()
        assert "<parser>" not in summary_label.text()
        assert details_label.textFormat() == Qt.TextFormat.PlainText
        assert details_label.textInteractionFlags() == selectable_flags
        assert "Caffè" in details_label.text()
    finally:
        widget.deleteLater()
