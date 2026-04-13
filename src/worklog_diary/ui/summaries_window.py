from __future__ import annotations

import logging
import threading
from datetime import date

from PySide6.QtCore import QDate, QTimer
from PySide6.QtGui import QColor, QTextCharFormat
from PySide6.QtWidgets import (
    QCalendarWidget,
    QFrame,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from ..core.services import MonitoringServices
from .summaries_view_model import DaySummaryView, SummaryCardView, build_calendar_highlight_days, build_day_summary_view


class SummariesWindow(QWidget):
    def __init__(self, services: MonitoringServices) -> None:
        super().__init__()
        self.services = services
        self.logger = logging.getLogger(__name__)
        self.setWindowTitle("WorkLog Diary Summaries")
        self.resize(1100, 700)

        self._highlighted_days: set[date] = set()
        self._selected_day = date.today()
        self._daily_recap_inflight = False

        root = QVBoxLayout(self)
        tools = QHBoxLayout()
        root.addLayout(tools)

        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.clicked.connect(self.refresh)
        tools.addWidget(self.refresh_button)

        self.generate_recap_button = QPushButton("Generate Daily Recap")
        self.generate_recap_button.clicked.connect(self._generate_daily_recap)
        tools.addWidget(self.generate_recap_button)
        tools.addStretch(1)

        body = QHBoxLayout()
        root.addLayout(body, 1)

        left = QVBoxLayout()
        body.addLayout(left, 1)

        left.addWidget(QLabel("Select day"))
        self.calendar = QCalendarWidget()
        self.calendar.setGridVisible(True)
        self.calendar.selectionChanged.connect(self._on_calendar_selection_changed)
        left.addWidget(self.calendar)

        right = QVBoxLayout()
        body.addLayout(right, 3)

        self.selected_date_label = QLabel()
        right.addWidget(self.selected_date_label)

        self.daily_recap_status_label = QLabel()
        right.addWidget(self.daily_recap_status_label)

        self.daily_recap_text = QTextEdit()
        self.daily_recap_text.setReadOnly(True)
        self.daily_recap_text.setPlaceholderText("No daily recap for this day.")
        self.daily_recap_text.setMinimumHeight(130)
        right.addWidget(self.daily_recap_text)

        self.no_data_label = QLabel("No summaries available for this day.")
        right.addWidget(self.no_data_label)

        self.summary_scroll = QScrollArea()
        self.summary_scroll.setWidgetResizable(True)
        right.addWidget(self.summary_scroll, 1)

        self.summary_cards_host = QWidget()
        self.summary_cards_layout = QVBoxLayout(self.summary_cards_host)
        self.summary_cards_layout.setContentsMargins(0, 0, 0, 0)
        self.summary_cards_layout.setSpacing(10)
        self.summary_scroll.setWidget(self.summary_cards_host)

    def refresh(self) -> None:
        selected_day = _qdate_to_day(self.calendar.selectedDate())
        self._refresh_calendar_highlights()
        self._load_day(selected_day)

    def _refresh_calendar_highlights(self) -> None:
        default_format = QTextCharFormat()
        for day_item in self._highlighted_days:
            self.calendar.setDateTextFormat(_day_to_qdate(day_item), default_format)

        summary_days = self.services.storage.list_summary_days(limit=3660)
        highlighted = build_calendar_highlight_days(summary_days)
        format_highlight = QTextCharFormat()
        format_highlight.setBackground(QColor("#d7ecff"))
        format_highlight.setForeground(QColor("#15415f"))

        for day_item in highlighted:
            self.calendar.setDateTextFormat(_day_to_qdate(day_item), format_highlight)
        self._highlighted_days = highlighted

    def _on_calendar_selection_changed(self) -> None:
        self._load_day(_qdate_to_day(self.calendar.selectedDate()))

    def _load_day(self, day: date) -> None:
        self._selected_day = day
        summaries = self.services.storage.list_summaries_for_day(day, limit=1000)
        daily_summary = self.services.storage.get_daily_summary_for_day(day)
        view = build_day_summary_view(day=day, summaries=summaries, daily_summary=daily_summary)

        self.logger.info(
            "event=calendar_summary_load day=%s summary_count=%s has_daily_recap=%s",
            day.isoformat(),
            len(view.cards),
            view.has_daily_recap,
        )
        self._render_day_view(view=view)

    def _render_day_view(self, view: DaySummaryView) -> None:
        self.selected_date_label.setText(f"Selected date: {view.day.isoformat()}")

        if view.has_daily_recap:
            self.daily_recap_status_label.setText(
                f"Daily recap: available (generated {view.daily_recap_created_label})"
            )
            self.daily_recap_text.setPlainText(view.daily_recap_text or "")
        else:
            self.daily_recap_status_label.setText("Daily recap: not generated")
            self.daily_recap_text.clear()

        self._clear_summary_cards()
        self.no_data_label.setVisible(len(view.cards) == 0)

        for card in view.cards:
            self.summary_cards_layout.addWidget(_build_summary_card_widget(card))
        self.summary_cards_layout.addStretch(1)

        self.generate_recap_button.setEnabled(not self._daily_recap_inflight and len(view.cards) > 0)

    def _clear_summary_cards(self) -> None:
        while self.summary_cards_layout.count():
            item = self.summary_cards_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()

    def _generate_daily_recap(self) -> None:
        selected_day = self._selected_day
        source_count = self.services.storage.count_batch_summaries_for_day(selected_day)
        if source_count <= 0:
            QMessageBox.information(self, "Daily Recap", "No batch summaries are available for the selected day.")
            return

        self._daily_recap_inflight = True
        self.generate_recap_button.setEnabled(False)
        self.generate_recap_button.setText("Generating...")

        def task() -> None:
            error_message: str | None = None
            try:
                self.services.generate_daily_recap(selected_day)
            except Exception as exc:
                error_message = str(exc)
            QTimer.singleShot(
                0,
                lambda: self._on_daily_recap_finished(selected_day, error_message),
            )

        threading.Thread(target=task, name="DailyRecapGeneration", daemon=True).start()

    def _on_daily_recap_finished(self, day: date, error_message: str | None) -> None:
        self._daily_recap_inflight = False
        self.generate_recap_button.setText("Generate Daily Recap")
        if error_message:
            QMessageBox.warning(self, "Daily Recap Failed", error_message)
        self._refresh_calendar_highlights()
        if self._selected_day == day:
            self._load_day(day)
        else:
            self.generate_recap_button.setEnabled(True)


def _build_summary_card_widget(card: SummaryCardView) -> QFrame:
    frame = QFrame()
    frame.setFrameShape(QFrame.Shape.StyledPanel)
    frame.setFrameShadow(QFrame.Shadow.Raised)

    layout = QVBoxLayout(frame)
    layout.setContentsMargins(10, 10, 10, 10)

    layout.addWidget(QLabel(f"Time range: {card.time_range}"))
    layout.addWidget(QLabel(f"Summary: {card.summary_text or '(empty)'}"))

    if card.major_activities:
        layout.addWidget(QLabel(_format_list_block("Major activities", card.major_activities)))
    if card.blocked_notes:
        layout.addWidget(QLabel(_format_list_block("Blocked/unanalyzed notes", card.blocked_notes)))
    if card.uncertainty_notes:
        layout.addWidget(QLabel(_format_list_block("Uncertainty/notes", card.uncertainty_notes)))

    for index in range(layout.count()):
        item = layout.itemAt(index)
        widget = item.widget()
        if isinstance(widget, QLabel):
            widget.setWordWrap(True)
    return frame


def _format_list_block(title: str, items: list[str]) -> str:
    lines = "\n".join(f"- {item}" for item in items)
    return f"{title}:\n{lines}"


def _qdate_to_day(value: QDate) -> date:
    return date(value.year(), value.month(), value.day())


def _day_to_qdate(value: date) -> QDate:
    return QDate(value.year, value.month, value.day)
