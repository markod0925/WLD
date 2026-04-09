from __future__ import annotations

from datetime import datetime

from PySide6.QtWidgets import (
    QHBoxLayout,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from ..core.models import SummaryRecord
from ..core.services import MonitoringServices


class SummariesWindow(QWidget):
    def __init__(self, services: MonitoringServices) -> None:
        super().__init__()
        self.services = services
        self.setWindowTitle("WorkLog Diary Summaries")
        self.resize(900, 600)

        self._items: list[SummaryRecord] = []

        root = QVBoxLayout(self)
        tools = QHBoxLayout()
        root.addLayout(tools)

        refresh_button = QPushButton("Refresh")
        refresh_button.clicked.connect(self.refresh)
        tools.addWidget(refresh_button)
        tools.addStretch(1)

        body = QHBoxLayout()
        root.addLayout(body)

        self.list_widget = QListWidget()
        self.list_widget.currentRowChanged.connect(self._on_row_changed)
        body.addWidget(self.list_widget, 1)

        self.details = QTextEdit()
        self.details.setReadOnly(True)
        body.addWidget(self.details, 2)

    def refresh(self) -> None:
        self.list_widget.clear()
        self.details.clear()

        self._items = self.services.storage.list_summaries(limit=200)
        for summary in self._items:
            created = datetime.fromtimestamp(summary.created_ts).strftime("%Y-%m-%d %H:%M:%S")
            text_preview = summary.summary_text.strip().replace("\n", " ")
            if len(text_preview) > 80:
                text_preview = text_preview[:80] + "..."
            label = f"{created} | {text_preview}"
            self.list_widget.addItem(QListWidgetItem(label))

        if self._items:
            self.list_widget.setCurrentRow(0)

    def _on_row_changed(self, row: int) -> None:
        if row < 0 or row >= len(self._items):
            self.details.clear()
            return

        item = self._items[row]
        start = datetime.fromtimestamp(item.start_ts).strftime("%Y-%m-%d %H:%M:%S")
        end = datetime.fromtimestamp(item.end_ts).strftime("%Y-%m-%d %H:%M:%S")

        body = (
            f"Summary ID: {item.id}\n"
            f"Job ID: {item.job_id}\n"
            f"Range: {start} -> {end}\n"
            f"Created: {datetime.fromtimestamp(item.created_ts).strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"{item.summary_text}\n\n"
            "Structured JSON:\n"
            f"{item.summary_json}"
        )
        self.details.setPlainText(body)
