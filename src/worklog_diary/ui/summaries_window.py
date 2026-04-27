from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from datetime import date

from PySide6.QtCore import QDate, QTimer, Signal
from PySide6.QtGui import QColor, QTextCharFormat
from PySide6.QtWidgets import (
    QCalendarWidget,
    QComboBox,
    QDateEdit,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QDialog,
    QTableWidget,
    QTableWidgetItem,
    QDialogButtonBox,
    QCheckBox,
    QFileDialog,
)

from worklog_diary.core.audit_export import AuditExportError, AuditExportOptions, export_audit_bundle
from worklog_diary.core.lmstudio_logging import get_failed_stage
from worklog_diary.core.summary_search import (
    SummarySearchParams,
    SummarySearchResult,
    SummarySearchScope,
    SummarySearchService,
    SummarySearchType,
)

from ..core.monitoring_components import DiagnosticsService
from ..core.services import MonitoringServices
from .summaries_view_model import (
    DaySummaryView,
    SummaryCardView,
    build_calendar_highlight_days,
    build_day_summary_view,
    format_summary_html,
)
from .semantic_diagnostics_view_model import build_semantic_diagnostics_rows
from .semantic_diagnostics_view_model import build_coalesced_traceability_map


class SummariesWindow(QWidget):
    daily_recap_finished = Signal(object, object)
    flush_finished = Signal(object, object)

    def __init__(
        self,
        services: MonitoringServices,
        diagnostics_service: DiagnosticsService | None = None,
    ) -> None:
        super().__init__()
        self.services = services
        self.diagnostics_service = diagnostics_service
        self.search_service = SummarySearchService(services.storage)
        self.logger = logging.getLogger(__name__)
        self.setWindowTitle("WorkLog Diary Summaries")
        self.resize(1100, 700)

        self._highlighted_days: set[date] = set()
        self._selected_day = date.today()
        self._daily_recap_inflight = False
        self._flush_inflight = False
        self._last_refresh_signature: tuple[object, ...] | None = None
        self.daily_recap_finished.connect(self._on_daily_recap_finished)
        self.flush_finished.connect(self._on_flush_finished)

        self._refresh_timer = QTimer(self)
        self._refresh_timer.setInterval(3000)
        self._refresh_timer.timeout.connect(self._maybe_refresh)

        root = QVBoxLayout(self)
        tools = QHBoxLayout()
        root.addLayout(tools)

        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.clicked.connect(self.refresh)
        tools.addWidget(self.refresh_button)

        self.flush_button = QPushButton("Start Flush")
        self.flush_button.clicked.connect(self._toggle_flush)
        tools.addWidget(self.flush_button)

        self.generate_recap_button = QPushButton("Generate Daily Recap")
        self.generate_recap_button.clicked.connect(self._generate_daily_recap)
        tools.addWidget(self.generate_recap_button)

        self.coalescing_diag_button = QPushButton("Semantic Diagnostics")
        self.coalescing_diag_button.clicked.connect(self._open_semantic_diagnostics)
        tools.addWidget(self.coalescing_diag_button)

        self.audit_export_button = QPushButton("Export audit bundle")
        self.audit_export_button.setToolTip(
            "Export generated summaries, daily recaps, coalesced summaries, and merge diagnostics."
        )
        self.audit_export_button.clicked.connect(self._export_audit_bundle)
        tools.addWidget(self.audit_export_button)
        tools.addStretch(1)

        search_tools = QHBoxLayout()
        root.addLayout(search_tools)
        search_tools.addWidget(QLabel("Search summaries"))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Type text to search event/day summaries...")
        self.search_input.returnPressed.connect(self._run_search)
        search_tools.addWidget(self.search_input, 2)

        self.search_scope_combo = QComboBox()
        self.search_scope_combo.addItem("Day", SummarySearchScope.DAY)
        self.search_scope_combo.addItem("Month", SummarySearchScope.MONTH)
        self.search_scope_combo.addItem("Year", SummarySearchScope.YEAR)
        self.search_scope_combo.addItem("All", SummarySearchScope.ALL)
        self.search_scope_combo.currentIndexChanged.connect(self._sync_search_anchor_visibility)
        search_tools.addWidget(self.search_scope_combo)

        self.search_anchor_date = QDateEdit()
        self.search_anchor_date.setCalendarPopup(True)
        self.search_anchor_date.setDate(_day_to_qdate(self._selected_day))
        self.search_anchor_date.dateChanged.connect(self._sync_search_anchor_label)
        self.search_anchor_label = QLabel()
        search_tools.addWidget(self.search_anchor_date)
        search_tools.addWidget(self.search_anchor_label)

        self.search_button = QPushButton("Search")
        self.search_button.clicked.connect(self._run_search)
        search_tools.addWidget(self.search_button)

        self.clear_search_button = QPushButton("Clear")
        self.clear_search_button.clicked.connect(self._clear_search)
        search_tools.addWidget(self.clear_search_button)

        self.search_status_label = QLabel("Enter a query and press Enter or Search.")
        root.addWidget(self.search_status_label)

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
        self.semantic_status_label = QLabel()
        right.addWidget(self.semantic_status_label)

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
        self._sync_search_anchor_visibility()
        self._sync_search_anchor_label()

    def refresh(self) -> None:
        selected_day = self._selected_day
        self._refresh_calendar_highlights()
        self._load_day(selected_day)
        self._sync_flush_button_state()

    def showEvent(self, event) -> None:  # type: ignore[override]
        super().showEvent(event)
        if not self._refresh_timer.isActive():
            self._refresh_timer.start()
        self._maybe_refresh()

    def hideEvent(self, event) -> None:  # type: ignore[override]
        if self._refresh_timer.isActive():
            self._refresh_timer.stop()
        super().hideEvent(event)

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
        selected = _qdate_to_day(self.calendar.selectedDate())
        self.search_anchor_date.setDate(_day_to_qdate(selected))
        self._load_day(selected)

    def _load_day(self, day: date) -> None:
        self._selected_day = day
        use_coalesced = bool(self.services.config.semantic_coalescing_enabled)
        summaries = self.services.storage.list_effective_summaries_for_day(day, use_coalesced=use_coalesced)
        daily_summary = self.services.storage.get_daily_summary_for_day(day)
        diagnostics = self.services.storage.list_semantic_merge_diagnostics(day, decision="merge", limit=2000)
        traceability = build_coalesced_traceability_map(summaries, diagnostics)
        view = build_day_summary_view(
            day=day,
            summaries=summaries,
            daily_summary=daily_summary,
            traceability_by_summary_id=traceability,
        )

        self.logger.info(
            "event=calendar_summary_load day=%s summary_count=%s has_daily_recap=%s",
            day.isoformat(),
            len(view.cards),
            view.has_daily_recap,
        )
        self._render_day_view(view=view)
        self._last_refresh_signature = self._build_refresh_signature(day)

    def _render_day_view(self, view: DaySummaryView) -> None:
        self.search_status_label.setText("Enter a query and press Enter or Search.")
        self.selected_date_label.setText(f"Selected date: {view.day.isoformat()}")

        if view.has_daily_recap:
            self.daily_recap_status_label.setText(
                f"Daily recap: available (generated {view.daily_recap_created_label})"
            )
            self.daily_recap_text.setPlainText(view.daily_recap_text or "")
        else:
            self.daily_recap_status_label.setText("Daily recap: not generated")
            self.daily_recap_text.clear()

        diagnostics = self.services.storage.list_semantic_merge_diagnostics(view.day, decision="merge", limit=1000)
        self.semantic_status_label.setText(f"Semantic coalescing: {len(diagnostics)} merge(s) for this day")

        self._clear_summary_cards()
        self.no_data_label.setVisible(len(view.cards) == 0)

        for card in view.cards:
            self.summary_cards_layout.addWidget(
                _build_summary_card_widget(
                    card,
                    highlight_query=None,
                    on_inspect=(lambda source_ids: self._open_semantic_diagnostics(source_summary_ids=source_ids)),
                )
            )
        self.summary_cards_layout.addStretch(1)

        self.generate_recap_button.setEnabled(not self._daily_recap_inflight and len(view.cards) > 0)
        self._sync_flush_button_state()

    def _clear_summary_cards(self) -> None:
        while self.summary_cards_layout.count():
            item = self.summary_cards_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()

    def _build_refresh_signature(self, day: date) -> tuple[object, ...]:
        summary_days = tuple(self.services.storage.list_summary_days(limit=3660))
        use_coalesced = bool(self.services.config.semantic_coalescing_enabled)
        summaries = self.services.storage.list_effective_summaries_for_day(day, use_coalesced=use_coalesced)
        daily_summary = self.services.storage.get_daily_summary_for_day(day)
        daily_signature = (
            None
            if daily_summary is None
            else (daily_summary.id, daily_summary.created_ts, daily_summary.source_batch_count)
        )
        summary_signature = tuple((item.id, item.start_ts, item.end_ts, item.created_ts) for item in summaries)
        return (day, summary_days, summary_signature, daily_signature)

    def _maybe_refresh(self) -> None:
        if not self.isVisible():
            return
        selected_day = self._selected_day
        signature = self._build_refresh_signature(selected_day)
        if signature != self._last_refresh_signature:
            self.refresh()
        else:
            self._sync_flush_button_state()

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
            try:
                self.services.generate_daily_recap(selected_day)
            except Exception as exc:
                self.logger.exception(
                    "event=daily_recap_generation_failed day=%s failed_stage=%s error_type=%s error=%s",
                    selected_day.isoformat(),
                    get_failed_stage(exc, default="unknown"),
                    exc.__class__.__name__,
                    exc,
                )
            finally:
                self.daily_recap_finished.emit(selected_day, None)

        threading.Thread(target=task, name="DailyRecapGeneration", daemon=True).start()

    def _on_daily_recap_finished(self, day: date, error_message: str | None) -> None:
        self._daily_recap_inflight = False
        self.generate_recap_button.setText("Generate Daily Recap")
        self._refresh_calendar_highlights()
        if self._selected_day == day:
            self._load_day(day)
        else:
            self.generate_recap_button.setEnabled(True)
        self._sync_flush_button_state()

    def _toggle_flush(self) -> None:
        if self._flush_inflight or self._is_flush_drain_active():
            if self.services.cancel_flush_drain():
                self._sync_flush_button_state()
            return

        self._flush_inflight = True
        self._sync_flush_button_state()

        def task() -> None:
            result = None
            try:
                result = self.services.flush_now(reason="summary-window")
            except Exception as exc:
                self.logger.exception("event=summary_window_flush_failed error=%s", exc)
            finally:
                self.flush_finished.emit(result, None)

        threading.Thread(target=task, name="SummaryWindowFlush", daemon=True).start()

    def _on_flush_finished(self, result: object, error_message: object) -> None:
        self._flush_inflight = False
        if hasattr(result, "stop_reason") and getattr(result, "stop_reason") == "error":
            self.logger.info("event=summary_window_flush_finished result=error")
        elif result is not None:
            self.logger.info("event=summary_window_flush_finished result=%s", type(result).__name__)
        self._sync_flush_button_state()
        self.refresh()

    def _sync_flush_button_state(self) -> None:
        active = self._flush_inflight or self._is_flush_drain_active()
        self.flush_button.setText("Stop Flush" if active else "Start Flush")
        self.flush_button.setEnabled(True)

    def _is_flush_drain_active(self) -> bool:
        if self.diagnostics_service is not None:
            return bool(self.diagnostics_service.get_health_snapshot()["flush_drain_active"])
        return bool(getattr(self.services, "is_drain_active", False))

    def _run_search(self) -> None:
        query = self.search_input.text().strip()
        if not query:
            self._clear_search()
            return

        scope = self.search_scope_combo.currentData()
        if not isinstance(scope, SummarySearchScope):
            scope = SummarySearchScope.DAY
        anchor_day = _qdate_to_day(self.search_anchor_date.date())
        params = SummarySearchParams(query=query, scope=scope, anchor_day=anchor_day)
        results = self.search_service.search(params)
        self._render_search_results(results=results, query=query, scope=scope, anchor_day=anchor_day)

    def _clear_search(self) -> None:
        self.search_input.clear()
        self.refresh()

    def _render_search_results(
        self,
        *,
        results: list[SummarySearchResult],
        query: str,
        scope: SummarySearchScope,
        anchor_day: date,
    ) -> None:
        self._clear_summary_cards()
        self.daily_recap_status_label.setText("Daily recap: hidden while searching")
        self.daily_recap_text.clear()
        scope_label = self.search_scope_combo.currentText()
        self.selected_date_label.setText(f"Search results ({scope_label.lower()})")
        self.no_data_label.setVisible(False)
        if not results:
            self.search_status_label.setText(f'No matches for "{query}" in {scope_label.lower()} scope.')
            self.summary_cards_layout.addWidget(QLabel("No summaries matched your query."))
            self.summary_cards_layout.addStretch(1)
            return

        self.search_status_label.setText(
            f'{len(results)} match(es) for "{query}" in {scope_label.lower()} scope anchored at {anchor_day.isoformat()}.'
        )
        for item in results:
            summary_type = "event" if item.summary_type == SummarySearchType.EVENT else "day"
            if summary_type == "event":
                timestamp_label = f"Event day: {item.day.isoformat()}"
            else:
                timestamp_label = f"Daily recap day: {item.day.isoformat()}"
            card = SummaryCardView(
                summary_id=item.source_id,
                time_range=timestamp_label,
                summary_text=item.text,
                major_activities=[f"Type: {summary_type}"],
                blocked_notes=[],
                uncertainty_notes=[],
                is_coalesced=False,
                coalesced_member_count=0,
            )
            self.summary_cards_layout.addWidget(_build_summary_card_widget(card, highlight_query=query))
        self.summary_cards_layout.addStretch(1)

    def _sync_search_anchor_visibility(self) -> None:
        scope = self.search_scope_combo.currentData()
        self.search_anchor_date.setVisible(scope != SummarySearchScope.ALL)
        self.search_anchor_label.setVisible(scope != SummarySearchScope.ALL)
        self._sync_search_anchor_label()

    def _sync_search_anchor_label(self, *_: object) -> None:
        scope = self.search_scope_combo.currentData()
        anchor_day = _qdate_to_day(self.search_anchor_date.date())
        if scope == SummarySearchScope.MONTH:
            self.search_anchor_label.setText(f"Anchor month: {anchor_day.strftime('%Y-%m')}")
        elif scope == SummarySearchScope.YEAR:
            self.search_anchor_label.setText(f"Anchor year: {anchor_day.strftime('%Y')}")
        elif scope == SummarySearchScope.DAY:
            self.search_anchor_label.setText(f"Anchor day: {anchor_day.isoformat()}")
        else:
            self.search_anchor_label.setText("")

    def _open_semantic_diagnostics(self, *, source_summary_ids: list[int] | None = None) -> None:
        selected_day = self._selected_day
        dialog = QDialog(self)
        dialog.setWindowTitle(f"Semantic diagnostics — {selected_day.isoformat()}")
        dialog.resize(1040, 540)
        layout = QVBoxLayout(dialog)
        header = QLabel("Read-only semantic coalescing diagnostics.", dialog)
        header.setWordWrap(True)
        layout.addWidget(header)

        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Day:", dialog))
        day_edit = QDateEdit(dialog)
        day_edit.setCalendarPopup(True)
        day_edit.setDate(_day_to_qdate(selected_day))
        filter_row.addWidget(day_edit)

        filter_row.addWidget(QLabel("Decision:", dialog))
        decision_combo = QComboBox(dialog)
        decision_combo.addItem("All", "")
        decision_combo.addItem("Merge", "merge")
        decision_combo.addItem("No merge", "no_merge")
        filter_row.addWidget(decision_combo)

        filter_row.addWidget(QLabel("Contains:", dialog))
        text_filter = QLineEdit(dialog)
        text_filter.setPlaceholderText("blocker/reason text...")
        filter_row.addWidget(text_filter, 1)
        low_score_only = QCheckBox("Low score merges only", dialog)
        filter_row.addWidget(low_score_only)

        apply_button = QPushButton("Apply", dialog)
        filter_row.addWidget(apply_button)
        layout.addLayout(filter_row)

        table = QTableWidget(dialog)
        table.setColumnCount(9)
        table.setHorizontalHeaderLabels(
            [
                "Pair",
                "Decision",
                "Score",
                "Cosine",
                "App",
                "Window",
                "Gap(s)",
                "Blockers",
                "Reasons",
            ]
        )
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        layout.addWidget(table)

        footer = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, dialog)
        footer.rejected.connect(dialog.reject)
        footer.accepted.connect(dialog.accept)
        layout.addWidget(footer)

        def reload_rows() -> None:
            target_day = _qdate_to_day(day_edit.date())
            decision_value = decision_combo.currentData()
            decision = decision_value if isinstance(decision_value, str) and decision_value else None
            query = text_filter.text().strip() or None
            diagnostics = self.services.storage.list_semantic_merge_diagnostics(
                target_day,
                decision=decision,
                text_query=query,
                summary_ids=source_summary_ids,
                max_merge_score=(self.services.config.semantic_min_merge_score if low_score_only.isChecked() else None),
                limit=500,
            )
            rows = build_semantic_diagnostics_rows(diagnostics)
            table.setRowCount(len(rows))
            for row_idx, row in enumerate(rows):
                table.setItem(row_idx, 0, QTableWidgetItem(row.pair_label))
                table.setItem(row_idx, 1, QTableWidgetItem(row.decision))
                table.setItem(row_idx, 2, QTableWidgetItem(row.score_label))
                table.setItem(row_idx, 3, QTableWidgetItem(row.cosine_label))
                table.setItem(row_idx, 4, QTableWidgetItem(row.app_label))
                table.setItem(row_idx, 5, QTableWidgetItem(row.window_label))
                table.setItem(row_idx, 6, QTableWidgetItem(row.gap_label))
                table.setItem(row_idx, 7, QTableWidgetItem(row.blockers_label))
                table.setItem(row_idx, 8, QTableWidgetItem(row.reasons_label))
            table.resizeColumnsToContents()
            header.setText(f"Read-only semantic coalescing diagnostics ({len(rows)} row(s)).")

        apply_button.clicked.connect(reload_rows)
        text_filter.returnPressed.connect(reload_rows)
        low_score_only.toggled.connect(reload_rows)
        table.setSortingEnabled(True)
        reload_rows()
        dialog.exec()

    def _export_audit_bundle(self) -> None:
        selected_dir = QFileDialog.getExistingDirectory(self, "Select export destination")
        if not selected_dir:
            return
        try:
            result = export_audit_bundle(
                self.services.storage,
                selected_dir,
                AuditExportOptions(),
                config=self.services.config,
            )
        except AuditExportError as exc:
            self.logger.error("event=audit_export_ui_failed error=%s", exc)
            QMessageBox.warning(
                self,
                "Audit export failed",
                f"Could not export summary and merge diagnostics audit bundle.\n\n{exc}",
            )
            return

        counts = result.counts
        QMessageBox.information(
            self,
            "Audit export complete",
            (
                "Audit bundle exported successfully.\n\n"
                "Scope: summaries and semantic merge diagnostics (no raw activity data).\n"
                f"Folder: {result.output_dir}\n"
                f"Summaries: {counts.get('summaries.jsonl', 0)}\n"
                f"Daily summaries: {counts.get('daily_summaries.jsonl', 0)}\n"
                f"Coalesced summaries: {counts.get('coalesced_summaries.jsonl', 0)}\n"
                f"Merge diagnostics: {counts.get('merge_diagnostics.jsonl', 0)}"
            ),
        )


def _build_summary_card_widget(
    card: SummaryCardView,
    highlight_query: str | None,
    on_inspect: Callable[[list[int]], None] | None = None,
) -> QFrame:
    frame = QFrame()
    frame.setFrameShape(QFrame.Shape.StyledPanel)
    frame.setFrameShadow(QFrame.Shadow.Raised)

    layout = QVBoxLayout(frame)
    layout.setContentsMargins(10, 10, 10, 10)

    header_text = f"Time range: {card.time_range}"
    if card.is_coalesced:
        count = max(card.coalesced_member_count, 2)
        confidence = f" | {card.confidence_bucket}" if card.confidence_bucket else ""
        header_text = f"{header_text}   [Coalesced ×{count}{confidence}]"
    layout.addWidget(QLabel(header_text))
    if card.is_coalesced and card.coalesced_source_ids and on_inspect is not None:
        inspect_button = QPushButton("Inspect merge diagnostics")
        inspect_button.clicked.connect(lambda *_: on_inspect(card.coalesced_source_ids or []))
        layout.addWidget(inspect_button)
    summary_label = QLabel()
    summary_label.setWordWrap(True)
    summary_label.setText(format_summary_html(card.summary_text or "(empty)", highlight_query))
    layout.addWidget(summary_label)

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
