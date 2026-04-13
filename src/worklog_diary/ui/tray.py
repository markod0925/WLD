from __future__ import annotations

import sys
import threading
from datetime import datetime

from PySide6.QtCore import QObject, QTimer, Qt, Signal
from PySide6.QtGui import QColor, QIcon, QPainter, QPixmap
from PySide6.QtWidgets import QApplication, QMenu, QMessageBox, QSystemTrayIcon

from ..core.services import MonitoringServices
from .settings_window import SettingsWindow
from .summaries_window import SummariesWindow


class NotificationBridge(QObject):
    user_error = Signal(str, str)


class TrayController:
    def __init__(self, app: QApplication, services: MonitoringServices) -> None:
        self.app = app
        self.services = services
        self._notification_bridge = NotificationBridge()
        self._notification_bridge.user_error.connect(self._show_user_error)
        self.services.set_error_notification_sink(self._notification_bridge.user_error.emit)

        self._tray_icons = {
            "green": _build_tray_icon(QColor("#2ecc71")),
            "yellow": _build_tray_icon(QColor("#f1c40f")),
            "red": _build_tray_icon(QColor("#e74c3c")),
        }

        self.tray = QSystemTrayIcon(self._tray_icons["red"], self.app)
        self.tray.setToolTip("WorkLog Diary")
        self.tray.activated.connect(self._on_tray_activated)

        self.settings_window = SettingsWindow(services)
        self.summaries_window = SummariesWindow(services)

        self.menu = QMenu()
        self.status_action = self.menu.addAction("Status: idle")
        self.status_action.setEnabled(False)
        self.buffer_action = self.menu.addAction("Buffer: unknown")
        self.buffer_action.setEnabled(False)
        self.jobs_action = self.menu.addAction("Summary jobs: queued=0 running=0")
        self.jobs_action.setEnabled(False)
        self.pending_action = self.menu.addAction("Pending: text=0 screenshots=0 summary_jobs=0")
        self.pending_action.setEnabled(False)
        self.menu.addSeparator()

        action_start = self.menu.addAction("Start Monitoring")
        action_start.triggered.connect(self._start)

        action_pause = self.menu.addAction("Pause Monitoring")
        action_pause.triggered.connect(self._pause)

        action_stop = self.menu.addAction("Stop Monitoring")
        action_stop.triggered.connect(self._stop)

        self.menu.addSeparator()

        action_flush = self.menu.addAction("Flush Now (Drain)")
        action_flush.triggered.connect(self._flush)

        action_stop_flush = self.menu.addAction("Stop Flush Drain")
        action_stop_flush.triggered.connect(self._stop_flush_drain)

        action_diagnostics = self.menu.addAction("Diagnostics Snapshot")
        action_diagnostics.triggered.connect(self._show_diagnostics)

        action_settings = self.menu.addAction("Settings")
        action_settings.triggered.connect(self._open_settings)

        action_summaries = self.menu.addAction("Summaries")
        action_summaries.triggered.connect(self._open_summaries)

        self.menu.addSeparator()

        action_exit = self.menu.addAction("Exit")
        action_exit.triggered.connect(self._exit)

        self.tray.setContextMenu(self.menu)

        self.status_timer = QTimer()
        self.status_timer.timeout.connect(self._refresh_status)

    def show(self) -> None:
        self.tray.show()
        self.status_timer.start(1000)
        self._refresh_status()

    def _start(self) -> None:
        self.services.start_monitoring()
        self._refresh_status()

    def _pause(self) -> None:
        self.services.pause_monitoring()
        self._refresh_status()

    def _stop(self) -> None:
        self.services.stop_monitoring()
        self._refresh_status()

    def _flush(self) -> None:
        def task() -> None:
            result = self.services.flush_now(reason="manual")
            if result is None:
                message = "Flush drain already running."
            elif result.stop_reason == "error":
                message = None
            else:
                message = (
                    "Drain finished: "
                    f"stop={result.stop_reason}, created={result.summaries_created}, "
                    f"failed={result.failed_jobs}, cancelled={result.cancelled_jobs}."
                )
            QTimer.singleShot(
                0,
                lambda: self._on_flush_finished(message),
            )

        threading.Thread(target=task, name="ManualFlushDrain", daemon=True).start()

    def _stop_flush_drain(self) -> None:
        stopped = self.services.cancel_flush_drain()
        message = "Drain cancel requested." if stopped else "No active drain to cancel."
        self.tray.showMessage("WorkLog Diary", message)
        self._refresh_status()

    def _on_flush_finished(self, message: str | None) -> None:
        if message:
            self.tray.showMessage("WorkLog Diary", message)
        self._refresh_status()

    def _show_user_error(self, category: str, message: str) -> None:
        titles = {
            "lmstudio_connection": "Connection error",
            "lmstudio_service_unavailable": "Service unavailable",
            "summary_generation_failure": "Summary generation failed",
            "flush_failure": "Flush failed",
        }
        title = titles.get(category, "WorkLog Diary")
        self.tray.showMessage(title, message, QSystemTrayIcon.MessageIcon.Warning, 5000)

    def _show_diagnostics(self) -> None:
        diagnostics = self.services.storage.get_diagnostics_snapshot()
        pending = diagnostics["pending_counts"]
        jobs = diagnostics["summary_jobs"]
        ranges = diagnostics["pending_ranges"]

        def _format_range(value: object) -> str:
            if not isinstance(value, dict):
                return "-"
            return f"count={value['count']} start={value['start_ts']:.3f} end={value['end_ts']:.3f}"

        body = (
            "Pending counts\n"
            f"- Intervals: {pending['intervals']}\n"
            f"- Key events (unprocessed): {pending['key_events']}\n"
            f"- Key events (processed): {pending['processed_key_events']}\n"
            f"- Text segments: {pending['text_segments']}\n"
            f"- Screenshots: {pending['screenshots']}\n\n"
            "Pending ranges\n"
            f"- Intervals: {_format_range(ranges['active_intervals_unsummarized'])}\n"
            f"- Blocked intervals: {_format_range(ranges['blocked_intervals_unsummarized'])}\n"
            f"- Unprocessed key events: {_format_range(ranges['key_events_unprocessed'])}\n"
            f"- Text segments: {_format_range(ranges['text_segments_pending'])}\n"
            f"- Screenshots: {_format_range(ranges['screenshots_pending'])}\n\n"
            "Summary jobs\n"
            f"- Queued: {jobs['queued']}\n"
            f"- Running: {jobs['running']}\n"
            f"- Failed: {jobs['failed']}\n"
            f"- Succeeded: {jobs['succeeded']}\n"
            f"- Cancelled: {jobs['cancelled']}"
        )
        QMessageBox.information(None, "WorkLog Diagnostics", body)

    def _open_settings(self) -> None:
        self.settings_window.load_from_config()
        self.settings_window.show()
        self.settings_window.raise_()
        self.settings_window.activateWindow()

    def _open_summaries(self) -> None:
        self.summaries_window.refresh()
        self.summaries_window.show()
        self.summaries_window.raise_()
        self.summaries_window.activateWindow()

    def _on_tray_activated(self, reason: QSystemTrayIcon.ActivationReason) -> None:
        if reason in (
            QSystemTrayIcon.ActivationReason.Trigger,
            QSystemTrayIcon.ActivationReason.DoubleClick,
        ):
            self._open_summaries()

    def _exit(self) -> None:
        self.status_timer.stop()
        self.services.shutdown()
        self.tray.hide()
        self.app.quit()

    def _refresh_status(self) -> None:
        status = self.services.get_status()
        monitoring = str(status["monitoring_state"])
        blocked = "yes" if status["blocked"] else "no"
        self.tray.setIcon(self._select_tray_icon(status))

        foreground = status["foreground"]
        if foreground is not None:
            context = f"{foreground.process_name} | {foreground.window_title[:60]}"
        else:
            context = "No active window"

        self.status_action.setText(f"Status: {monitoring} | blocked: {blocked}")
        self.buffer_action.setText(
            f"Buffer: {status['buffer_state']} | approx batches: {status['approx_remaining_batches']}"
        )

        jobs = status["summary_jobs"]
        self.jobs_action.setText(
            "Summary jobs: "
            f"queued={jobs['queued']} running={jobs['running']} completed={jobs['completed']} failed={jobs['failed']}"
        )
        self.pending_action.setText(
            "Pending: "
            f"text={status['pending_text_segment_count']} "
            f"screenshots={status['pending_screenshot_count']} "
            f"summary_jobs={status['pending_summary_job_count']}"
        )

        last_flush = "-"
        if status["last_flush_ts"] is not None:
            last_flush = datetime.fromtimestamp(status["last_flush_ts"]).strftime("%H:%M:%S")

        next_flush = "-"
        if status["next_flush_ts"] is not None:
            next_flush = datetime.fromtimestamp(status["next_flush_ts"]).strftime("%H:%M:%S")

        pending = status["pending"]
        drain_state = "active" if status["flush_drain_active"] else "idle"
        tooltip = (
            "WorkLog Diary\n"
            f"Monitoring: {monitoring}\n"
            f"Blocked: {blocked}\n"
            f"Context: {context}\n"
            f"Buffer state: {status['buffer_state']}\n"
            f"Approx remaining batches: {status['approx_remaining_batches']}\n"
            f"Flush drain: {drain_state}\n"
            f"Last flush: {last_flush}\n"
            f"Next flush: {next_flush}\n"
            f"Pending intervals: {pending['intervals']}\n"
            f"Pending keys: {pending['key_events']}\n"
            f"Pending text: {pending['text_segments']}\n"
            f"Pending screenshots: {pending['screenshots']}\n"
            f"Summary jobs queued/running/completed/failed: "
            f"{jobs['queued']}/{jobs['running']}/{jobs['completed']}/{jobs['failed']}"
        )
        self.tray.setToolTip(tooltip)

    def _select_tray_icon(self, status: dict) -> QIcon:
        if not bool(status["monitoring_active"]):
            return self._tray_icons["yellow"] if bool(status["monitoring_requested"]) else self._tray_icons["red"]
        if bool(status["blocked"]):
            return self._tray_icons["red"]
        return self._tray_icons["green"]



def run_tray_app(services: MonitoringServices) -> int:
    app = QApplication.instance() or QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(False)

    controller = TrayController(app, services)
    controller.show()

    if services.config.start_monitoring_on_launch:
        services.start_monitoring()

    return app.exec()


def _build_tray_icon(color: QColor) -> QIcon:
    pixmap = QPixmap(64, 64)
    pixmap.fill(Qt.GlobalColor.transparent)

    painter = QPainter(pixmap)
    try:
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(color)
        margin = 10
        painter.drawEllipse(margin, margin, 64 - margin * 2, 64 - margin * 2)
    finally:
        painter.end()
    return QIcon(pixmap)
