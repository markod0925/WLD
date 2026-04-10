from __future__ import annotations

import sys
import threading
from datetime import datetime

from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QApplication, QMenu, QMessageBox, QStyle, QSystemTrayIcon

from ..core.services import MonitoringServices
from .settings_window import SettingsWindow
from .summaries_window import SummariesWindow


class TrayController:
    def __init__(self, app: QApplication, services: MonitoringServices) -> None:
        self.app = app
        self.services = services

        icon = self.app.style().standardIcon(QStyle.SP_ComputerIcon)
        self.tray = QSystemTrayIcon(icon, self.app)
        self.tray.setToolTip("WorkLog Diary")

        self.settings_window = SettingsWindow(services)
        self.summaries_window = SummariesWindow(services)

        self.menu = QMenu()
        self.status_action = self.menu.addAction("Status: idle")
        self.status_action.setEnabled(False)
        self.menu.addSeparator()

        action_start = self.menu.addAction("Start Monitoring")
        action_start.triggered.connect(self._start)

        action_pause = self.menu.addAction("Pause Monitoring")
        action_pause.triggered.connect(self._pause)

        action_stop = self.menu.addAction("Stop Monitoring")
        action_stop.triggered.connect(self._stop)

        self.menu.addSeparator()

        action_flush = self.menu.addAction("Flush Now")
        action_flush.triggered.connect(self._flush)

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
            summary_id = self.services.flush_now(reason="manual")
            message = (
                "Flush completed: no pending data or flush already in progress."
                if summary_id is None
                else f"Flush completed: summary #{summary_id} created."
            )
            QTimer.singleShot(
                0,
                lambda: self._on_flush_finished(message),
            )

        threading.Thread(target=task, name="ManualFlush", daemon=True).start()

    def _on_flush_finished(self, message: str) -> None:
        self.tray.showMessage("WorkLog Diary", message)
        self._refresh_status()

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
            f"- Running: {jobs['running']}\n"
            f"- Failed: {jobs['failed']}\n"
            f"- Succeeded: {jobs['succeeded']}"
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

    def _exit(self) -> None:
        self.status_timer.stop()
        self.services.shutdown()
        self.tray.hide()
        self.app.quit()

    def _refresh_status(self) -> None:
        status = self.services.get_status()
        monitoring = "running" if status["monitoring_active"] else "paused"
        blocked = "yes" if status["blocked"] else "no"

        foreground = status["foreground"]
        if foreground is not None:
            context = f"{foreground.process_name} | {foreground.window_title[:60]}"
        else:
            context = "No active window"

        self.status_action.setText(f"Status: {monitoring} | blocked: {blocked}")

        last_flush = "-"
        if status["last_flush_ts"] is not None:
            last_flush = datetime.fromtimestamp(status["last_flush_ts"]).strftime("%H:%M:%S")

        next_flush = "-"
        if status["next_flush_ts"] is not None:
            next_flush = datetime.fromtimestamp(status["next_flush_ts"]).strftime("%H:%M:%S")

        pending = status["pending"]
        tooltip = (
            "WorkLog Diary\n"
            f"Monitoring: {monitoring}\n"
            f"Blocked: {blocked}\n"
            f"Context: {context}\n"
            f"Last flush: {last_flush}\n"
            f"Next flush: {next_flush}\n"
            f"Pending intervals: {pending['intervals']}\n"
            f"Pending keys: {pending['key_events']}\n"
            f"Pending text: {pending['text_segments']}\n"
            f"Pending screenshots: {pending['screenshots']}"
        )
        self.tray.setToolTip(tooltip)



def run_tray_app(services: MonitoringServices) -> int:
    app = QApplication.instance() or QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(False)

    controller = TrayController(app, services)
    controller.show()

    if services.config.start_monitoring_on_launch:
        services.start_monitoring()

    return app.exec()
