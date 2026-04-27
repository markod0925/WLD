from __future__ import annotations

import sys
import threading
from collections.abc import Callable

from PySide6.QtCore import QObject, QTimer, Qt, Signal
from PySide6.QtGui import QColor, QIcon, QPainter, QPixmap
from PySide6.QtWidgets import QApplication, QMenu, QSystemTrayIcon

from ..core.services import MonitoringServices
from ..core.monitoring_components import DiagnosticsService
from ..resources import app_logo_path
from .settings_window import SettingsWindow
from .summaries_window import SummariesWindow
from .tray_status_view_model import build_tray_menu_actions, build_tray_status_snapshot, format_tray_tooltip


class NotificationBridge(QObject):
    user_error = Signal(str, str)


class TrayController:
    def __init__(
        self,
        app: QApplication,
        services: MonitoringServices,
        diagnostics_service: DiagnosticsService | None = None,
    ) -> None:
        self.app = app
        self.services = services
        self.diagnostics_service = diagnostics_service
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
        window_icon = self.app.windowIcon()
        self.settings_window.setWindowIcon(window_icon)
        self.summaries_window.setWindowIcon(window_icon)

        self.menu = QMenu()
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

    def _open_search_summaries(self) -> None:
        self._open_summaries()
        self.summaries_window.search_input.setFocus()
        self.summaries_window.search_input.selectAll()

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
        if self.diagnostics_service is not None:
            status = self.diagnostics_service.get_status()
        else:
            status = self.services.get_status()
        snapshot = build_tray_status_snapshot(status)
        self.tray.setIcon(self._select_tray_icon(status))
        self.tray.setToolTip(format_tray_tooltip(snapshot))
        self._rebuild_menu(snapshot)

    def _rebuild_menu(self, snapshot) -> None:
        self.menu.clear()
        for spec in build_tray_menu_actions(snapshot):
            if spec.separator_before and self.menu.actions():
                self.menu.addSeparator()
            action = self.menu.addAction(spec.label)
            action.setEnabled(spec.enabled)
            action.triggered.connect(self._menu_callback_for(spec.command))

    def _menu_callback_for(self, command: str) -> Callable[[], None]:
        callbacks = {
            "show_summaries": self._open_summaries,
            "search_summaries": self._open_search_summaries,
            "start_capture": self._start,
            "resume_capture": self._start,
            "pause_capture": self._pause,
            "flush_now": self._flush,
            "stop_flush_drain": self._stop_flush_drain,
            "settings": self._open_settings,
            "quit": self._exit,
        }
        callback = callbacks.get(command)
        if callback is None:
            return lambda: None
        return callback

    def _select_tray_icon(self, status: dict) -> QIcon:
        if not bool(status["monitoring_active"]):
            return self._tray_icons["yellow"] if bool(status["monitoring_requested"]) else self._tray_icons["red"]
        if bool(status["blocked"]):
            return self._tray_icons["red"]
        return self._tray_icons["green"]



def run_tray_app(services: MonitoringServices) -> int:
    app = QApplication.instance() or QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(False)
    app.setWindowIcon(QIcon(str(app_logo_path())))

    controller = TrayController(app, services, services.diagnostics_service)
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
