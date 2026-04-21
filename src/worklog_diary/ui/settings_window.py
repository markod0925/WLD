from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QDoubleSpinBox,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from ..core.config import AppConfig
from ..core.services import MonitoringServices
from .settings_metadata import EDITABLE_SETTINGS, READONLY_SETTINGS, UI_SETTINGS_BY_KEY


class SettingsWindow(QWidget):
    def __init__(self, services: MonitoringServices) -> None:
        super().__init__()
        self.services = services
        self.setWindowTitle("WorkLog Diary Settings")
        self.resize(560, 600)

        layout = QVBoxLayout(self)
        form = QFormLayout()
        layout.addLayout(form)

        self.blocked_processes = QPlainTextEdit()
        self.blocked_processes.setPlaceholderText("chrome.exe\nmsedge.exe\nwebex.exe")
        form.addRow(self._label_with_info("blocked_processes"), self.blocked_processes)

        self.screenshot_interval = QSpinBox()
        self.screenshot_interval.setRange(5, 3600)
        form.addRow(self._label_with_info("screenshot_interval_seconds"), self.screenshot_interval)

        self.capture_mode = QComboBox()
        self.capture_mode.addItems(["full_screen", "active_window"])
        form.addRow(self._label_with_info("capture_mode"), self.capture_mode)

        self.foreground_poll_interval = QDoubleSpinBox()
        self.foreground_poll_interval.setRange(0.2, 5.0)
        self.foreground_poll_interval.setSingleStep(0.1)
        form.addRow(self._label_with_info("foreground_poll_interval_seconds"), self.foreground_poll_interval)

        self.text_gap_interval = QDoubleSpinBox()
        self.text_gap_interval.setRange(1.0, 60.0)
        self.text_gap_interval.setSingleStep(0.5)
        form.addRow(self._label_with_info("text_inactivity_gap_seconds"), self.text_gap_interval)

        self.reconstruction_interval = QDoubleSpinBox()
        self.reconstruction_interval.setRange(0.5, 10.0)
        self.reconstruction_interval.setSingleStep(0.5)
        form.addRow(self._label_with_info("reconstruction_poll_interval_seconds"), self.reconstruction_interval)

        self.flush_interval = QSpinBox()
        self.flush_interval.setRange(30, 7200)
        form.addRow(self._label_with_info("flush_interval_seconds"), self.flush_interval)

        self.max_parallel_summary_jobs = QSpinBox()
        self.max_parallel_summary_jobs.setRange(1, 16)
        form.addRow(self._label_with_info("max_parallel_summary_jobs"), self.max_parallel_summary_jobs)

        self.max_screenshots = QSpinBox()
        self.max_screenshots.setRange(0, 10)
        form.addRow(self._label_with_info("max_screenshots_per_summary"), self.max_screenshots)

        self.max_text_segments = QSpinBox()
        self.max_text_segments.setRange(10, 2000)
        form.addRow(self._label_with_info("max_text_segments_per_summary"), self.max_text_segments)

        self.base_url = QLineEdit()
        form.addRow(self._label_with_info("lmstudio_base_url"), self.base_url)

        self.model_name = QLineEdit()
        form.addRow(self._label_with_info("lmstudio_model"), self.model_name)

        self.timeout = QSpinBox()
        self.timeout.setRange(5, 600)
        form.addRow(self._label_with_info("request_timeout_seconds"), self.timeout)

        self.data_dir_label = QLabel()
        self.data_dir_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        form.addRow(self._label_with_info("app_data_dir"), self.data_dir_label)

        self.db_path_label = QLabel()
        self.db_path_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        form.addRow(self._label_with_info("db_path"), self.db_path_label)

        self.log_dir_label = QLabel()
        self.log_dir_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        form.addRow(self._label_with_info("log_dir"), self.log_dir_label)

        self.screenshot_dir_label = QLabel()
        self.screenshot_dir_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        form.addRow(self._label_with_info("screenshot_dir"), self.screenshot_dir_label)

        actions = QHBoxLayout()
        layout.addLayout(actions)

        self.status_label = QLabel("")
        layout.addWidget(self.status_label)

        save_button = QPushButton("Save")
        save_button.clicked.connect(self._save)
        actions.addWidget(save_button)

        reload_button = QPushButton("Reload")
        reload_button.clicked.connect(self.load_from_config)
        actions.addWidget(reload_button)

        actions.addStretch(1)

        self._editable_widgets: dict[str, QWidget] = {
            "blocked_processes": self.blocked_processes,
            "screenshot_interval_seconds": self.screenshot_interval,
            "capture_mode": self.capture_mode,
            "foreground_poll_interval_seconds": self.foreground_poll_interval,
            "text_inactivity_gap_seconds": self.text_gap_interval,
            "reconstruction_poll_interval_seconds": self.reconstruction_interval,
            "flush_interval_seconds": self.flush_interval,
            "max_parallel_summary_jobs": self.max_parallel_summary_jobs,
            "max_screenshots_per_summary": self.max_screenshots,
            "max_text_segments_per_summary": self.max_text_segments,
            "lmstudio_base_url": self.base_url,
            "lmstudio_model": self.model_name,
            "request_timeout_seconds": self.timeout,
        }
        self._readonly_labels: dict[str, QLabel] = {
            "app_data_dir": self.data_dir_label,
            "db_path": self.db_path_label,
            "log_dir": self.log_dir_label,
            "screenshot_dir": self.screenshot_dir_label,
        }

        self.load_from_config()

    def _label_with_info(self, config_key: str) -> QWidget:
        metadata = UI_SETTINGS_BY_KEY[config_key]
        container = QWidget(self)
        row = QHBoxLayout(container)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(6)

        label = QLabel(metadata.label, container)
        row.addWidget(label)

        info = QLabel("ⓘ", container)
        info.setToolTip(metadata.tooltip)
        info.setCursor(Qt.WhatsThisCursor)
        info.setTextInteractionFlags(Qt.NoTextInteraction)
        row.addWidget(info)
        row.addStretch(1)
        return container

    def load_from_config(self) -> None:
        cfg = self.services.config

        for setting in EDITABLE_SETTINGS:
            value = getattr(cfg, setting.key)
            widget = self._editable_widgets[setting.key]
            if setting.key == "blocked_processes":
                assert isinstance(widget, QPlainTextEdit)
                widget.setPlainText("\n".join(value))
            elif isinstance(widget, QSpinBox):
                widget.setValue(int(value))
            elif isinstance(widget, QDoubleSpinBox):
                widget.setValue(float(value))
            elif isinstance(widget, QComboBox):
                widget.setCurrentText(str(value))
            elif isinstance(widget, QLineEdit):
                widget.setText(str(value))

        for setting in READONLY_SETTINGS:
            self._readonly_labels[setting.key].setText(str(getattr(cfg, setting.key)))

        self.status_label.setText("")

    def _save(self) -> None:
        raw_blocked = self.blocked_processes.toPlainText().replace(",", "\n")
        blocked = [item.strip().lower() for item in raw_blocked.splitlines() if item.strip()]

        if not blocked:
            QMessageBox.warning(self, "Invalid config", "Blocked process list cannot be empty.")
            return

        data = self.services.config.to_dict()
        cfg = AppConfig.from_dict(data)

        for setting in EDITABLE_SETTINGS:
            widget = self._editable_widgets[setting.key]
            if setting.key == "blocked_processes":
                value = blocked
            elif isinstance(widget, QSpinBox):
                value = int(widget.value())
            elif isinstance(widget, QDoubleSpinBox):
                value = float(widget.value())
            elif isinstance(widget, QComboBox):
                value = widget.currentText().strip().lower()
            elif isinstance(widget, QLineEdit):
                value = widget.text().strip()
            else:
                continue
            setattr(cfg, setting.key, value)

        self.services.apply_config(cfg)
        self.status_label.setText("Saved")
