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
        form.addRow("Blocked processes:", self.blocked_processes)

        self.screenshot_interval = QSpinBox()
        self.screenshot_interval.setRange(5, 3600)
        form.addRow("Screenshot interval (s):", self.screenshot_interval)

        self.capture_mode = QComboBox()
        self.capture_mode.addItems(["full_screen", "active_window"])
        form.addRow("Screenshot capture mode:", self.capture_mode)

        self.foreground_poll_interval = QDoubleSpinBox()
        self.foreground_poll_interval.setRange(0.2, 5.0)
        self.foreground_poll_interval.setSingleStep(0.1)
        form.addRow("Foreground poll interval (s):", self.foreground_poll_interval)

        self.text_gap_interval = QDoubleSpinBox()
        self.text_gap_interval.setRange(1.0, 60.0)
        self.text_gap_interval.setSingleStep(0.5)
        form.addRow("Text inactivity gap (s):", self.text_gap_interval)

        self.reconstruction_interval = QDoubleSpinBox()
        self.reconstruction_interval.setRange(0.5, 10.0)
        self.reconstruction_interval.setSingleStep(0.5)
        form.addRow("Text reconstruction poll (s):", self.reconstruction_interval)

        self.flush_interval = QSpinBox()
        self.flush_interval.setRange(30, 7200)
        form.addRow("Flush interval (s):", self.flush_interval)

        self.max_parallel_summary_jobs = QSpinBox()
        self.max_parallel_summary_jobs.setRange(1, 16)
        form.addRow("Max parallel summary jobs:", self.max_parallel_summary_jobs)

        self.max_screenshots = QSpinBox()
        self.max_screenshots.setRange(0, 10)
        form.addRow("Max screenshots per summary:", self.max_screenshots)

        self.max_text_segments = QSpinBox()
        self.max_text_segments.setRange(10, 2000)
        form.addRow("Max text segments per summary:", self.max_text_segments)

        self.base_url = QLineEdit()
        form.addRow("LM Studio base URL:", self.base_url)

        self.model_name = QLineEdit()
        form.addRow("LM Studio model:", self.model_name)

        self.timeout = QSpinBox()
        self.timeout.setRange(5, 600)
        form.addRow("LM request timeout (s):", self.timeout)

        self.data_dir_label = QLabel()
        self.data_dir_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        form.addRow("Data directory:", self.data_dir_label)

        self.db_path_label = QLabel()
        self.db_path_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        form.addRow("SQLite database:", self.db_path_label)

        self.log_dir_label = QLabel()
        self.log_dir_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        form.addRow("Logs folder:", self.log_dir_label)

        self.screenshot_dir_label = QLabel()
        self.screenshot_dir_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        form.addRow("Screenshot folder:", self.screenshot_dir_label)

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

        self.load_from_config()

    def load_from_config(self) -> None:
        cfg = self.services.config
        self.blocked_processes.setPlainText("\n".join(cfg.blocked_processes))
        self.screenshot_interval.setValue(cfg.screenshot_interval_seconds)
        self.capture_mode.setCurrentText(cfg.capture_mode)
        self.foreground_poll_interval.setValue(cfg.foreground_poll_interval_seconds)
        self.text_gap_interval.setValue(cfg.text_inactivity_gap_seconds)
        self.reconstruction_interval.setValue(cfg.reconstruction_poll_interval_seconds)
        self.flush_interval.setValue(cfg.flush_interval_seconds)
        self.max_parallel_summary_jobs.setValue(cfg.max_parallel_summary_jobs)
        self.max_screenshots.setValue(cfg.max_screenshots_per_summary)
        self.max_text_segments.setValue(cfg.max_text_segments_per_summary)
        self.base_url.setText(cfg.lmstudio_base_url)
        self.model_name.setText(cfg.lmstudio_model)
        self.timeout.setValue(cfg.request_timeout_seconds)

        self.data_dir_label.setText(cfg.app_data_dir)
        self.db_path_label.setText(cfg.db_path)
        self.log_dir_label.setText(cfg.log_dir)
        self.screenshot_dir_label.setText(cfg.screenshot_dir)
        self.status_label.setText("")

    def _save(self) -> None:
        raw_blocked = self.blocked_processes.toPlainText().replace(",", "\n")
        blocked = [item.strip().lower() for item in raw_blocked.splitlines() if item.strip()]

        if not blocked:
            QMessageBox.warning(self, "Invalid config", "Blocked process list cannot be empty.")
            return

        data = self.services.config.to_dict()
        cfg = AppConfig.from_dict(data)
        cfg.blocked_processes = blocked
        cfg.screenshot_interval_seconds = int(self.screenshot_interval.value())
        cfg.capture_mode = self.capture_mode.currentText().strip().lower()
        cfg.foreground_poll_interval_seconds = float(self.foreground_poll_interval.value())
        cfg.text_inactivity_gap_seconds = float(self.text_gap_interval.value())
        cfg.reconstruction_poll_interval_seconds = float(self.reconstruction_interval.value())
        cfg.flush_interval_seconds = int(self.flush_interval.value())
        cfg.max_parallel_summary_jobs = int(self.max_parallel_summary_jobs.value())
        cfg.max_screenshots_per_summary = int(self.max_screenshots.value())
        cfg.max_text_segments_per_summary = int(self.max_text_segments.value())
        cfg.lmstudio_base_url = self.base_url.text().strip()
        cfg.lmstudio_model = self.model_name.text().strip()
        cfg.request_timeout_seconds = int(self.timeout.value())

        self.services.apply_config(cfg)
        self.status_label.setText("Saved")
