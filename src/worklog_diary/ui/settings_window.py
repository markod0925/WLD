from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QScrollArea,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from ..core.config import AppConfig
from ..core.services import MonitoringServices
from .settings_metadata import (
    ADVANCED_SETTINGS,
    DEBUG_SETTINGS,
    DEFAULTS,
    UI_SETTINGS_BY_KEY,
    USER_SETTINGS,
    SettingUiMetadata,
    float_step_decimals,
    modified_debug_keys,
)


class DebugSettingsDialog(QDialog):
    def __init__(self, parent: QWidget, *, widgets: dict[str, QWidget]) -> None:
        super().__init__(parent)
        self.setWindowTitle("Debug Params")
        self.resize(580, 640)
        self._widgets = widgets
        self._row_containers: dict[str, QWidget] = {}

        layout = QVBoxLayout(self)

        warning = QLabel(
            "Experimental parameters. Changing these may affect performance, summary quality, or stability.",
            self,
        )
        warning.setWordWrap(True)
        warning.setStyleSheet("color: #b45309; font-weight: 600;")
        layout.addWidget(warning)
        self._summary_label = QLabel("", self)
        layout.addWidget(self._summary_label)

        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        container = QWidget(scroll)
        self.form = QFormLayout(container)
        self.form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)

        for setting in DEBUG_SETTINGS:
            label = self._label_with_info(setting.key)
            self._row_containers[setting.key] = label
            self.form.addRow(label, self._widgets[setting.key])
            _connect_value_changed(self._widgets[setting.key], self._refresh_modified_state)

        scroll.setWidget(container)
        layout.addWidget(scroll)

        actions = QHBoxLayout()
        restore_button = QPushButton("Restore debug defaults", self)
        restore_button.clicked.connect(self._restore_defaults)
        actions.addWidget(restore_button)
        actions.addStretch(1)
        layout.addLayout(actions)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, parent=self)
        buttons.rejected.connect(self.reject)
        buttons.accepted.connect(self.accept)
        layout.addWidget(buttons)
        self._refresh_modified_state()

    def _label_with_info(self, config_key: str) -> QWidget:
        metadata = UI_SETTINGS_BY_KEY[config_key]
        container = QWidget(self)
        row = QHBoxLayout(container)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(6)

        label = QLabel(_decorated_label(metadata), container)
        row.addWidget(label)

        info = QLabel("ⓘ", container)
        info.setToolTip(metadata.tooltip)
        info.setCursor(Qt.CursorShape.WhatsThisCursor)
        row.addWidget(info)
        row.addStretch(1)
        return container

    def _restore_defaults(self) -> None:
        for setting in DEBUG_SETTINGS:
            _set_widget_value(self._widgets[setting.key], DEFAULTS[setting.key], setting)
        self._refresh_modified_state()

    def _refresh_modified_state(self) -> None:
        current_values = {setting.key: _read_widget_value(self._widgets[setting.key], setting) for setting in DEBUG_SETTINGS}
        modified = set(modified_debug_keys(current_values))
        self._summary_label.setText(
            "Using default debug values" if not modified else f"Modified debug parameters: {len(modified)}"
        )

        for setting in DEBUG_SETTINGS:
            widget = self._widgets[setting.key]
            row = self._row_containers[setting.key]
            if setting.key in modified:
                widget.setStyleSheet("background-color: #fff7ed;")
                row.setStyleSheet("color: #9a3412;")
            else:
                widget.setStyleSheet("")
                row.setStyleSheet("")


class SettingsWindow(QWidget):
    def __init__(self, services: MonitoringServices) -> None:
        super().__init__()
        self.services = services
        self.setWindowTitle("WorkLog Diary Settings")
        self.resize(600, 660)

        layout = QVBoxLayout(self)

        user_group = QGroupBox("Settings", self)
        user_form = QFormLayout(user_group)
        user_form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        layout.addWidget(user_group)

        self._widgets: dict[str, QWidget] = {}

        for setting in USER_SETTINGS:
            widget = _create_widget(setting)
            self._widgets[setting.key] = widget
            user_form.addRow(self._label_with_info(setting.key), widget)

        advanced_group = QGroupBox("Advanced Settings", self)
        advanced_group.setCheckable(True)
        advanced_group.setChecked(False)
        advanced_form = QFormLayout(advanced_group)
        advanced_form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        for setting in ADVANCED_SETTINGS:
            widget = _create_widget(setting)
            self._widgets[setting.key] = widget
            advanced_form.addRow(self._label_with_info(setting.key), widget)
        layout.addWidget(advanced_group)

        self._debug_widgets: dict[str, QWidget] = {}
        for setting in DEBUG_SETTINGS:
            self._debug_widgets[setting.key] = _create_widget(setting)

        actions = QHBoxLayout()
        layout.addLayout(actions)

        self.status_label = QLabel("")
        layout.addWidget(self.status_label)

        debug_button = QPushButton("Debug Params")
        debug_button.clicked.connect(self._open_debug_params)
        actions.addWidget(debug_button)

        save_button = QPushButton("Save")
        save_button.clicked.connect(self._save)
        actions.addWidget(save_button)

        reload_button = QPushButton("Reload")
        reload_button.clicked.connect(self.load_from_config)
        actions.addWidget(reload_button)

        actions.addStretch(1)

        self.load_from_config()

    def _label_with_info(self, config_key: str) -> QWidget:
        metadata = UI_SETTINGS_BY_KEY[config_key]
        container = QWidget(self)
        row = QHBoxLayout(container)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(6)

        label = QLabel(_decorated_label(metadata), container)
        row.addWidget(label)

        info = QLabel("ⓘ", container)
        info.setToolTip(metadata.tooltip)
        info.setCursor(Qt.CursorShape.WhatsThisCursor)
        row.addWidget(info)
        row.addStretch(1)
        return container

    def _open_debug_params(self) -> None:
        dialog = DebugSettingsDialog(self, widgets=self._debug_widgets)
        dialog.exec()

    def load_from_config(self) -> None:
        cfg = self.services.config

        for setting in (*USER_SETTINGS, *ADVANCED_SETTINGS):
            _set_widget_value(self._widgets[setting.key], getattr(cfg, setting.key), setting)

        for setting in DEBUG_SETTINGS:
            _set_widget_value(self._debug_widgets[setting.key], getattr(cfg, setting.key), setting)

        self.status_label.setText("")

    def _save(self) -> None:
        cfg = AppConfig.from_dict(self.services.config.to_dict())

        for setting in (*USER_SETTINGS, *ADVANCED_SETTINGS):
            value = _read_widget_value(self._widgets[setting.key], setting)
            setattr(cfg, setting.key, value)

        for setting in DEBUG_SETTINGS:
            value = _read_widget_value(self._debug_widgets[setting.key], setting)
            setattr(cfg, setting.key, value)

        if not cfg.blocked_processes:
            QMessageBox.warning(self, "Invalid config", "Blocked process list cannot be empty.")
            return

        self.services.apply_config(cfg)
        self.status_label.setText("Saved")


def _create_widget(setting: SettingUiMetadata) -> QWidget:
    if setting.widget == "multiline":
        widget = QPlainTextEdit()
        widget.setPlaceholderText("chrome.exe\\nmsedge.exe\\nwebex.exe")
        return widget
    if setting.widget == "int":
        widget = QSpinBox()
        if setting.min_value is not None and setting.max_value is not None:
            widget.setRange(int(setting.min_value), int(setting.max_value))
        return widget
    if setting.widget == "float":
        widget = QDoubleSpinBox()
        if setting.min_value is not None and setting.max_value is not None:
            widget.setRange(float(setting.min_value), float(setting.max_value))
        step = float(setting.step or 0.1)
        widget.setSingleStep(step)
        widget.setDecimals(float_step_decimals(step))
        return widget
    if setting.widget == "select":
        widget = QComboBox()
        widget.addItems(list(setting.options))
        return widget
    if setting.widget == "text":
        return QLineEdit()
    if setting.widget == "bool":
        return QCheckBox()
    raise ValueError(f"Unsupported widget type for setting {setting.key}: {setting.widget}")


def _set_widget_value(widget: QWidget, value: object, setting: SettingUiMetadata) -> None:
    if setting.widget == "multiline":
        assert isinstance(widget, QPlainTextEdit)
        blocked = [str(item).strip().lower() for item in value if str(item).strip()] if isinstance(value, list) else []
        widget.setPlainText("\n".join(blocked))
        return
    if isinstance(widget, QSpinBox):
        widget.setValue(int(value))
        return
    if isinstance(widget, QDoubleSpinBox):
        widget.setValue(float(value))
        return
    if isinstance(widget, QComboBox):
        widget.setCurrentText(str(value))
        return
    if isinstance(widget, QLineEdit):
        widget.setText(str(value))
        return
    if isinstance(widget, QCheckBox):
        widget.setChecked(bool(value))


def _read_widget_value(widget: QWidget, setting: SettingUiMetadata) -> object:
    if setting.widget == "multiline":
        assert isinstance(widget, QPlainTextEdit)
        raw = widget.toPlainText().replace(",", "\n")
        return [item.strip().lower() for item in raw.splitlines() if item.strip()]
    if isinstance(widget, QSpinBox):
        return int(widget.value())
    if isinstance(widget, QDoubleSpinBox):
        return float(widget.value())
    if isinstance(widget, QComboBox):
        return widget.currentText().strip().lower()
    if isinstance(widget, QLineEdit):
        return widget.text().strip()
    if isinstance(widget, QCheckBox):
        return widget.isChecked()
    raise ValueError(f"Unsupported widget type for setting {setting.key}")


def _decorated_label(setting: SettingUiMetadata) -> str:
    suffixes: list[str] = []
    if setting.requires_restart:
        suffixes.append("restart required")
    if setting.is_experimental:
        suffixes.append("experimental")
    if not suffixes:
        return setting.label
    return f"{setting.label} ({', '.join(suffixes)})"


def _connect_value_changed(widget: QWidget, callback) -> None:
    if isinstance(widget, QPlainTextEdit):
        widget.textChanged.connect(callback)
    elif isinstance(widget, (QSpinBox, QDoubleSpinBox)):
        widget.valueChanged.connect(lambda *_: callback())
    elif isinstance(widget, QComboBox):
        widget.currentTextChanged.connect(lambda *_: callback())
    elif isinstance(widget, QLineEdit):
        widget.textChanged.connect(lambda *_: callback())
    elif isinstance(widget, QCheckBox):
        widget.toggled.connect(lambda *_: callback())
