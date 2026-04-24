from __future__ import annotations

import sys

from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QApplication, QMessageBox

from ..core.security.db_key_manager import DatabaseKeyMissingError
from ..resources import app_logo_path

DATABASE_STARTUP_ERROR_TITLE = "Encrypted database cannot be opened"
DATABASE_STARTUP_ERROR_MISSING_TEXT = "The database encryption key file is missing."
DATABASE_STARTUP_ERROR_GENERIC_TEXT = "The encrypted database cannot be opened."
DATABASE_STARTUP_ERROR_INFORMATIVE_TEXT = """WorkLog Diary cannot open the encrypted database without its matching key file.

To continue, either:
- copy db_key.bin back into the same data folder as the database, or
- delete the existing database so WorkLog Diary can create a new empty encrypted database.

Deleting the database will permanently remove the stored diary data."""


def show_encrypted_database_startup_error(exc: BaseException) -> None:
    app = QApplication.instance() or QApplication(sys.argv)
    app.setWindowIcon(QIcon(str(app_logo_path())))

    dialog = QMessageBox()
    dialog.setIcon(QMessageBox.Icon.Critical)
    dialog.setWindowTitle(DATABASE_STARTUP_ERROR_TITLE)
    if isinstance(exc, DatabaseKeyMissingError):
        dialog.setText(DATABASE_STARTUP_ERROR_MISSING_TEXT)
    else:
        dialog.setText(DATABASE_STARTUP_ERROR_GENERIC_TEXT)
    dialog.setInformativeText(DATABASE_STARTUP_ERROR_INFORMATIVE_TEXT)
    dialog.setDetailedText(f"{exc.__class__.__name__}: {exc}")
    dialog.exec()
