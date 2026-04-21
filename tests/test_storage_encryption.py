from __future__ import annotations

from pathlib import Path

import pytest

from worklog_diary.core.security.db_key_manager import DatabaseKeyCorruptedError, DatabaseKeyMissingError
from worklog_diary.core.security.sqlcipher import SqlCipherKeyMismatchError
from worklog_diary.core.storage import SQLiteStorage


def test_storage_bootstrap_creates_key_blob_and_reopens(tmp_path: Path) -> None:
    db_path = tmp_path / "worklog.db"
    storage = SQLiteStorage(str(db_path))
    key_path = tmp_path / "db_key.bin"
    try:
        assert key_path.exists()
        assert key_path.read_bytes()
    finally:
        storage.close()

    reopened = SQLiteStorage(str(db_path))
    try:
        assert reopened.get_diagnostics_snapshot()["table_counts"]["active_intervals"] == 0
    finally:
        reopened.close()


def test_storage_fails_when_key_blob_is_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "worklog.db"
    storage = SQLiteStorage(str(db_path))
    key_path = tmp_path / "db_key.bin"
    storage.close()
    key_path.unlink()

    with pytest.raises(DatabaseKeyMissingError):
        SQLiteStorage(str(db_path))


def test_storage_fails_when_key_blob_is_corrupted(tmp_path: Path) -> None:
    db_path = tmp_path / "worklog.db"
    storage = SQLiteStorage(str(db_path))
    key_path = tmp_path / "db_key.bin"
    storage.close()
    key_path.write_bytes(b"corrupted")

    with pytest.raises(DatabaseKeyCorruptedError):
        SQLiteStorage(str(db_path))


def test_storage_fails_when_database_key_does_not_match(tmp_path: Path) -> None:
    db_path = tmp_path / "worklog.db"
    storage = SQLiteStorage(str(db_path))
    storage.close()

    key_sidecar = tmp_path / "worklog.db.cipherkey"
    key_sidecar.write_bytes(b"wrong-key")

    with pytest.raises(SqlCipherKeyMismatchError):
        SQLiteStorage(str(db_path))

