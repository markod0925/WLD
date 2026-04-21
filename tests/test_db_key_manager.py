from __future__ import annotations

from pathlib import Path

import pytest

from worklog_diary.core.security.db_key_manager import (
    DatabaseKeyCorruptedError,
    DatabaseKeyMissingError,
    ensure_database_key,
    load_database_key,
)


def test_ensure_database_key_generates_and_persists_protected_blob(tmp_path: Path) -> None:
    db_path = tmp_path / "worklog.db"
    key_path = tmp_path / "db_key.bin"

    raw_key = ensure_database_key(db_path, key_path)
    protected_key = key_path.read_bytes()

    assert len(raw_key) == 32
    assert protected_key != raw_key
    assert load_database_key(key_path) == raw_key


def test_ensure_database_key_rejects_missing_key_for_existing_database(tmp_path: Path) -> None:
    db_path = tmp_path / "worklog.db"
    db_path.write_bytes(b"legacy sqlite")
    key_path = tmp_path / "db_key.bin"

    with pytest.raises(DatabaseKeyMissingError):
        ensure_database_key(db_path, key_path)


def test_load_database_key_rejects_corrupted_blob(tmp_path: Path) -> None:
    key_path = tmp_path / "db_key.bin"
    key_path.write_bytes(b"not-a-dpapi-blob")

    with pytest.raises(DatabaseKeyCorruptedError):
        load_database_key(key_path)

