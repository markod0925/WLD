from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from worklog_diary.core import storage as storage_module
from worklog_diary.core.security import dpapi as dpapi_module
from worklog_diary.core.security import sqlcipher as sqlcipher_module


class _FakeDPAPIBackend:
    _prefix = b"WLD-DPAPI:"

    def protect(self, data: bytes) -> bytes:
        return self._prefix + bytes(data)[::-1]

    def unprotect(self, data: bytes) -> bytes:
        blob = bytes(data)
        if blob == self._prefix + b"DPAPI-FAIL":
            raise dpapi_module.DPAPIUnavailableError("Windows DPAPI is unavailable.")
        if not blob.startswith(self._prefix):
            raise dpapi_module.DPAPIError("The protected database key blob is corrupted.")
        return blob[len(self._prefix) :][::-1]


def _fake_open_sqlcipher_connection(db_path: str | Path, key_bytes: bytes):
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    key_path = path.with_name(f"{path.name}.cipherkey")
    raw_key = bytes(key_bytes)

    if key_path.exists():
        stored_key = key_path.read_bytes()
        if stored_key != raw_key:
            raise sqlcipher_module.SqlCipherKeyMismatchError(
                "The encrypted database key does not match this database file."
            )
    else:
        key_path.write_bytes(raw_key)

    connection = sqlite3.connect(str(path), check_same_thread=False)
    connection.row_factory = sqlite3.Row
    return connection


@pytest.fixture(autouse=True)
def fake_encryption_backends(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dpapi_module, "_backend", _FakeDPAPIBackend(), raising=False)
    monkeypatch.setattr(storage_module, "open_sqlcipher_connection", _fake_open_sqlcipher_connection)
    monkeypatch.setattr(sqlcipher_module, "open_sqlcipher_connection", _fake_open_sqlcipher_connection)
    yield

