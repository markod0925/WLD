from __future__ import annotations

import importlib
import sqlite3
from pathlib import Path


class SqlCipherError(RuntimeError):
    pass


class SqlCipherUnavailableError(SqlCipherError):
    pass


class SqlCipherOpenError(SqlCipherError):
    pass


class SqlCipherKeyMismatchError(SqlCipherOpenError):
    pass


def open_sqlcipher_connection(db_path: str | Path, key_bytes: bytes):
    if len(key_bytes) != 32:
        raise SqlCipherOpenError("The database key must be exactly 32 bytes long.")

    dbapi = _load_sqlcipher_dbapi()
    connection = None
    try:
        connection = dbapi.connect(str(db_path), check_same_thread=False)
        row_factory = getattr(dbapi, "Row", sqlite3.Row)
        try:
            connection.row_factory = row_factory
        except Exception:
            pass

        key_hex = key_bytes.hex().upper()
        connection.execute(f'PRAGMA key = "x\'{key_hex}\'"')
        connection.execute("SELECT count(*) FROM sqlite_master").fetchone()
        return connection
    except SqlCipherError:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
        raise
    except Exception as exc:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
        if _looks_like_key_mismatch(exc):
            raise SqlCipherKeyMismatchError(
                "The encrypted database key does not match this database file."
            ) from exc
        raise SqlCipherOpenError("Failed to open the encrypted database with SQLCipher.") from exc


def _load_sqlcipher_dbapi():
    last_error: Exception | None = None
    for module_name in ("sqlcipher3", "sqlcipher3.dbapi2", "pysqlcipher3", "pysqlcipher3.dbapi2"):
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:
            last_error = exc
            continue
        if hasattr(module, "connect"):
            return module

    message = "SQLCipher is not available. Install a SQLCipher-compatible database driver."
    raise SqlCipherUnavailableError(message) from last_error


def _looks_like_key_mismatch(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "file is not a database",
            "not a database",
            "database disk image is malformed",
            "file is encrypted or is not a database",
        )
    )

