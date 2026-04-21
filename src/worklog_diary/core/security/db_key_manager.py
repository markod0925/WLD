from __future__ import annotations

import os
import secrets
from pathlib import Path

from .dpapi import DPAPIError, DPAPIUnavailableError, protect_bytes, unprotect_bytes


class DatabaseKeyError(RuntimeError):
    pass


class DatabaseKeyMissingError(DatabaseKeyError):
    pass


class DatabaseKeyCorruptedError(DatabaseKeyError):
    pass


class DatabaseKeyProtectionError(DatabaseKeyError):
    pass


class DatabaseKeyUnprotectError(DatabaseKeyError):
    pass


def ensure_database_key(db_path: str | Path, key_path: str | Path) -> bytes:
    db_path = Path(db_path)
    key_path = Path(key_path)
    if key_path.exists():
        return load_database_key(key_path)
    if db_path.exists():
        raise DatabaseKeyMissingError(
            f"Missing encrypted database key blob: {key_path}. The encrypted database cannot be opened."
        )

    raw_key = secrets.token_bytes(32)
    try:
        protected_key = protect_bytes(raw_key)
    except (DPAPIError, DPAPIUnavailableError) as exc:
        raise DatabaseKeyProtectionError("Failed to protect the database key with Windows DPAPI.") from exc

    key_path.parent.mkdir(parents=True, exist_ok=True)
    _write_atomic(key_path, protected_key)
    return raw_key


def load_database_key(key_path: str | Path) -> bytes:
    key_path = Path(key_path)
    if not key_path.exists():
        raise DatabaseKeyMissingError(
            f"Missing encrypted database key blob: {key_path}. The encrypted database cannot be opened."
        )

    protected_key = key_path.read_bytes()
    if not protected_key:
        raise DatabaseKeyCorruptedError(f"Encrypted database key blob is empty: {key_path}")

    try:
        raw_key = unprotect_bytes(protected_key)
    except DPAPIUnavailableError as exc:
        raise DatabaseKeyUnprotectError("Failed to unprotect the encrypted database key blob with Windows DPAPI.") from exc
    except DPAPIError as exc:
        raise DatabaseKeyCorruptedError(
            f"Encrypted database key blob is corrupted or unreadable: {key_path}"
        ) from exc

    if len(raw_key) != 32:
        raise DatabaseKeyCorruptedError(
            f"Encrypted database key blob has an invalid size: {key_path}"
        )
    return raw_key


def _write_atomic(path: Path, content: bytes) -> None:
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        temp_path.write_bytes(content)
        temp_path.replace(path)
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass
