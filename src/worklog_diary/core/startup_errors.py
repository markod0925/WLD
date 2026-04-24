from __future__ import annotations

from .security.db_key_manager import (
    DatabaseKeyCorruptedError,
    DatabaseKeyMissingError,
    DatabaseKeyProtectionError,
    DatabaseKeyUnprotectError,
)
from .security.sqlcipher import (
    SqlCipherKeyMismatchError,
    SqlCipherOpenError,
    SqlCipherUnavailableError,
)

ENCRYPTED_DATABASE_STARTUP_ERRORS = (
    DatabaseKeyMissingError,
    DatabaseKeyCorruptedError,
    DatabaseKeyProtectionError,
    DatabaseKeyUnprotectError,
    SqlCipherUnavailableError,
    SqlCipherKeyMismatchError,
    SqlCipherOpenError,
)
