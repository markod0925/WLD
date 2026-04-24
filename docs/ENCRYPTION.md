# Database Encryption

## Overview

WorkLog Diary stores its local database in an encrypted SQLCipher file.

The database encryption key is:

- generated as 32 random bytes with a cryptographically secure RNG,
- protected with Windows DPAPI using `CurrentUser` scope,
- stored only as an encrypted blob in `db_key.bin`.

The database file alone is not enough to open the data outside the application.

## Bootstrap Flow

On first run:

1. Generate a 32-byte database key.
2. Protect the key with Windows DPAPI (`CurrentUser`).
3. Store the protected key blob in `db_key.bin`.
4. Open the SQLCipher database with the raw key.
5. Initialize the schema.

On normal startup:

1. Load `db_key.bin`.
2. Unprotect it with Windows DPAPI.
3. Open the SQLCipher database with the recovered key.
4. Run a sanity query to confirm the file decrypts correctly.

## Key Storage

The key blob file lives next to the database file as `db_key.bin`.

This file is not plaintext. It contains only the DPAPI-protected key blob. There is no recovery key, master password, or user-entered secret.

## DPAPI Usage

The application uses Windows DPAPI with `CurrentUser` scope only.

That means:

- the protected key is tied to the Windows user context,
- another Windows user on the same machine cannot decrypt it,
- the same user context is required to reopen the database later.

## Threat Model And Limits

This design protects against copying the database file by itself.

It does not protect against:

- a compromised Windows account,
- runtime memory inspection,
- temporary artifacts such as screenshots, logs, cache files, or exports,
- loss of the Windows user context that owns the DPAPI-protected key.

There is no independent recovery mechanism. If the user context or DPAPI state is lost, the database may become unreadable.

## Failure Modes

The application treats the following as hard errors:

- missing `db_key.bin` when an existing database is present,
- corrupted or unreadable key blob,
- DPAPI decryption failure,
- SQLCipher not available,
- database/key mismatch,
- SQLCipher sanity query failure.

Each case is logged and surfaced as a user-facing startup failure. There is no plaintext fallback.

If `db_key.bin` is missing, restore the original file to the same folder as the database before starting WorkLog Diary again. Without that matching key file, the existing encrypted database is unreadable. If the key cannot be restored, the only recovery option is to delete the existing database and let WorkLog Diary create a new empty encrypted database; this permanently removes the stored diary data.

## Packaging And Development Notes

Windows builds should include a SQLCipher-compatible Python binding. The project uses `sqlcipher3-binary` in the Python dependency list.

PyInstaller builds should collect the SQLCipher package and any bundled native libraries so the frozen app does not depend on a system-wide SQLCipher installation.

The project does not migrate legacy plaintext databases. If a pre-encryption database exists, it is treated as incompatible with this development branch.
