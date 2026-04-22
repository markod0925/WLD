from __future__ import annotations

from pathlib import Path

from worklog_diary.core.security import sqlcipher as sqlcipher_module
from worklog_diary.core.security.sqlcipher import open_sqlcipher_connection as real_open_sqlcipher_connection


class _FakeCursor:
    def __init__(self, connection: "_FakeConnection", row: tuple[int, ...]) -> None:
        self._connection = connection
        self._row = row

    def fetchone(self):
        if self._connection.row_factory is None:
            return self._row
        return self._connection.row_factory(self, self._row)


class _FakeConnection:
    def __init__(self) -> None:
        self.row_factory = None
        self.closed = False
        self.queries: list[str] = []

    def execute(self, query: str):
        self.queries.append(query)
        if "sqlite_master" in query:
            return _FakeCursor(self, (1,))
        return _FakeCursor(self, (0,))

    def close(self) -> None:
        self.closed = True


class _FakeDbApi:
    @staticmethod
    def connect(_db_path: str, check_same_thread: bool = False):  # noqa: ARG004
        return _FakeConnection()

    @staticmethod
    def Row(cursor: object, row: tuple[int, ...]):
        if not isinstance(cursor, _FakeCursor):
            raise TypeError(f"Row() argument 1 must be _FakeCursor, not {type(cursor).__name__}")
        return {"value": row[0]}


def test_open_sqlcipher_connection_uses_dbapi_row_factory(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(sqlcipher_module, "_load_sqlcipher_dbapi", lambda: _FakeDbApi)

    connection = real_open_sqlcipher_connection(tmp_path / "worklog.db", b"\xAB" * 32)
    try:
        row = connection.execute("SELECT count(*) FROM sqlite_master").fetchone()
        assert row == {"value": 1}
    finally:
        connection.close()
