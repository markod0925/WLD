from __future__ import annotations

import pytest

from worklog_diary.core.security import dpapi as dpapi_module


def test_dpapi_wrapper_round_trip_with_mocked_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    class Backend:
        def protect(self, data: bytes) -> bytes:
            return b"wrapped:" + bytes(data)[::-1]

        def unprotect(self, data: bytes) -> bytes:
            blob = bytes(data)
            if not blob.startswith(b"wrapped:"):
                raise dpapi_module.DPAPIError("bad blob")
            return blob[len(b"wrapped:") :][::-1]

    monkeypatch.setattr(dpapi_module, "_get_backend", lambda: Backend())

    protected = dpapi_module.protect_bytes(b"secret-key")

    assert protected == b"wrapped:" + b"secret-key"[::-1]
    assert dpapi_module.unprotect_bytes(protected) == b"secret-key"


def test_dpapi_wrapper_surfaces_backend_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    class FailingBackend:
        def protect(self, data: bytes) -> bytes:
            raise dpapi_module.DPAPIUnavailableError("DPAPI unavailable")

        def unprotect(self, data: bytes) -> bytes:
            raise dpapi_module.DPAPIUnavailableError("DPAPI unavailable")

    monkeypatch.setattr(dpapi_module, "_get_backend", lambda: FailingBackend())

    with pytest.raises(dpapi_module.DPAPIUnavailableError):
        dpapi_module.protect_bytes(b"secret-key")

