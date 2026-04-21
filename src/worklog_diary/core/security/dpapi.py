from __future__ import annotations

import ctypes
import sys


class DPAPIError(RuntimeError):
    pass


class DPAPIUnavailableError(DPAPIError):
    pass


class _DataBlob(ctypes.Structure):
    _fields_ = [
        ("cbData", ctypes.c_uint),
        ("pbData", ctypes.c_void_p),
    ]


class _WindowsDPAPIBackend:
    def __init__(self) -> None:
        self._crypt32 = ctypes.WinDLL("Crypt32.dll", use_last_error=True)
        self._kernel32 = ctypes.WinDLL("Kernel32.dll", use_last_error=True)
        self._crypt32.CryptProtectData.argtypes = [
            ctypes.POINTER(_DataBlob),
            ctypes.c_wchar_p,
            ctypes.POINTER(_DataBlob),
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_uint,
            ctypes.POINTER(_DataBlob),
        ]
        self._crypt32.CryptProtectData.restype = ctypes.c_int
        self._crypt32.CryptUnprotectData.argtypes = [
            ctypes.POINTER(_DataBlob),
            ctypes.c_void_p,
            ctypes.POINTER(_DataBlob),
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_uint,
            ctypes.POINTER(_DataBlob),
        ]
        self._crypt32.CryptUnprotectData.restype = ctypes.c_int
        self._kernel32.LocalFree.argtypes = [ctypes.c_void_p]
        self._kernel32.LocalFree.restype = ctypes.c_void_p

    def protect(self, data: bytes) -> bytes:
        input_buffer = ctypes.create_string_buffer(data)
        input_blob = _DataBlob(len(data), ctypes.cast(input_buffer, ctypes.c_void_p))
        output_blob = _DataBlob(0, ctypes.c_void_p())
        result = self._crypt32.CryptProtectData(
            ctypes.byref(input_blob),
            None,
            None,
            None,
            None,
            0,
            ctypes.byref(output_blob),
        )
        if not result:
            raise DPAPIError(self._format_error("CryptProtectData"))

        try:
            return ctypes.string_at(output_blob.pbData, output_blob.cbData)
        finally:
            if output_blob.pbData:
                self._kernel32.LocalFree(output_blob.pbData)

    def unprotect(self, data: bytes) -> bytes:
        input_buffer = ctypes.create_string_buffer(data)
        input_blob = _DataBlob(len(data), ctypes.cast(input_buffer, ctypes.c_void_p))
        output_blob = _DataBlob(0, ctypes.c_void_p())
        result = self._crypt32.CryptUnprotectData(
            ctypes.byref(input_blob),
            None,
            None,
            None,
            None,
            0,
            ctypes.byref(output_blob),
        )
        if not result:
            raise DPAPIError(self._format_error("CryptUnprotectData"))

        try:
            return ctypes.string_at(output_blob.pbData, output_blob.cbData)
        finally:
            if output_blob.pbData:
                self._kernel32.LocalFree(output_blob.pbData)

    def _format_error(self, operation: str) -> str:
        error_code = ctypes.get_last_error()
        if error_code:
            return f"{operation} failed with Windows error {error_code}: {ctypes.WinError(error_code)}"
        return f"{operation} failed with an unspecified Windows error"


_backend: _WindowsDPAPIBackend | None = None


def _get_backend() -> _WindowsDPAPIBackend:
    global _backend
    if _backend is not None:
        return _backend
    if sys.platform != "win32":
        raise DPAPIUnavailableError("Windows DPAPI is only available on Windows.")
    _backend = _WindowsDPAPIBackend()
    return _backend


def protect_bytes(data: bytes) -> bytes:
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError("data must be bytes-like")
    return _get_backend().protect(bytes(data))


def unprotect_bytes(data: bytes) -> bytes:
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError("data must be bytes-like")
    return _get_backend().unprotect(bytes(data))
