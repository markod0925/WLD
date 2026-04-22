from __future__ import annotations


class LMStudioConnectionError(RuntimeError):
    pass


class LMStudioTimeoutError(LMStudioConnectionError):
    pass


class LMStudioServiceUnavailableError(RuntimeError):
    pass
