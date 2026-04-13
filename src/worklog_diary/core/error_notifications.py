from __future__ import annotations

import threading
from collections.abc import Callable


class ErrorNotificationManager:
    def __init__(self, sink: Callable[[str, str], None] | None = None) -> None:
        self._sink = sink
        self._lock = threading.Lock()
        self._active_keys: dict[str, str] = {}

    def set_sink(self, sink: Callable[[str, str], None] | None) -> None:
        with self._lock:
            self._sink = sink

    def notify(self, category: str, message: str, *, key: str | None = None) -> bool:
        normalized_key = key or message
        with self._lock:
            if self._active_keys.get(category) == normalized_key:
                return False
            self._active_keys[category] = normalized_key
            sink = self._sink

        if sink is not None:
            sink(category, message)
        return True

    def resolve(self, category: str, *, key: str | None = None) -> None:
        with self._lock:
            if key is None or self._active_keys.get(category) == key:
                self._active_keys.pop(category, None)

    def resolve_many(self, *categories: str) -> None:
        with self._lock:
            for category in categories:
                self._active_keys.pop(category, None)
