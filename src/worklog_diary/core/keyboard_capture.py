from __future__ import annotations

import logging
import threading
import time
from typing import Any

from .models import KeyEvent, SharedState
from .storage import SQLiteStorage


class KeyboardCaptureService:
    def __init__(self, storage: SQLiteStorage, state: SharedState) -> None:
        self.storage = storage
        self.state = state
        self.logger = logging.getLogger(__name__)

        self._listener: Any | None = None
        self._pressed_modifiers: set[str] = set()
        self._lock = threading.Lock()

    def start(self) -> None:
        if self._listener is not None:
            return
        try:
            from pynput import keyboard
        except Exception as exc:
            self.logger.warning("Keyboard capture disabled: %s", exc)
            return

        self._listener = keyboard.Listener(on_press=self._on_press, on_release=self._on_release)
        self._listener.daemon = True
        self._listener.start()
        self.logger.info("Keyboard capture service started")

    def stop(self) -> None:
        if self._listener is None:
            return
        self._listener.stop()
        self._listener = None
        self.logger.info("Keyboard capture service stopped")

    def _on_press(self, key: Any) -> None:
        self._handle_event(key, "down")

    def _on_release(self, key: Any) -> None:
        self._handle_event(key, "up")

    def _handle_event(self, key: Any, event_type: str) -> None:
        key_name = _normalize_key_name(key)
        modifier = _modifier_for_key(key_name)

        with self._lock:
            if event_type == "down" and modifier:
                self._pressed_modifiers.add(modifier)

            modifiers = sorted(self._pressed_modifiers)

            snapshot = self.state.snapshot()
            should_record = (
                snapshot.monitoring_active
                and not snapshot.blocked
                and snapshot.foreground_info is not None
            )

            if should_record:
                event = KeyEvent(
                    id=None,
                    ts=time.time(),
                    key=key_name,
                    event_type=event_type,
                    modifiers=modifiers,
                    process_name=snapshot.foreground_info.process_name,
                    window_title=snapshot.foreground_info.window_title,
                    hwnd=snapshot.foreground_info.hwnd,
                    active_interval_id=snapshot.active_interval_id,
                    processed=False,
                )
                self.storage.insert_key_event(event)

            if event_type == "up" and modifier:
                self._pressed_modifiers.discard(modifier)



def _normalize_key_name(key: Any) -> str:
    char_value = getattr(key, "char", None)
    if char_value is not None:
        return str(char_value)
    return str(key)



def _modifier_for_key(key_name: str) -> str | None:
    key_value = key_name.lower()
    if "ctrl" in key_value:
        return "ctrl"
    if "alt" in key_value:
        return "alt"
    if "shift" in key_value:
        return "shift"
    if "cmd" in key_value or "win" in key_value or "super" in key_value:
        return "win"
    return None
