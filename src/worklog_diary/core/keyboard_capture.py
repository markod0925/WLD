from __future__ import annotations

import logging
import threading
import time
from typing import Any
from collections.abc import Callable

from .config import native_hooks_disabled
from .models import KeyEvent, SharedState
from .privacy import PrivacyPolicyEngine
from .storage import SQLiteStorage
from .window_tracker import get_foreground_window_info


class KeyboardCaptureService:
    def __init__(
        self,
        storage: SQLiteStorage,
        state: SharedState,
        privacy: PrivacyPolicyEngine,
        foreground_provider: Callable[[], Any] = get_foreground_window_info,
    ) -> None:
        self.storage = storage
        self.state = state
        self.privacy = privacy
        self.foreground_provider = foreground_provider
        self.logger = logging.getLogger(__name__)

        self._listener: Any | None = None
        self._pressed_modifiers: set[str] = set()
        self._lock = threading.Lock()

    def start(self) -> None:
        if self._listener is not None:
            return
        if native_hooks_disabled():
            self.logger.info("Keyboard capture disabled in test/native-hook-off mode")
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
            if not (
                snapshot.monitoring_active
                and snapshot.foreground_info is not None
                and snapshot.active_interval_id is not None
            ):
                self.logger.debug(
                    "event=key_capture_skipped reason=inactive_or_missing_context key=%s event_type=%s",
                    key_name,
                    event_type,
                )
                if event_type == "up" and modifier:
                    self._pressed_modifiers.discard(modifier)
                return

            current_info = self.foreground_provider()
            blocked_now = self.privacy.is_blocked(current_info.process_name)
            matches_state = (
                snapshot.foreground_info.hwnd == current_info.hwnd
                and snapshot.foreground_info.pid == current_info.pid
            )
            should_record = not blocked_now and matches_state

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
                self.logger.debug(
                    (
                        "event=key_capture_accepted key=%s event_type=%s modifiers=%s "
                        "process=%s title=%s interval_id=%s"
                    ),
                    key_name,
                    event_type,
                    ",".join(modifiers),
                    snapshot.foreground_info.process_name,
                    snapshot.foreground_info.window_title,
                    snapshot.active_interval_id,
                )
            else:
                reason = "blocked_process" if blocked_now else "foreground_mismatch"
                self.logger.debug(
                    (
                        "event=key_capture_skipped reason=%s key=%s event_type=%s "
                        "state_process=%s state_hwnd=%s current_process=%s current_hwnd=%s"
                    ),
                    reason,
                    key_name,
                    event_type,
                    snapshot.foreground_info.process_name,
                    snapshot.foreground_info.hwnd,
                    current_info.process_name,
                    current_info.hwnd,
                )

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
