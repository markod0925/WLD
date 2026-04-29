from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field

from .models import KeyEvent, SharedState, TextSegment
from .storage import SQLiteStorage

SHIFTED_CHAR_MAP = {
    "1": "!",
    "2": "@",
    "3": "#",
    "4": "$",
    "5": "%",
    "6": "^",
    "7": "&",
    "8": "*",
    "9": "(",
    "0": ")",
    "-": "_",
    "=": "+",
    "[": "{",
    "]": "}",
    "\\": "|",
    ";": ":",
    "'": '"',
    ",": "<",
    ".": ">",
    "/": "?",
    "`": "~",
}


@dataclass(slots=True)
class _SegmentBuffer:
    start_ts: float
    end_ts: float
    process_name: str
    window_title: str
    hwnd: int
    chars: list[str] = field(default_factory=list)
    hotkeys: list[str] = field(default_factory=list)
    raw_key_count: int = 0


class TextReconstructor:
    def __init__(self, inactivity_gap_seconds: float = 8.0) -> None:
        self.inactivity_gap_seconds = inactivity_gap_seconds
        self._current: _SegmentBuffer | None = None

    def feed(self, events: list[KeyEvent], force_flush: bool = False) -> list[TextSegment]:
        completed: list[TextSegment] = []

        for event in sorted(events, key=lambda item: item.ts):
            if event.event_type != "down":
                continue
            if _is_modifier_key(event.key):
                continue

            if self._needs_split(event):
                segment = self._finalize_current()
                if segment:
                    completed.append(segment)

            if _is_hotkey(event):
                segment = self._finalize_current()
                if segment:
                    completed.append(segment)
                completed.append(
                    TextSegment(
                        id=None,
                        start_ts=event.ts,
                        end_ts=event.ts,
                        process_name=event.process_name,
                        window_title=event.window_title,
                        text="",
                        hotkeys=[_format_hotkey(event)],
                        raw_key_count=1,
                    )
                )
                continue

            if self._current is None:
                self._current = _SegmentBuffer(
                    start_ts=event.ts,
                    end_ts=event.ts,
                    process_name=event.process_name,
                    window_title=event.window_title,
                    hwnd=event.hwnd,
                )

            self._current.end_ts = event.ts
            self._current.raw_key_count += 1

            token = _key_to_token(event.key, event.modifiers)
            if token is None:
                continue
            if token == "<BACKSPACE>":
                if self._current.chars:
                    self._current.chars.pop()
            else:
                self._current.chars.append(token)

        if force_flush:
            segment = self._finalize_current()
            if segment:
                completed.append(segment)

        return completed

    def reconstruct_events(self, events: list[KeyEvent], force_flush: bool = True) -> list[TextSegment]:
        return self.feed(events, force_flush=force_flush)

    def flush_if_inactive(self, now_ts: float | None = None) -> TextSegment | None:
        if self._current is None:
            return None
        current_time = now_ts if now_ts is not None else time.time()
        if current_time - self._current.end_ts <= self.inactivity_gap_seconds:
            return None
        return self._finalize_current()

    def _needs_split(self, event: KeyEvent) -> bool:
        if self._current is None:
            return False
        if event.process_name != self._current.process_name:
            return True
        if event.window_title != self._current.window_title:
            return True
        if event.hwnd != self._current.hwnd:
            return True
        if event.ts - self._current.end_ts > self.inactivity_gap_seconds:
            return True
        return False

    def _finalize_current(self) -> TextSegment | None:
        if self._current is None:
            return None
        segment = TextSegment(
            id=None,
            start_ts=self._current.start_ts,
            end_ts=self._current.end_ts,
            process_name=self._current.process_name,
            window_title=self._current.window_title,
            text="".join(self._current.chars),
            hotkeys=self._current.hotkeys,
            raw_key_count=self._current.raw_key_count,
        )
        self._current = None

        if not segment.text and not segment.hotkeys:
            return None
        return segment

    def get_runtime_diagnostics(self) -> dict[str, int | bool]:
        if self._current is None:
            return {"has_open_segment": False, "open_segment_raw_key_count": 0, "open_segment_char_count": 0}
        return {
            "has_open_segment": True,
            "open_segment_raw_key_count": self._current.raw_key_count,
            "open_segment_char_count": len(self._current.chars),
        }


class TextReconstructionService:
    def __init__(
        self,
        storage: SQLiteStorage,
        reconstructor: TextReconstructor,
        poll_interval_seconds: float = 2.0,
        state: SharedState | None = None,
        shutdown_event: threading.Event | None = None,
    ) -> None:
        self.storage = storage
        self.reconstructor = reconstructor
        self.poll_interval_seconds = max(0.5, poll_interval_seconds)
        self.state = state
        self.logger = logging.getLogger(__name__)

        self._shutdown_event = shutdown_event or threading.Event()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="TextReconstruction", daemon=True)
        self._thread.start()
        self.logger.info("Text reconstruction service started")

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        try:
            self.process_once(force_flush=True)
        except Exception as exc:
            self.logger.exception("Text reconstruction final flush failed: %s", exc)
        self.logger.info("Text reconstruction service stopped")

    def process_once(self, force_flush: bool = False) -> int:
        if self._shutdown_event.is_set() and not force_flush:
            return 0
        if not force_flush and self.state is not None and not self.state.snapshot().monitoring_active:
            return 0

        events = self.storage.fetch_unprocessed_key_events()
        if not events:
            if force_flush:
                segments = self.reconstructor.feed([], force_flush=True)
                if segments:
                    self.storage.insert_text_segments(segments)
                    self._log_segments(segments)
                    return len(segments)
            stale_segment = self.reconstructor.flush_if_inactive()
            if stale_segment:
                self.storage.insert_text_segments([stale_segment])
                self._log_segments([stale_segment])
                return 1
            return 0

        segments = self.reconstructor.reconstruct_events(events, force_flush=force_flush)
        if segments:
            self.storage.insert_text_segments(segments)
            self._log_segments(segments)

        event_ids = [event.id for event in events if event.id is not None]
        self.storage.mark_key_events_processed([event_id for event_id in event_ids if event_id is not None])
        return len(segments)

    def _run(self) -> None:
        while not self._stop_event.is_set() and not self._shutdown_event.is_set():
            should_stop = False
            try:
                self.process_once(force_flush=False)
            except Exception as exc:
                self.logger.exception("Text reconstruction failed: %s", exc)
            finally:
                should_stop = self._stop_event.wait(self.poll_interval_seconds) or self._shutdown_event.is_set()
            if should_stop:
                break

    def _log_segments(self, segments: list[TextSegment]) -> None:
        for segment in segments:
            self.logger.info(
                (
                    "event=text_segment_finalized start_ts=%.3f end_ts=%.3f process=%s "
                    "title=%s raw_keys=%s hotkeys=%s char_count=%s"
                ),
                segment.start_ts,
                segment.end_ts,
                segment.process_name,
                segment.window_title,
                segment.raw_key_count,
                ",".join(segment.hotkeys),
                len(segment.text),
            )



def _is_modifier_key(key: str) -> bool:
    value = key.lower()
    return any(token in value for token in ("shift", "ctrl", "alt", "cmd", "win", "super"))



def _is_hotkey(event: KeyEvent) -> bool:
    hotkey_mods = {"ctrl", "alt", "win", "cmd"}
    modifiers = {item.lower() for item in event.modifiers}
    if not modifiers.intersection(hotkey_mods):
        return False
    return not _is_modifier_key(event.key)



def _format_hotkey(event: KeyEvent) -> str:
    modifiers = [item.upper() for item in event.modifiers if item.lower() in {"ctrl", "alt", "shift", "win", "cmd"}]
    key_name = event.key
    if key_name.startswith("Key."):
        key_name = key_name.split(".", 1)[1]
    return "+".join(modifiers + [key_name.upper()])



def _key_to_token(key: str, modifiers: list[str]) -> str | None:
    normalized = key.strip()

    if normalized == "Key.space":
        return " "
    if normalized == "Key.tab":
        return "\t"
    if normalized in {"Key.enter", "Key.return"}:
        return "\n"
    if normalized == "Key.backspace":
        return "<BACKSPACE>"

    if len(normalized) != 1:
        return None

    if "shift" in {item.lower() for item in modifiers}:
        if normalized.isalpha():
            return normalized.upper()
        if normalized in SHIFTED_CHAR_MAP:
            return SHIFTED_CHAR_MAP[normalized]

    if normalized.isalpha():
        return normalized.lower()

    return normalized
