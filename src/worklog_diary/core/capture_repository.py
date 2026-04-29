from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time

from .models import KeyEvent, ScreenshotRecord, TextSegment
from .summary_repository import _LogDbQueryTiming


class CaptureRepository:
    """SQLite-backed capture persistence for keyboard, text, and screenshot data."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        lock: threading.Lock,
        logger: logging.Logger,
        log_db_query_timing: _LogDbQueryTiming,
    ) -> None:
        self._conn = conn
        self._lock = lock
        self._logger = logger
        self._log_db_query_timing = log_db_query_timing

    def insert_key_event(self, event: KeyEvent) -> int:
        started_at = time.perf_counter()
        values = self._key_event_values(event)
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO key_events(
                    ts, key, event_type, modifiers, process_name, window_title, hwnd, active_interval_id, processed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
            self._conn.commit()
            key_event_id = int(cursor.lastrowid)
        self._log_db_query_timing("insert_key_event", started_at, rows=1)
        return key_event_id

    def insert_key_events(self, events: list[KeyEvent]) -> int:
        if not events:
            return 0

        started_at = time.perf_counter()
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO key_events(
                    ts, key, event_type, modifiers, process_name, window_title, hwnd, active_interval_id, processed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [self._key_event_values(event) for event in events],
            )
            self._conn.commit()
        self._log_db_query_timing("insert_key_events", started_at, rows=len(events))
        return len(events)

    def _key_event_values(self, event: KeyEvent) -> tuple[object, ...]:
        return (
            event.ts,
            event.key,
            event.event_type,
            json.dumps(event.modifiers),
            event.process_name,
            event.window_title,
            event.hwnd,
            event.active_interval_id,
            int(event.processed),
        )

    def fetch_unprocessed_key_events(self, limit: int = 5000) -> list[KeyEvent]:
        started_at = time.perf_counter()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, ts, key, event_type, modifiers, process_name, window_title, hwnd, active_interval_id, processed
                FROM key_events
                WHERE processed = 0
                ORDER BY ts ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        result = [
            KeyEvent(
                id=int(row["id"]),
                ts=float(row["ts"]),
                key=str(row["key"]),
                event_type=str(row["event_type"]),
                modifiers=json.loads(str(row["modifiers"])),
                process_name=str(row["process_name"]),
                window_title=str(row["window_title"]),
                hwnd=int(row["hwnd"]),
                active_interval_id=int(row["active_interval_id"]) if row["active_interval_id"] is not None else None,
                processed=bool(row["processed"]),
            )
            for row in rows
        ]
        self._log_db_query_timing("fetch_unprocessed_key_events", started_at, rows=len(result))
        return result

    def mark_key_events_processed(self, ids: list[int]) -> None:
        if not ids:
            return
        started_at = time.perf_counter()
        placeholders = ",".join("?" for _ in ids)
        with self._lock:
            self._conn.execute(f"UPDATE key_events SET processed = 1 WHERE id IN ({placeholders})", ids)
            self._conn.commit()
        self._log_db_query_timing("mark_key_events_processed", started_at, rows=len(ids))

    def insert_text_segments(self, segments: list[TextSegment]) -> None:
        if not segments:
            return
        started_at = time.perf_counter()
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO text_segments(
                    start_ts, end_ts, process_name, window_title, text, hotkeys, raw_key_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        segment.start_ts,
                        segment.end_ts,
                        segment.process_name,
                        segment.window_title,
                        segment.text,
                        json.dumps(segment.hotkeys),
                        segment.raw_key_count,
                    )
                    for segment in segments
                ],
            )
            self._conn.commit()
        self._log_db_query_timing("insert_text_segments", started_at, rows=len(segments))

    def fetch_unsummarized_text_segments(self, limit: int = 200) -> list[TextSegment]:
        started_at = time.perf_counter()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, start_ts, end_ts, process_name, window_title, text, hotkeys, raw_key_count
                FROM text_segments
                ORDER BY start_ts ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        result = [
            TextSegment(
                id=int(row["id"]),
                start_ts=float(row["start_ts"]),
                end_ts=float(row["end_ts"]),
                process_name=str(row["process_name"]),
                window_title=str(row["window_title"]),
                text=str(row["text"]),
                hotkeys=json.loads(str(row["hotkeys"])),
                raw_key_count=int(row["raw_key_count"]),
            )
            for row in rows
        ]
        self._log_db_query_timing("fetch_unsummarized_text_segments", started_at, rows=len(result))
        return result

    def insert_screenshot(self, screenshot: ScreenshotRecord) -> int:
        started_at = time.perf_counter()
        perceptual_hash = screenshot.perceptual_hash or screenshot.fingerprint
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO screenshots(
                    ts, file_path, process_name, window_title, active_interval_id, window_hwnd,
                    fingerprint, exact_hash, perceptual_hash, image_width, image_height,
                    nearest_phash_distance, nearest_ssim, dedup_reason, visual_context_streak
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    screenshot.ts,
                    screenshot.file_path,
                    screenshot.process_name,
                    screenshot.window_title,
                    screenshot.active_interval_id,
                    screenshot.window_hwnd,
                    perceptual_hash,
                    screenshot.exact_hash,
                    perceptual_hash,
                    screenshot.image_width,
                    screenshot.image_height,
                    screenshot.nearest_phash_distance,
                    screenshot.nearest_ssim,
                    screenshot.dedup_reason,
                    int(screenshot.visual_context_streak),
                ),
            )
            self._conn.commit()
            screenshot_id = int(cursor.lastrowid)
        self._log_db_query_timing("insert_screenshot", started_at, rows=1)
        return screenshot_id

    def fetch_unsummarized_screenshots(self, limit: int = 20) -> list[ScreenshotRecord]:
        started_at = time.perf_counter()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT
                    id, ts, file_path, process_name, window_title, active_interval_id, window_hwnd,
                    fingerprint, exact_hash, perceptual_hash, image_width, image_height,
                    nearest_phash_distance, nearest_ssim, dedup_reason, visual_context_streak
                FROM screenshots
                ORDER BY ts ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        result = [
            ScreenshotRecord(
                id=int(row["id"]),
                ts=float(row["ts"]),
                file_path=str(row["file_path"]),
                process_name=str(row["process_name"]),
                window_title=str(row["window_title"]),
                active_interval_id=int(row["active_interval_id"]) if row["active_interval_id"] is not None else None,
                window_hwnd=int(row["window_hwnd"]) if row["window_hwnd"] is not None else None,
                fingerprint=str(row["fingerprint"]) if row["fingerprint"] is not None else None,
                exact_hash=str(row["exact_hash"]) if row["exact_hash"] is not None else None,
                perceptual_hash=str(row["perceptual_hash"]) if row["perceptual_hash"] is not None else None,
                image_width=int(row["image_width"]) if row["image_width"] is not None else None,
                image_height=int(row["image_height"]) if row["image_height"] is not None else None,
                nearest_phash_distance=int(row["nearest_phash_distance"])
                if row["nearest_phash_distance"] is not None
                else None,
                nearest_ssim=float(row["nearest_ssim"]) if row["nearest_ssim"] is not None else None,
                dedup_reason=str(row["dedup_reason"]) if row["dedup_reason"] is not None else None,
                visual_context_streak=int(row["visual_context_streak"]) if row["visual_context_streak"] is not None else 0,
            )
            for row in rows
        ]
        self._log_db_query_timing("fetch_unsummarized_screenshots", started_at, rows=len(result))
        return result

    def fetch_recent_screenshots(self, limit: int = 20) -> list[ScreenshotRecord]:
        started_at = time.perf_counter()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT
                    id, ts, file_path, process_name, window_title, active_interval_id, window_hwnd,
                    fingerprint, exact_hash, perceptual_hash, image_width, image_height,
                    nearest_phash_distance, nearest_ssim, dedup_reason, visual_context_streak
                FROM screenshots
                ORDER BY ts DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        result = [
            ScreenshotRecord(
                id=int(row["id"]),
                ts=float(row["ts"]),
                file_path=str(row["file_path"]),
                process_name=str(row["process_name"]),
                window_title=str(row["window_title"]),
                active_interval_id=int(row["active_interval_id"]) if row["active_interval_id"] is not None else None,
                window_hwnd=int(row["window_hwnd"]) if row["window_hwnd"] is not None else None,
                fingerprint=str(row["fingerprint"]) if row["fingerprint"] is not None else None,
                exact_hash=str(row["exact_hash"]) if row["exact_hash"] is not None else None,
                perceptual_hash=str(row["perceptual_hash"]) if row["perceptual_hash"] is not None else None,
                image_width=int(row["image_width"]) if row["image_width"] is not None else None,
                image_height=int(row["image_height"]) if row["image_height"] is not None else None,
                nearest_phash_distance=int(row["nearest_phash_distance"])
                if row["nearest_phash_distance"] is not None
                else None,
                nearest_ssim=float(row["nearest_ssim"]) if row["nearest_ssim"] is not None else None,
                dedup_reason=str(row["dedup_reason"]) if row["dedup_reason"] is not None else None,
                visual_context_streak=int(row["visual_context_streak"]) if row["visual_context_streak"] is not None else 0,
            )
            for row in rows
        ]
        self._log_db_query_timing("fetch_recent_screenshots", started_at, rows=len(result))
        return result

    def get_pending_counts(self) -> dict[str, int]:
        started_at = time.perf_counter()
        with self._lock:
            result = self._pending_counts_locked()
        self._log_db_query_timing("get_pending_counts", started_at, rows=4)
        return result

    def _pending_counts_locked(self) -> dict[str, int]:
        keys = int(self._conn.execute("SELECT COUNT(1) AS c FROM key_events WHERE processed = 0").fetchone()["c"])
        processed_keys = int(self._conn.execute("SELECT COUNT(1) AS c FROM key_events WHERE processed = 1").fetchone()["c"])
        segments = int(self._conn.execute("SELECT COUNT(1) AS c FROM text_segments").fetchone()["c"])
        screenshots = int(self._conn.execute("SELECT COUNT(1) AS c FROM screenshots").fetchone()["c"])

        return {
            "key_events": keys,
            "processed_key_events": processed_keys,
            "text_segments": segments,
            "screenshots": screenshots,
        }

    def count_unprocessed_key_events(self) -> int:
        started_at = time.perf_counter()
        with self._lock:
            count = self._count_unprocessed_key_events_locked()
        self._log_db_query_timing("count_unprocessed_key_events", started_at, rows=1)
        return count

    def _count_unprocessed_key_events_locked(self) -> int:
        row = self._conn.execute("SELECT COUNT(1) AS c FROM key_events WHERE processed = 0").fetchone()
        return int(row["c"])
