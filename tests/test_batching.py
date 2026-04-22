from datetime import date

from worklog_diary.core.batching import BatchBuilder, build_batch_from_pending
from worklog_diary.core.models import ActiveInterval, BlockedInterval, ScreenshotRecord, SummaryRecord, TextSegment


class FakeActivityRepository:
    def __init__(
        self,
        *,
        intervals: list[ActiveInterval],
        blocked_intervals: list[BlockedInterval],
        text_segments: list[TextSegment],
        screenshots: list[ScreenshotRecord],
        summaries: list[SummaryRecord] | None = None,
    ) -> None:
        self._intervals = list(intervals)
        self._blocked_intervals = list(blocked_intervals)
        self._text_segments = list(text_segments)
        self._screenshots = list(screenshots)
        self._summaries = list(summaries or [])

    def fetch_unsummarized_intervals(self, limit: int = 10000) -> list[ActiveInterval]:
        return self._intervals[:limit]

    def fetch_unsummarized_blocked_intervals(self, limit: int = 10000) -> list[BlockedInterval]:
        return self._blocked_intervals[:limit]

    def fetch_unsummarized_text_segments(self, limit: int = 200) -> list[TextSegment]:
        return self._text_segments[:limit]

    def fetch_unsummarized_screenshots(self, limit: int = 20) -> list[ScreenshotRecord]:
        return self._screenshots[:limit]

    def list_summaries_for_day(self, day: date, limit: int = 500) -> list[SummaryRecord]:
        return self._summaries[:limit]


def _screenshot(
    *,
    ts: float,
    fingerprint: str,
    window_title: str = "A",
    process_name: str = "a.exe",
    window_hwnd: int = 1,
    active_interval_id: int | None = 1,
) -> ScreenshotRecord:
    return ScreenshotRecord(
        id=None,
        ts=ts,
        file_path=f"shot-{ts}.png",
        process_name=process_name,
        window_title=window_title,
        active_interval_id=active_interval_id,
        window_hwnd=window_hwnd,
        fingerprint=fingerprint,
    )



def test_pending_data_grouped_correctly() -> None:
    intervals = [
        ActiveInterval(id=1, start_ts=10.0, end_ts=20.0, hwnd=1, pid=1, process_name="a.exe", window_title="A", blocked=False),
        ActiveInterval(id=2, start_ts=30.0, end_ts=40.0, hwnd=2, pid=2, process_name="b.exe", window_title="B", blocked=False),
    ]
    blocked: list[BlockedInterval] = []
    text_segments = [
        TextSegment(id=1, start_ts=12.0, end_ts=15.0, process_name="a.exe", window_title="A", text="hello", hotkeys=[], raw_key_count=5)
    ]
    screenshots = [
        ScreenshotRecord(id=1, ts=35.0, file_path="shot.png", process_name="b.exe", window_title="B", active_interval_id=2)
    ]

    batch = build_batch_from_pending(intervals, blocked, text_segments, screenshots)

    assert batch is not None
    assert batch.start_ts == 10.0
    assert batch.end_ts == 40.0
    assert len(batch.active_intervals) == 2
    assert len(batch.text_segments) == 1
    assert len(batch.screenshots) == 1



def test_blocked_intervals_preserved_in_batch() -> None:
    intervals: list[ActiveInterval] = []
    blocked = [
        BlockedInterval(
            id=1,
            active_interval_id=10,
            start_ts=50.0,
            end_ts=60.0,
            process_name="chrome.exe",
            window_title="Sensitive",
        )
    ]

    batch = build_batch_from_pending(intervals, blocked, [], [])

    assert batch is not None
    assert len(batch.blocked_intervals) == 1
    assert batch.blocked_intervals[0].process_name == "chrome.exe"



def test_empty_batch_returns_none() -> None:
    batch = build_batch_from_pending([], [], [], [])
    assert batch is None


def test_batch_builder_works_with_fake_repository() -> None:
    intervals = [
        ActiveInterval(id=1, start_ts=10.0, end_ts=20.0, hwnd=1, pid=1, process_name="a.exe", window_title="A", blocked=False),
    ]
    text_segments = [
        TextSegment(
            id=1,
            start_ts=12.0,
            end_ts=13.0,
            process_name="a.exe",
            window_title="A",
            text="hello",
            hotkeys=[],
            raw_key_count=3,
        )
    ]
    repo = FakeActivityRepository(
        intervals=intervals,
        blocked_intervals=[],
        text_segments=text_segments,
        screenshots=[],
    )

    batch = BatchBuilder(storage=repo, max_text_segments=10, max_screenshots=3).build_pending_batch()

    assert batch is not None
    assert batch.start_ts == 10.0
    assert batch.end_ts == 20.0
    assert batch.active_intervals == intervals
    assert batch.text_segments == text_segments


def test_batch_builder_honors_excluded_ranges_with_fake_repository() -> None:
    intervals = [
        ActiveInterval(id=1, start_ts=10.0, end_ts=20.0, hwnd=1, pid=1, process_name="a.exe", window_title="A", blocked=False),
        ActiveInterval(id=2, start_ts=30.0, end_ts=40.0, hwnd=2, pid=2, process_name="b.exe", window_title="B", blocked=False),
    ]
    repo = FakeActivityRepository(
        intervals=intervals,
        blocked_intervals=[],
        text_segments=[],
        screenshots=[],
    )

    batch = BatchBuilder(storage=repo).build_pending_batch(excluded_ranges=[(9.0, 21.0)])

    assert batch is not None
    assert len(batch.active_intervals) == 1
    assert batch.active_intervals[0].id == 2


def test_batch_builder_expands_text_limit_after_excluding_ranges() -> None:
    intervals = [
        ActiveInterval(id=1, start_ts=10.0, end_ts=20.0, hwnd=1, pid=1, process_name="a.exe", window_title="A", blocked=False),
        ActiveInterval(id=2, start_ts=30.0, end_ts=40.0, hwnd=2, pid=2, process_name="b.exe", window_title="B", blocked=False),
    ]
    text_segments = [
        TextSegment(id=1, start_ts=12.0, end_ts=13.0, process_name="a.exe", window_title="A", text="excluded", hotkeys=[], raw_key_count=1),
        TextSegment(id=2, start_ts=32.0, end_ts=33.0, process_name="b.exe", window_title="B", text="kept", hotkeys=[], raw_key_count=1),
    ]
    repo = FakeActivityRepository(
        intervals=intervals,
        blocked_intervals=[],
        text_segments=text_segments,
        screenshots=[],
    )

    batch = BatchBuilder(storage=repo, max_text_segments=1, max_screenshots=3).build_pending_batch(
        excluded_ranges=[(11.0, 14.0)]
    )

    assert batch is not None
    assert len(batch.text_segments) == 1
    assert batch.text_segments[0].id == 2


def test_batch_builder_filters_consecutive_identical_screenshots() -> None:
    repo = FakeActivityRepository(
        intervals=[],
        blocked_intervals=[],
        text_segments=[],
        screenshots=[
            _screenshot(ts=10.0, fingerprint="ffffffffffffffff"),
            _screenshot(ts=20.0, fingerprint="ffffffffffffffff"),
            _screenshot(ts=30.0, fingerprint="ffffffffffffffff"),
        ],
    )

    batch = BatchBuilder(storage=repo, max_text_segments=10, max_screenshots=3).build_pending_batch()

    assert batch is not None
    assert len(batch.screenshots) == 1
    assert batch.screenshots[0].ts == 10.0


def test_batch_builder_keeps_visually_distinct_screenshots() -> None:
    repo = FakeActivityRepository(
        intervals=[],
        blocked_intervals=[],
        text_segments=[],
        screenshots=[
            _screenshot(ts=10.0, fingerprint="0000000000000000"),
            _screenshot(ts=11.0, fingerprint="ffffffffffffffff"),
        ],
    )

    batch = BatchBuilder(storage=repo, max_text_segments=10, max_screenshots=3).build_pending_batch()

    assert batch is not None
    assert len(batch.screenshots) == 2


def test_batch_builder_keeps_screenshots_when_window_title_changes() -> None:
    repo = FakeActivityRepository(
        intervals=[],
        blocked_intervals=[],
        text_segments=[],
        screenshots=[
            _screenshot(ts=10.0, fingerprint="1234567890abcdef", window_title="Editor"),
            _screenshot(ts=11.0, fingerprint="1234567890abcdef", window_title="Settings"),
        ],
    )

    batch = BatchBuilder(storage=repo, max_text_segments=10, max_screenshots=3).build_pending_batch()

    assert batch is not None
    assert len(batch.screenshots) == 2
    assert [item.window_title for item in batch.screenshots] == ["Editor", "Settings"]


def test_batch_builder_respects_max_screenshot_count_after_dedup() -> None:
    repo = FakeActivityRepository(
        intervals=[],
        blocked_intervals=[],
        text_segments=[],
        screenshots=[
            _screenshot(ts=float(index), fingerprint=f"{index:016x}", window_hwnd=index)
            for index in range(10)
        ],
    )

    batch = BatchBuilder(storage=repo, max_text_segments=10, max_screenshots=3).build_pending_batch()

    assert batch is not None
    assert len(batch.screenshots) == 3


def test_batch_builder_skips_short_closed_segments_below_min_duration() -> None:
    intervals = [
        ActiveInterval(id=1, start_ts=0.0, end_ts=30.0, hwnd=1, pid=1, process_name="a.exe", window_title="A", blocked=False),
        ActiveInterval(id=2, start_ts=31.0, end_ts=70.0, hwnd=2, pid=2, process_name="b.exe", window_title="B", blocked=False),
    ]
    repo = FakeActivityRepository(
        intervals=intervals,
        blocked_intervals=[],
        text_segments=[],
        screenshots=[],
    )

    batch = BatchBuilder(
        storage=repo,
        activity_segment_min_duration_seconds=180.0,
        activity_segment_idle_gap_seconds=300.0,
    ).build_pending_batch()

    assert batch is None


def test_batch_builder_selects_first_closed_segment_meeting_min_duration() -> None:
    intervals = [
        ActiveInterval(id=1, start_ts=0.0, end_ts=30.0, hwnd=1, pid=1, process_name="a.exe", window_title="A", blocked=False),
        ActiveInterval(id=2, start_ts=31.0, end_ts=70.0, hwnd=2, pid=2, process_name="b.exe", window_title="B", blocked=False),
        ActiveInterval(id=3, start_ts=71.0, end_ts=320.0, hwnd=2, pid=2, process_name="b.exe", window_title="B", blocked=False),
        ActiveInterval(id=4, start_ts=321.0, end_ts=360.0, hwnd=3, pid=3, process_name="c.exe", window_title="C", blocked=False),
    ]
    repo = FakeActivityRepository(
        intervals=intervals,
        blocked_intervals=[],
        text_segments=[],
        screenshots=[],
    )

    batch = BatchBuilder(
        storage=repo,
        activity_segment_min_duration_seconds=180.0,
        activity_segment_idle_gap_seconds=300.0,
    ).build_pending_batch()

    assert batch is not None
    assert batch.start_ts == 0.0
    assert batch.end_ts == 320.0
    assert [item.id for item in batch.active_intervals] == [1, 2, 3]


def test_batch_builder_accumulates_short_closed_segments_until_min_duration() -> None:
    intervals = [
        ActiveInterval(id=1, start_ts=0.0, end_ts=50.0, hwnd=1, pid=1, process_name="a.exe", window_title="A", blocked=False),
        ActiveInterval(id=2, start_ts=51.0, end_ts=100.0, hwnd=2, pid=2, process_name="b.exe", window_title="B", blocked=False),
        ActiveInterval(id=3, start_ts=101.0, end_ts=150.0, hwnd=3, pid=3, process_name="c.exe", window_title="C", blocked=False),
        ActiveInterval(id=4, start_ts=151.0, end_ts=190.0, hwnd=4, pid=4, process_name="d.exe", window_title="D", blocked=False),
    ]
    repo = FakeActivityRepository(
        intervals=intervals,
        blocked_intervals=[],
        text_segments=[],
        screenshots=[],
    )

    batch = BatchBuilder(
        storage=repo,
        activity_segment_min_duration_seconds=120.0,
        activity_segment_idle_gap_seconds=300.0,
    ).build_pending_batch()

    assert batch is not None
    assert batch.start_ts == 0.0
    assert batch.end_ts == 150.0
    assert [item.id for item in batch.active_intervals] == [1, 2, 3]
    assert len(batch.activity_segments) == 3


def test_batch_builder_force_flush_includes_short_pending_segment() -> None:
    intervals = [
        ActiveInterval(id=1, start_ts=0.0, end_ts=45.0, hwnd=1, pid=1, process_name="a.exe", window_title="A", blocked=False),
    ]
    repo = FakeActivityRepository(
        intervals=intervals,
        blocked_intervals=[],
        text_segments=[],
        screenshots=[],
    )

    batch = BatchBuilder(
        storage=repo,
        activity_segment_min_duration_seconds=180.0,
        activity_segment_idle_gap_seconds=300.0,
    ).build_pending_batch(force_flush=True)

    assert batch is not None
    assert batch.start_ts == 0.0
    assert batch.end_ts == 45.0
    assert [item.id for item in batch.active_intervals] == [1]
