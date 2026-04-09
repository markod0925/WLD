from worklog_diary.core.batching import build_batch_from_pending
from worklog_diary.core.models import ActiveInterval, BlockedInterval, ScreenshotRecord, TextSegment



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
