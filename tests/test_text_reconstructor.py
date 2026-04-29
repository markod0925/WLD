from worklog_diary.core.models import KeyEvent
from worklog_diary.core.text_reconstructor import TextReconstructor



def _event(
    ts: float,
    key: str,
    *,
    event_type: str = "down",
    modifiers: list[str] | None = None,
    process: str = "python.exe",
    title: str = "Editor",
    hwnd: int = 100,
) -> KeyEvent:
    return KeyEvent(
        id=None,
        ts=ts,
        key=key,
        event_type=event_type,
        modifiers=modifiers or [],
        process_name=process,
        window_title=title,
        hwnd=hwnd,
        active_interval_id=1,
        processed=False,
    )



def test_plain_typing() -> None:
    reconstructor = TextReconstructor()
    events = [
        _event(1.0, "h"),
        _event(1.1, "e"),
        _event(1.2, "l"),
        _event(1.3, "l"),
        _event(1.4, "o"),
    ]

    segments = reconstructor.reconstruct_events(events, force_flush=True)

    assert len(segments) == 1
    assert segments[0].text == "hello"
    assert segments[0].raw_key_count == 5



def test_backspace_handling() -> None:
    reconstructor = TextReconstructor()
    events = [
        _event(1.0, "h"),
        _event(1.1, "e"),
        _event(1.2, "x"),
        _event(1.3, "Key.backspace"),
        _event(1.4, "l"),
        _event(1.5, "l"),
        _event(1.6, "o"),
    ]

    segments = reconstructor.reconstruct_events(events, force_flush=True)

    assert len(segments) == 1
    assert segments[0].text == "hello"



def test_enter_newline_handling() -> None:
    reconstructor = TextReconstructor()
    events = [
        _event(1.0, "a"),
        _event(1.1, "Key.enter"),
        _event(1.2, "b"),
    ]

    segments = reconstructor.reconstruct_events(events, force_flush=True)

    assert len(segments) == 1
    assert segments[0].text == "a\nb"



def test_hotkey_separation() -> None:
    reconstructor = TextReconstructor()
    events = [
        _event(1.0, "a"),
        _event(1.1, "c", modifiers=["ctrl"]),
        _event(1.2, "d"),
    ]

    segments = reconstructor.reconstruct_events(events, force_flush=True)

    assert len(segments) == 3
    assert segments[0].text == "a"
    assert segments[1].hotkeys == ["CTRL+C"]
    assert segments[2].text == "d"



def test_segmentation_on_window_change() -> None:
    reconstructor = TextReconstructor()
    events = [
        _event(1.0, "a", process="python.exe", title="Window A", hwnd=1),
        _event(1.1, "b", process="python.exe", title="Window A", hwnd=1),
        _event(2.0, "c", process="code.exe", title="Window B", hwnd=2),
    ]

    segments = reconstructor.reconstruct_events(events, force_flush=True)

    assert len(segments) == 2
    assert segments[0].text == "ab"
    assert segments[1].text == "c"


def test_segmentation_on_inactivity_gap() -> None:
    reconstructor = TextReconstructor(inactivity_gap_seconds=1.0)
    events = [
        _event(1.0, "a"),
        _event(1.4, "b"),
        _event(3.0, "c"),
    ]

    segments = reconstructor.reconstruct_events(events, force_flush=True)

    assert len(segments) == 2
    assert segments[0].text == "ab"
    assert segments[1].text == "c"


def test_mixed_text_and_hotkey_sequence() -> None:
    reconstructor = TextReconstructor()
    events = [
        _event(1.0, "h", modifiers=["shift"]),
        _event(1.1, "e"),
        _event(1.2, "Key.backspace"),
        _event(1.3, "e"),
        _event(1.4, "s", modifiers=["ctrl"]),
        _event(1.5, "y"),
    ]

    segments = reconstructor.reconstruct_events(events, force_flush=True)

    assert len(segments) == 3
    assert segments[0].text == "He"
    assert segments[1].hotkeys == ["CTRL+S"]
    assert segments[2].text == "y"


def test_inactivity_flush_without_followup_event() -> None:
    reconstructor = TextReconstructor(inactivity_gap_seconds=1.0)
    reconstructor.feed([_event(1.0, "a"), _event(1.2, "b")], force_flush=False)

    segment = reconstructor.flush_if_inactive(now_ts=3.0)

    assert segment is not None
    assert segment.text == "ab"


def test_runtime_diagnostics_track_open_segment_state() -> None:
    reconstructor = TextReconstructor(inactivity_gap_seconds=10.0)
    reconstructor.feed([_event(1.0, "a"), _event(1.1, "b")], force_flush=False)
    diagnostics = reconstructor.get_runtime_diagnostics()
    assert diagnostics["has_open_segment"] is True
    assert diagnostics["open_segment_char_count"] == 2
    assert diagnostics["open_segment_raw_key_count"] == 2

    reconstructor.feed([], force_flush=True)
    diagnostics_after = reconstructor.get_runtime_diagnostics()
    assert diagnostics_after["has_open_segment"] is False
