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
