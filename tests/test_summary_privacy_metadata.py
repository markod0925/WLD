from __future__ import annotations

from worklog_diary.core.batching import SummaryBatch
from worklog_diary.core.models import BlockedInterval
from worklog_diary.core.summarizer import _augment_privacy_limited_event_evidence


def _blocked_batch(*, process: str, window_title: str) -> SummaryBatch:
    return SummaryBatch(
        start_ts=1.0,
        end_ts=2.0,
        blocked_intervals=[
            BlockedInterval(
                id=1,
                active_interval_id=1,
                start_ts=1.0,
                end_ts=2.0,
                process_name=process,
                window_title=window_title,
                summarized=False,
            )
        ],
    )


def test_blocked_chrome_pdf_title_preserves_reference_with_privacy_caveat() -> None:
    payload: dict[str, object] = {}
    batch = _blocked_batch(process="chrome.exe", window_title="ginopino.pdf - Google Chrome")

    _augment_privacy_limited_event_evidence(payload, batch)

    blocked_refs = payload["blocked_observed_references"]
    assert isinstance(blocked_refs, list) and blocked_refs
    first = blocked_refs[0]
    assert isinstance(first, dict)
    assert first["inferred_reference_type"] == "pdf"
    assert first["content_captured"] is False
    assert first["metadata_used"] is True
    assert "ginopino.pdf" in str(first["safe_interpretation"])

    files = payload["files_and_documents"]
    assert isinstance(files, list) and files
    file_entry = files[0]
    assert isinstance(file_entry, dict)
    assert file_entry["path/name"] == "ginopino.pdf"
    assert file_entry["status"] == "read_or_viewed"
    assert file_entry["privacy_limited"] is True


def test_blocked_browser_web_page_title_is_preserved_without_content_inference() -> None:
    payload: dict[str, object] = {}
    batch = _blocked_batch(
        process="msedge.exe",
        window_title="WLD sprint notes - Microsoft Edge",
    )

    _augment_privacy_limited_event_evidence(payload, batch)

    blocked_refs = payload["blocked_observed_references"]
    assert isinstance(blocked_refs, list) and blocked_refs
    first = blocked_refs[0]
    assert isinstance(first, dict)
    assert first["inferred_reference_type"] == "web_page"
    assert first["content_captured"] is False
    assert first["metadata_used"] is True
    assert "WLD sprint notes" in str(first["safe_interpretation"])


def test_blocked_outlook_and_webex_titles_preserve_subject_or_meeting_title() -> None:
    for process, title, expected_type in (
        ("outlook.exe", "Q2 roadmap sync - Outlook", "mail_subject"),
        ("webex.exe", "Backend review - Webex", "meeting_title"),
    ):
        payload: dict[str, object] = {}
        batch = _blocked_batch(process=process, window_title=title)
        _augment_privacy_limited_event_evidence(payload, batch)

        blocked_refs = payload["blocked_observed_references"]
        assert isinstance(blocked_refs, list) and blocked_refs
        first = blocked_refs[0]
        assert isinstance(first, dict)
        assert first["inferred_reference_type"] == expected_type
        assert first["content_captured"] is False

        conversations = payload["conversations_or_references"]
        assert isinstance(conversations, list) and conversations
        conv = conversations[0]
        assert isinstance(conv, dict)
        assert conv["reference"] == title
        assert conv["content_captured"] is False
