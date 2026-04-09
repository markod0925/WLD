from worklog_diary.core.privacy import PrivacyPolicyEngine



def test_default_blocked_apps_are_blocked() -> None:
    engine = PrivacyPolicyEngine.with_defaults()
    assert engine.is_blocked_process("chrome.exe")
    assert engine.is_blocked_process("msedge.exe")
    assert engine.is_blocked_process("webex.exe")



def test_allowed_app_is_not_blocked() -> None:
    engine = PrivacyPolicyEngine.with_defaults()
    assert not engine.is_blocked_process("notepad.exe")



def test_custom_blocked_apps_work() -> None:
    engine = PrivacyPolicyEngine.with_defaults()
    engine.update_blocked_processes(["notepad.exe", "code.exe"])

    assert engine.is_blocked_process("notepad.exe")
    assert engine.is_blocked_process("code.exe")
    assert not engine.is_blocked_process("chrome.exe")
