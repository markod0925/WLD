from worklog_diary.core.internal_artifacts import ActivityPathKind, classify_activity_path, is_internal_artifact_path


def test_classify_internal_screenshot_under_data_dir() -> None:
    p = r"C:\Users\11261\Desktop\WLD\data\screenshots\20260513_220741_100202.png"
    assert classify_activity_path(p, app_data_dir=r"C:\Users\11261\Desktop\WLD\data") == ActivityPathKind.INTERNAL_ARTIFACT


def test_preserve_user_file_outside_data_dir_even_with_wld_name() -> None:
    p = r"C:\Users\11261\Documents\ProjectWLD\notes.txt"
    assert classify_activity_path(p, app_data_dir=r"C:\Users\11261\Desktop\WLD\data") == ActivityPathKind.USER_FILE


def test_case_insensitive_windows_path_filtering() -> None:
    p = r"c:\USERS\11261\desktop\wld\DATA\Screenshots\a.png"
    assert is_internal_artifact_path(p, app_data_dir=r"C:\Users\11261\Desktop\WLD\data") is True


def test_false_positive_paths_outside_app_data_dir_are_preserved() -> None:
    base = r"C:\Users\11261\Desktop\WLD\data"
    samples = [
        r"C:\Dati\Marco\GameDev\WLD_notes\analysis.xlsx",
        r"C:\Users\11261\Desktop\WLD_export_review.md",
        r"C:\Projects\SomeApp\data\screenshots\bug.png",
        r"C:\Users\11261\Documents\screenshots\matlab_result.png",
    ]
    for sample in samples:
        assert classify_activity_path(sample, app_data_dir=base) == ActivityPathKind.USER_FILE
