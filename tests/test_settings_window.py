from __future__ import annotations

from worklog_diary.ui.settings_metadata import float_step_decimals


def test_float_decimals_derives_precision_from_step() -> None:
    assert float_step_decimals(1.0) == 0
    assert float_step_decimals(0.1) == 1
    assert float_step_decimals(0.01) == 2
    assert float_step_decimals(0.001) == 3
