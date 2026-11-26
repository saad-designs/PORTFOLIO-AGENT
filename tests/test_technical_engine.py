import pandas as pd
import pytest

from technical_engine import wilder_rsi


def test_rsi_matches_known_wilder_reference():
    # Classic Wilder example series; reference RSI(14) on the 15th close is ~70.53.
    closes = pd.Series([
        44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.10, 45.42,
        45.84, 46.08, 45.89, 46.03, 45.61, 46.28, 46.28,
    ])
    rsi = wilder_rsi(closes, 14)
    assert rsi.iloc[:14].isna().all()
    assert rsi.iloc[14] == pytest.approx(70.53, abs=0.1)


def test_rsi_bounds_for_monotonic_series():
    assert wilder_rsi(pd.Series(range(1, 40), dtype=float)).iloc[-1] == 100.0
    assert wilder_rsi(pd.Series(range(40, 1, -1), dtype=float)).iloc[-1] == pytest.approx(0.0)
