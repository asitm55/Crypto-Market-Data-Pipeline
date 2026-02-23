import pandas as pd
from datetime import datetime

from transform import (
    DataCleaner,
    MetricsCalculator,
    DataQualityChecker,
)


def test_sma_calculation_correct():
    dates = [datetime(2024, 1, i) for i in range(1, 11)]
    df = pd.DataFrame({"date": dates, "priceUsd": list(range(1, 11))})
    res = MetricsCalculator.calculate_sma(df, window=7)

    
    assert res.iloc[6]["sma_7d"] == 4.0


def test_sma_min_periods_one():
    dates = [datetime(2024, 1, 1)]
    df = pd.DataFrame({"date": dates, "priceUsd": [10.0]})
    res = MetricsCalculator.calculate_sma(df, window=7)
    assert res.iloc[0]["sma_7d"] == 10.0
    assert not pd.isna(res.iloc[0]["sma_7d"])


def test_high_volatility_flagged():
    dates = [datetime(2024, 1, 1), datetime(2024, 1, 2)]
    df = pd.DataFrame({"date": dates, "priceUsd": [100.0, 110.0]})
    res = MetricsCalculator.identify_high_volatility(df)
    assert bool(res.iloc[1]["is_high_volatility"]) is True


def test_low_volatility_not_flagged():
    dates = [datetime(2024, 1, 1), datetime(2024, 1, 2)]
    df = pd.DataFrame({"date": dates, "priceUsd": [100.0, 102.0]})
    res = MetricsCalculator.identify_high_volatility(df)
    assert bool(res.iloc[1]["is_high_volatility"]) is False


def test_top_winners_count():
    df = pd.DataFrame(
        {
            "rank": range(1, 11),
            "name": [f"Coin{i}" for i in range(10)],
            "symbol": [f"C{i}" for i in range(10)],
            "priceUsd": [1.0] * 10,
            "changePercent24Hr": range(10, 0, -1),
        }
    )
    res = MetricsCalculator.get_top_winners(df, n=5)
    assert len(res) == 5


def test_top_winners_order():
    df = pd.DataFrame(
        {
            "rank": [1, 2, 3],
            "name": ["A", "B", "C"],
            "symbol": ["A", "B", "C"],
            "priceUsd": [1.0, 1.0, 1.0],
            "changePercent24Hr": [1.0, 50.0, 5.0],
        }
    )
    res = MetricsCalculator.get_top_winners(df, n=5)
    assert res.iloc[0]["name"] == "B"
    assert res.iloc[0]["winner_rank"] == 1


def test_dq_volume_non_negative():
    df = pd.DataFrame({"volumeUsd24Hr": [-100.0, 200.0]})
    res = DataQualityChecker.check_volume_non_negative(df)
    assert res.passed is False
    assert res.failing_count == 1


def test_dq_volume_passes():
    df = pd.DataFrame({"volumeUsd24Hr": [0.0, 200.0, None]})
    res = DataQualityChecker.check_volume_non_negative(df)
    assert res.passed is True


def test_dq_no_date_gaps_passes():
    dates = pd.date_range("2024-01-01", "2024-01-20", freq="D")
    df = pd.DataFrame({"date": dates})
    res = DataQualityChecker.check_no_date_gaps(df, "bitcoin")
    assert res.passed is True


def test_dq_date_gap_detected():
    dates = pd.date_range("2024-01-01", "2024-01-20", freq="D").tolist()
    dates.pop(9)  
    df = pd.DataFrame({"date": dates})
    res = DataQualityChecker.check_no_date_gaps(df, "bitcoin")
    assert res.passed is False
    assert "2024-01-10" in res.message


def test_clean_assets_removes_negative_price():
    df = pd.DataFrame(
        {"priceUsd": [-1.0, 100.0], "volumeUsd24Hr": [10.0, 10.0], "rank": [1, 2]}
    )
    res = DataCleaner.clean_assets(df)
    assert len(res) == 1
    assert res.iloc[0]["priceUsd"] == 100.0


def test_clean_assets_removes_null_price():
    df = pd.DataFrame(
        {"priceUsd": [None, 100.0], "volumeUsd24Hr": [10.0, 10.0], "rank": [1, 2]}
    )
    res = DataCleaner.clean_assets(df)
    assert len(res) == 1
    assert res.iloc[0]["priceUsd"] == 100.0
