from __future__ import annotations

import numpy as np
import pandas as pd

from config.settings import AppSettings
from features.feature_calculator import FeatureCalculator


def _sample_history(rows: int = 300) -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=rows, freq="B")
    close = np.linspace(100, 160, rows)
    frame = pd.DataFrame(
        {
            "Open": close - 1,
            "High": close + 2,
            "Low": close - 2,
            "Close": close,
            "Volume": np.linspace(1_000_000, 1_500_000, rows),
        },
        index=idx,
    )
    frame.index.name = "date"
    return frame


def test_feature_calculator_adds_expected_columns() -> None:
    calculator = FeatureCalculator(settings=AppSettings())
    frame = _sample_history()

    output = calculator.compute(frame)

    expected = {
        "daily_return",
        "avg_volume_20",
        "ma_20",
        "ma_50",
        "rsi_14",
        "atr_14",
        "volatility_20",
        "dist_from_52w_high",
        "distance_from_ma20",
        "distance_from_ma50",
        "short_run_return_20",
        "support_20",
        "resistance_20",
        "distance_to_support",
        "distance_to_resistance",
        "relative_strength",
    }
    assert expected.issubset(output.columns)
