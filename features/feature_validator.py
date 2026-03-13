from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class FeatureValidator:
    """Validates feature frames before scoring."""

    required_columns: tuple[str, ...] = (
        "Close",
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
        "support_20",
        "resistance_20",
        "distance_to_support",
        "distance_to_resistance",
        "short_run_return_20",
    )

    def validate(self, frame: pd.DataFrame) -> bool:
        if frame.empty:
            return False
        return all(column in frame.columns for column in self.required_columns)

    def clean(self, frame: pd.DataFrame) -> pd.DataFrame:
        if not self.validate(frame):
            return pd.DataFrame()
        return frame.dropna(subset=list(self.required_columns)).copy()
