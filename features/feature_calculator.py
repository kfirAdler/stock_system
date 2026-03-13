from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from config.settings import AppSettings
from features.indicators import TechnicalIndicators


@dataclass
class FeatureCalculator:
    """Builds deterministic feature sets used by scoring and simulation."""

    settings: AppSettings

    def compute(self, frame: pd.DataFrame, benchmark_frame: pd.DataFrame | None = None) -> pd.DataFrame:
        enriched = frame.copy()
        close = enriched["Close"]

        enriched["daily_return"] = TechnicalIndicators.daily_returns(close)
        enriched["avg_volume_20"] = TechnicalIndicators.moving_average(
            enriched["Volume"],
            self.settings.rolling_volume_window,
        )
        enriched["ma_20"] = TechnicalIndicators.moving_average(close, self.settings.ma_short_window)
        enriched["ma_50"] = TechnicalIndicators.moving_average(close, self.settings.ma_long_window)
        enriched["rsi_14"] = TechnicalIndicators.rsi(close, self.settings.rsi_window)
        enriched["atr_14"] = TechnicalIndicators.atr(
            high=enriched["High"],
            low=enriched["Low"],
            close=close,
            window=14,
        )
        enriched["volatility_20"] = TechnicalIndicators.rolling_volatility(
            enriched["daily_return"],
            self.settings.volatility_window,
        )
        enriched["distance_from_ma20"] = (close - enriched["ma_20"]) / enriched["ma_20"]
        enriched["distance_from_ma50"] = (close - enriched["ma_50"]) / enriched["ma_50"]
        enriched["short_run_return_20"] = close / close.shift(20) - 1.0

        enriched["support_20"] = enriched["Low"].rolling(window=20, min_periods=20).min()
        enriched["resistance_20"] = enriched["High"].rolling(window=20, min_periods=20).max()
        enriched["distance_to_support"] = (close - enriched["support_20"]) / close
        enriched["distance_to_resistance"] = (enriched["resistance_20"] - close) / close

        rolling_high = close.rolling(window=self.settings.high_window, min_periods=1).max()
        enriched["dist_from_52w_high"] = (close / rolling_high) - 1.0

        enriched["relative_strength"] = self._relative_strength(
            stock_close=close,
            benchmark_frame=benchmark_frame,
        )
        return enriched

    def _relative_strength(self, stock_close: pd.Series, benchmark_frame: pd.DataFrame | None) -> pd.Series:
        if benchmark_frame is None or benchmark_frame.empty:
            return pd.Series(index=stock_close.index, data=float("nan"), dtype="float64")

        benchmark_close = benchmark_frame["Close"].reindex(stock_close.index).ffill()
        stock_perf = stock_close / stock_close.shift(self.settings.relative_strength_window)
        benchmark_perf = benchmark_close / benchmark_close.shift(self.settings.relative_strength_window)
        return stock_perf - benchmark_perf
