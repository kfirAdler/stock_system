from __future__ import annotations

import numpy as np
import pandas as pd


class TechnicalIndicators:
    """Static indicator calculations over price series."""

    @staticmethod
    def daily_returns(close: pd.Series) -> pd.Series:
        return close.pct_change()

    @staticmethod
    def moving_average(series: pd.Series, window: int) -> pd.Series:
        return series.rolling(window=window, min_periods=window).mean()

    @staticmethod
    def rolling_volatility(returns: pd.Series, window: int) -> pd.Series:
        return returns.rolling(window=window, min_periods=window).std()

    @staticmethod
    def rsi(close: pd.Series, window: int = 14) -> pd.Series:
        delta = close.diff()
        gains = delta.clip(lower=0)
        losses = -delta.clip(upper=0)
        avg_gain = gains.rolling(window=window, min_periods=window).mean()
        avg_loss = losses.rolling(window=window, min_periods=window).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        return rsi.astype("float64").fillna(50.0)

    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, window: int = 14) -> pd.Series:
        """Average True Range used for volatility-aware stop placement."""
        prev_close = close.shift(1)
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return true_range.rolling(window=window, min_periods=window).mean()
