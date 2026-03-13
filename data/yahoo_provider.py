from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd
import yfinance as yf

from data.base_provider import MarketDataProvider


@dataclass
class YahooFinanceDataProvider(MarketDataProvider):
    """Fetches daily OHLCV history from Yahoo Finance."""

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

    def fetch_history(
        self,
        ticker: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        symbol = ticker.strip().upper().replace(".", "-")
        start = start_date or date(1980, 1, 1)
        # yfinance end date is exclusive, so add one day.
        end = (end_date + timedelta(days=1)) if end_date else (date.today() + timedelta(days=1))

        try:
            frame = yf.download(
                tickers=symbol,
                start=start.isoformat(),
                end=end.isoformat(),
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
            )
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("Yahoo fetch failed for %s: %s", ticker, exc)
            return pd.DataFrame()

        if frame.empty:
            return frame

        if isinstance(frame.columns, pd.MultiIndex):
            frame.columns = frame.columns.get_level_values(0)

        required = ["Open", "High", "Low", "Close", "Volume"]
        if any(col not in frame.columns for col in required):
            return pd.DataFrame()

        cleaned = frame[required].copy()
        cleaned.index = pd.to_datetime(cleaned.index, errors="coerce")
        cleaned = cleaned.dropna().sort_index()
        cleaned = cleaned[cleaned["Close"] > 0]
        cleaned.index.name = "date"
        return cleaned

