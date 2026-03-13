from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import pandas as pd

from data.base_provider import MarketDataProvider


@dataclass
class FallbackMarketDataProvider(MarketDataProvider):
    """Tries providers in order until one returns non-empty history."""

    providers: list[MarketDataProvider]

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

    def fetch_history(
        self,
        ticker: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        last_empty = pd.DataFrame()
        for provider in self.providers:
            frame = provider.fetch_history(ticker=ticker, start_date=start_date, end_date=end_date)
            if not frame.empty:
                self._logger.info("Fetched %s from %s", ticker, provider.__class__.__name__)
                return frame
            last_empty = frame
        return last_empty

