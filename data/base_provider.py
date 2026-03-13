from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

import pandas as pd


class MarketDataProvider(ABC):
    """Abstract market data provider contract."""

    @abstractmethod
    def fetch_history(
        self,
        ticker: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """Return historical OHLCV DataFrame indexed by date."""
