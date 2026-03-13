from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import date

import pandas as pd
import requests

from data.base_provider import MarketDataProvider
from data.symbol_normalizer import SymbolNormalizer


@dataclass
class StooqDataProvider(MarketDataProvider):
    """Fetches daily OHLCV history from Stooq."""

    normalizer: SymbolNormalizer
    interval: str = "d"
    timeout_seconds: int = 20

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

    def fetch_history(
        self,
        ticker: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        symbol = self.normalizer.to_stooq_symbol(ticker)
        url = f"https://stooq.com/q/d/l/?s={symbol}&i={self.interval}"
        params: dict[str, str] = {}
        if start_date is not None:
            params["d1"] = start_date.strftime("%Y%m%d")
        if end_date is not None:
            params["d2"] = end_date.strftime("%Y%m%d")

        try:
            response = requests.get(
                url,
                params=params,
                timeout=self.timeout_seconds,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (X11; Linux x86_64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0 Safari/537.36"
                    ),
                    "Accept": "text/csv,*/*;q=0.9",
                },
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            self._logger.warning("Failed fetching %s from Stooq: %s", ticker, exc)
            return pd.DataFrame()

        if not response.text.strip() or "No data" in response.text:
            self._logger.warning("No data returned for %s", ticker)
            return pd.DataFrame()

        content_type = (response.headers.get("content-type") or "").lower()
        first_line = response.text.splitlines()[0] if response.text else ""
        # Stooq anti-bot/challenge responses often return HTML instead of CSV.
        if "text/html" in content_type or first_line.startswith("<!DOCTYPE") or "<html" in first_line.lower():
            self._logger.warning(
                "Stooq returned non-CSV response for %s (content-type=%s, sample=%s)",
                ticker,
                content_type,
                first_line[:80],
            )
            return pd.DataFrame()

        frame = pd.read_csv(io.StringIO(response.text))
        return self._clean_frame(frame)

    def _clean_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        required_columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
        if any(column not in frame.columns for column in required_columns):
            return pd.DataFrame()

        cleaned = frame[required_columns].copy()
        cleaned["Date"] = pd.to_datetime(cleaned["Date"], errors="coerce")
        for column in ["Open", "High", "Low", "Close", "Volume"]:
            cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

        cleaned = cleaned.dropna().sort_values("Date")
        cleaned = cleaned[cleaned["Close"] > 0]
        if cleaned.empty:
            return cleaned

        cleaned = cleaned.set_index("Date")
        cleaned.index.name = "date"
        return cleaned
