from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class TickerUniverseLoader:
    """Loads ticker universes from dynamic sources with local fallback."""

    source_file: Path
    universe_source: str = "file"
    universe_size: int = 500
    sp500_constituents_url: str = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._sector_by_symbol: dict[str, str] = {}

    def load(self) -> list[str]:
        """Return a de-duplicated ordered list of uppercase symbols."""
        source = self.universe_source.lower().strip()
        if source == "sp500":
            dynamic = self._load_sp500_dynamic()
            if dynamic:
                return dynamic
            self._logger.warning("Falling back to local ticker file after dynamic universe failure")

        return self._load_from_file()

    def _load_sp500_dynamic(self) -> list[str]:
        try:
            frame = pd.read_csv(self.sp500_constituents_url)
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("Failed loading S&P 500 constituents: %s", exc)
            return []

        if "Symbol" not in frame.columns:
            self._logger.warning("S&P 500 constituents source missing Symbol column")
            return []

        raw = frame["Symbol"].astype(str).tolist()
        sector_col = "Sector" if "Sector" in frame.columns else None
        if sector_col is not None:
            self._sector_by_symbol = {
                str(symbol).strip().upper(): str(sector).strip() or "Unknown"
                for symbol, sector in zip(frame["Symbol"], frame[sector_col], strict=False)
            }
        cleaned = self._clean(raw)
        selected = cleaned if self.universe_size <= 0 else cleaned[: self.universe_size]

        self._logger.info(
            "Loaded %s dynamic S&P 500 tickers (requested=%s)",
            len(selected),
            self.universe_size,
        )
        return selected

    def _load_from_file(self) -> list[str]:
        with self.source_file.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)

        raw_tickers = payload.get("tickers", [])
        cleaned = self._clean(raw_tickers)
        if self.universe_size > 0:
            return cleaned[: self.universe_size]
        return cleaned

    def sector_for(self, ticker: str) -> str:
        """Return known sector for ticker, or 'Unknown' if not available."""
        return self._sector_by_symbol.get(ticker.upper(), "Unknown")

    @staticmethod
    def _clean(raw_tickers: list[str]) -> list[str]:
        seen: set[str] = set()
        cleaned: list[str] = []
        for ticker in raw_tickers:
            symbol = str(ticker).strip().upper()
            if symbol and symbol not in seen:
                cleaned.append(symbol)
                seen.add(symbol)
        return cleaned
