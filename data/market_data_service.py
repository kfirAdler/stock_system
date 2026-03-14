from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from data.base_provider import MarketDataProvider
from storage.supabase_market_data_cache import SupabaseMarketDataCache


@dataclass
class MarketDataService:
    """Coordinates market data fetch, validation, and local cache updates."""

    provider: MarketDataProvider
    raw_data_dir: Path
    min_history_rows: int = 120
    cache_enabled: bool = True
    force_refresh: bool = False
    supabase_cache: SupabaseMarketDataCache | None = None
    local_file_cache_enabled: bool = True

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        if self.local_file_cache_enabled:
            self.raw_data_dir.mkdir(parents=True, exist_ok=True)

    def get_history(self, ticker: str, use_cache: bool = True) -> pd.DataFrame:
        cache_file = self.raw_data_dir / f"{ticker.upper()}.csv"
        allow_cache = self.cache_enabled and use_cache
        use_supabase_cache = self.supabase_cache is not None and self.supabase_cache.enabled
        if not use_supabase_cache and not self.local_file_cache_enabled:
            raise RuntimeError("No cache backend configured. Enable Supabase or local file cache.")

        if allow_cache and not self.force_refresh:
            cached = self.supabase_cache.load_history(ticker) if use_supabase_cache else self._read_cached(cache_file)
            validated_cached = self._validate(cached, ticker, allow_short=True)
            if validated_cached.empty:
                self._logger.info("Cache for %s is invalid, fetching full history", ticker)
                return self._fetch_and_cache_full(ticker, cache_file)

            source = "Supabase cache" if use_supabase_cache else "file cache"
            self._logger.info("Loaded %s rows for %s from %s", len(validated_cached), ticker, source)
            last_cached_date = validated_cached.index.max().date()
            last_complete_date = date.today() - timedelta(days=1)
            if last_cached_date >= last_complete_date:
                self._logger.info("Cache up to date for %s through %s", ticker, last_cached_date)
                return self._validate(validated_cached, ticker)

            incremental_start = last_cached_date + timedelta(days=1)
            self._logger.info(
                "Fetching incremental data for %s from %s to %s",
                ticker,
                incremental_start,
                date.today(),
            )
            incremental = self.provider.fetch_history(
                ticker=ticker,
                start_date=incremental_start,
                end_date=date.today(),
            )
            if incremental.empty:
                self._logger.info("No new rows returned for %s", ticker)
                return self._validate(validated_cached, ticker)

            merged = self._merge_frames(validated_cached, incremental)
            validated_merged = self._validate(merged, ticker)
            if not validated_merged.empty:
                if use_supabase_cache:
                    self.supabase_cache.save_history(ticker=ticker, frame=validated_merged)
                else:
                    if not self.local_file_cache_enabled:
                        raise RuntimeError("Local file cache is disabled; cannot persist raw data")
                    validated_merged.to_csv(cache_file)
                self._logger.info("Merged and saved updated raw data for %s", ticker)
            return validated_merged

        return self._fetch_and_cache_full(ticker, cache_file)

    def get_histories(self, tickers: list[str], use_cache: bool = True) -> dict[str, pd.DataFrame]:
        histories: dict[str, pd.DataFrame] = {}
        for ticker in tickers:
            histories[ticker] = self.get_history(ticker=ticker, use_cache=use_cache)
        return histories

    def _fetch_and_cache_full(self, ticker: str, cache_file: Path) -> pd.DataFrame:
        self._logger.info("Fetching full history for %s", ticker)
        fetched = self.provider.fetch_history(ticker=ticker)
        validated = self._validate(fetched, ticker)
        if not validated.empty:
            if self.supabase_cache is not None and self.supabase_cache.enabled:
                self.supabase_cache.save_history(ticker=ticker, frame=validated)
                self._logger.info("Saved %s rows to Supabase cache for %s", len(validated), ticker)
            else:
                if not self.local_file_cache_enabled:
                    raise RuntimeError("Local file cache is disabled; cannot persist raw data")
                validated.to_csv(cache_file)
                self._logger.info("Saved %s rows to file cache for %s", len(validated), ticker)
        return validated

    def _read_cached(self, cache_file: Path) -> pd.DataFrame:
        try:
            frame = pd.read_csv(cache_file, parse_dates=["date"]).set_index("date")
            frame.index.name = "date"
            return frame
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("Failed reading cache file %s: %s", cache_file, exc)
            return pd.DataFrame()

    def _merge_frames(self, cached: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
        merged = pd.concat([cached, incoming], axis=0)
        merged = merged[~merged.index.duplicated(keep="last")]
        return merged.sort_index()

    def _validate(self, frame: pd.DataFrame, ticker: str, allow_short: bool = False) -> pd.DataFrame:
        if frame.empty:
            self._logger.warning("Empty history for %s", ticker)
            return frame

        required_columns = {"Open", "High", "Low", "Close", "Volume"}
        if not required_columns.issubset(frame.columns):
            self._logger.warning("History for %s missing required columns", ticker)
            return pd.DataFrame()

        validated = frame.sort_index()
        if not allow_short and len(validated) < self.min_history_rows:
            self._logger.warning(
                "History for %s has only %s rows (minimum %s)",
                ticker,
                len(validated),
                self.min_history_rows,
            )
            return pd.DataFrame()

        return validated
