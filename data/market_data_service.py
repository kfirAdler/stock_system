from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
import time
from typing import Callable

import pandas as pd

from data.base_provider import MarketDataProvider
from storage.supabase_market_data_cache import SupabaseMarketDataCache


@dataclass
class MarketDataService:
    """Coordinates market data fetch, validation, and local cache updates."""

    provider: MarketDataProvider
    raw_data_dir: Path
    min_history_rows: int = 120
    history_tail_rows: int = 1200
    history_fetch_lookback_days: int = 1800
    min_refetch_interval_minutes: int = 30
    worker_stall_timeout_seconds: int = 90
    cache_enabled: bool = True
    force_refresh: bool = False
    supabase_cache: SupabaseMarketDataCache | None = None
    local_file_cache_enabled: bool = True

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        if self.local_file_cache_enabled:
            self.raw_data_dir.mkdir(parents=True, exist_ok=True)

    def get_history(self, ticker: str, use_cache: bool = True, allow_remote_fetch: bool = True) -> pd.DataFrame:
        return self._get_history(ticker=ticker, use_cache=use_cache, allow_remote_fetch=allow_remote_fetch)

    def _get_history(self, ticker: str, use_cache: bool, allow_remote_fetch: bool) -> pd.DataFrame:
        cache_file = self.raw_data_dir / f"{ticker.upper()}.csv"
        allow_cache = self.cache_enabled and use_cache
        use_supabase_cache = self.supabase_cache is not None and self.supabase_cache.enabled
        if not use_supabase_cache and not self.local_file_cache_enabled:
            raise RuntimeError("No cache backend configured. Enable Supabase or local file cache.")
        if use_supabase_cache:
            return self._get_history_supabase(
                ticker=ticker,
                allow_cache=allow_cache,
                allow_remote_fetch=allow_remote_fetch,
            )

        if not allow_remote_fetch:
            if not allow_cache:
                self._logger.warning("Cache-only mode for %s but cache is disabled", ticker)
                return pd.DataFrame()
            cached = self._read_cached(cache_file)
            validated_cached = self._validate(cached, ticker, allow_short=True)
            if validated_cached.empty:
                self._logger.warning("Cache-only mode: no cached data for %s", ticker)
                return pd.DataFrame()
            return self._trim_for_analysis(self._validate(validated_cached, ticker), ticker)

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
                return self._trim_for_analysis(self._validate(validated_cached, ticker), ticker)

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
                return self._trim_for_analysis(self._validate(validated_cached, ticker), ticker)

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
            return self._trim_for_analysis(validated_merged, ticker)

        return self._fetch_and_cache_full(ticker, cache_file)

    def _get_history_supabase(self, ticker: str, allow_cache: bool, allow_remote_fetch: bool) -> pd.DataFrame:
        if self.supabase_cache is None:
            raise RuntimeError("Supabase cache is not configured")

        if not allow_remote_fetch:
            if not allow_cache:
                self._logger.warning("Cache-only mode for %s but cache is disabled", ticker)
                return pd.DataFrame()
            recent_limit = max(self.min_history_rows, self.history_tail_rows)
            cached_recent = self.supabase_cache.load_recent_history(ticker=ticker, limit_rows=recent_limit)
            validated_cached = self._validate(cached_recent, ticker, allow_short=True)
            if validated_cached.empty:
                self._logger.warning("Cache-only mode: no cached data for %s", ticker)
                return pd.DataFrame()
            self._logger.info("Cache-only mode: loaded %s recent rows for %s", len(validated_cached), ticker)
            return self._trim_for_analysis(self._validate(validated_cached, ticker), ticker)

        if not allow_cache or self.force_refresh:
            self._logger.info("Force refresh for %s (bounded lookback window)", ticker)
            fetched = self.provider.fetch_history(
                ticker=ticker,
                start_date=date.today() - timedelta(days=self.history_fetch_lookback_days),
                end_date=date.today(),
            )
            validated = self._validate(fetched, ticker)
            if not validated.empty:
                self.supabase_cache.save_history(ticker=ticker, frame=validated)
                self._logger.info("Saved %s rows to Supabase cache for %s", len(validated), ticker)
            return self._trim_for_analysis(validated, ticker)

        last_cached_date = self.supabase_cache.latest_trade_date(ticker)
        last_complete_date = date.today() - timedelta(days=1)
        if last_cached_date is None:
            self._logger.info("No Supabase cache for %s, fetching bounded history", ticker)
            fetched = self.provider.fetch_history(
                ticker=ticker,
                start_date=date.today() - timedelta(days=self.history_fetch_lookback_days),
                end_date=date.today(),
            )
            validated = self._validate(fetched, ticker)
            if not validated.empty:
                self.supabase_cache.save_history(ticker=ticker, frame=validated)
                self._logger.info("Saved %s rows to Supabase cache for %s", len(validated), ticker)
            return self._trim_for_analysis(validated, ticker)

        recent_inserted_at = self.supabase_cache.latest_inserted_at(ticker)
        if recent_inserted_at is not None:
            inserted_utc = recent_inserted_at.astimezone(UTC) if recent_inserted_at.tzinfo else recent_inserted_at.replace(tzinfo=UTC)
            age = datetime.now(UTC) - inserted_utc
            if age < timedelta(minutes=self.min_refetch_interval_minutes):
                self._logger.info(
                    "Skipping remote fetch for %s; cache was refreshed at %s (within %s minutes)",
                    ticker,
                    inserted_utc.isoformat(),
                    self.min_refetch_interval_minutes,
                )
                recent_limit = max(self.min_history_rows, self.history_tail_rows)
                cached_recent = self.supabase_cache.load_recent_history(ticker=ticker, limit_rows=recent_limit)
                validated_cached = self._validate(cached_recent, ticker)
                return self._trim_for_analysis(validated_cached, ticker)

        if last_cached_date < last_complete_date:
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
            validated_incremental = self._validate(incremental, ticker, allow_short=True)
            if validated_incremental.empty:
                self._logger.info("No new rows returned for %s", ticker)
            else:
                self.supabase_cache.save_history(ticker=ticker, frame=validated_incremental)
                self._logger.info("Merged and saved updated raw data for %s", ticker)

        recent_limit = max(self.min_history_rows, self.history_tail_rows)
        cached_recent = self.supabase_cache.load_recent_history(ticker=ticker, limit_rows=recent_limit)
        validated_cached = self._validate(cached_recent, ticker)
        if validated_cached.empty:
            self._logger.info("Recent cache invalid for %s, fetching bounded history", ticker)
            fetched = self.provider.fetch_history(
                ticker=ticker,
                start_date=date.today() - timedelta(days=self.history_fetch_lookback_days),
                end_date=date.today(),
            )
            validated = self._validate(fetched, ticker)
            if not validated.empty:
                self.supabase_cache.save_history(ticker=ticker, frame=validated)
                self._logger.info("Saved %s rows to Supabase cache for %s", len(validated), ticker)
            return self._trim_for_analysis(validated, ticker)

        self._logger.info("Loaded %s recent rows for %s from Supabase cache", len(validated_cached), ticker)
        self._logger.info("Cache up to date for %s through %s", ticker, validated_cached.index.max().date())
        return self._trim_for_analysis(validated_cached, ticker)

    def get_histories(
        self,
        tickers: list[str],
        use_cache: bool = True,
        max_workers: int = 1,
        progress_callback: Callable[[int, int, str], None] | None = None,
        on_history: Callable[[str, pd.DataFrame], None] | None = None,
        allow_remote_fetch: bool = True,
    ) -> dict[str, pd.DataFrame]:
        histories: dict[str, pd.DataFrame] = {}
        total = len(tickers)
        if total == 0:
            return histories

        worker_count = max(1, min(max_workers, total))
        if worker_count == 1:
            for idx, ticker in enumerate(tickers, start=1):
                history = self._get_history(ticker=ticker, use_cache=use_cache, allow_remote_fetch=allow_remote_fetch)
                if on_history is None:
                    histories[ticker] = history
                else:
                    on_history(ticker, history)
                if progress_callback is not None:
                    progress_callback(idx, total, ticker)
            return histories

        executor = ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="md")
        futures = {
            executor.submit(self._get_history, ticker=ticker, use_cache=use_cache, allow_remote_fetch=allow_remote_fetch): ticker
            for ticker in tickers
        }
        pending = set(futures.keys())
        completed = 0
        last_progress = time.monotonic()
        try:
            while pending:
                done, still_pending = wait(pending, timeout=2.0, return_when=FIRST_COMPLETED)
                if not done:
                    stalled_for = time.monotonic() - last_progress
                    if stalled_for >= self.worker_stall_timeout_seconds:
                        self._logger.warning(
                            "Parallel fetch stalled for %.1fs with %s pending tickers; cancelling pending tasks",
                            stalled_for,
                            len(still_pending),
                        )
                        for future in list(still_pending):
                            ticker = futures[future]
                            future.cancel()
                            if on_history is None:
                                histories[ticker] = pd.DataFrame()
                            else:
                                on_history(ticker, pd.DataFrame())
                            completed += 1
                            if progress_callback is not None:
                                progress_callback(completed, total, ticker)
                        pending = set()
                        break
                    continue

                last_progress = time.monotonic()
                for future in done:
                    ticker = futures[future]
                    try:
                        history = future.result()
                        if on_history is None:
                            histories[ticker] = history
                        else:
                            on_history(ticker, history)
                    except Exception:  # noqa: BLE001
                        self._logger.exception("Failed fetching history for %s", ticker)
                        if on_history is None:
                            histories[ticker] = pd.DataFrame()
                        else:
                            on_history(ticker, pd.DataFrame())
                    completed += 1
                    if progress_callback is not None:
                        progress_callback(completed, total, ticker)
                pending = still_pending
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
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
        return self._trim_for_analysis(validated, ticker)

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

        validated = frame.sort_index().copy()
        for column in ["Open", "High", "Low", "Close", "Volume"]:
            validated[column] = pd.to_numeric(validated[column], errors="coerce")
        validated = validated.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
        if not allow_short and len(validated) < self.min_history_rows:
            self._logger.warning(
                "History for %s has only %s rows (minimum %s)",
                ticker,
                len(validated),
                self.min_history_rows,
            )
            return pd.DataFrame()

        return validated

    def _trim_for_analysis(self, frame: pd.DataFrame, ticker: str) -> pd.DataFrame:
        if frame.empty:
            return frame
        if self.history_tail_rows <= 0:
            return frame
        if len(frame) <= self.history_tail_rows:
            return frame
        trimmed = frame.tail(self.history_tail_rows)
        self._logger.debug(
            "Trimmed %s history from %s to %s rows for analysis",
            ticker,
            len(frame),
            len(trimmed),
        )
        return trimmed
