from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from collections import Counter
from math import isinf, isnan
from typing import Any

import pandas as pd

from config.universe_loader import TickerUniverseLoader
from data.market_data_service import MarketDataService
from domain.enums import SignalClassification
from domain.models import PortfolioSnapshot, StockAnalysis
from domain.result_models import AnalysisBatchResult, BacktestSummary, RunDiagnostics
from features.feature_calculator import FeatureCalculator
from features.feature_validator import FeatureValidator
from planning.trade_planner import TradePlanner
from portfolio.simulator import PortfolioSimulator
from scoring.stock_scorer import StockScorer
from storage.supabase_postgres_repository import SupabasePostgresRepository


@dataclass
class AnalysisRunner:
    """Coordinates end-to-end analysis, planning, simulation, and persistence."""

    universe_loader: TickerUniverseLoader
    market_data_service: MarketDataService
    feature_calculator: FeatureCalculator
    feature_validator: FeatureValidator
    scorer: StockScorer
    planner: TradePlanner
    simulator: PortfolioSimulator
    supabase_repository: SupabasePostgresRepository | None = None
    benchmark_symbol: str = "SPY"
    max_workers: int = 1
    progress_callback: Callable[[str, int, int, str], None] | None = None
    log_callback: Callable[[str], None] | None = None

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

    def run(self, use_cache_only: bool = False) -> AnalysisBatchResult:
        tickers = self.universe_loader.load()
        self._logger.info("Loaded %s tickers", len(tickers))
        self._emit_log(f"Loaded {len(tickers)} tickers")
        if use_cache_only:
            self._emit_log("Running in cache-only mode (remote fetch disabled)")
        sector_by_ticker = {ticker: self.universe_loader.sector_for(ticker) for ticker in tickers}

        self._emit_log("Loading benchmark history")
        benchmark = self.market_data_service.get_history(
            ticker=self.benchmark_symbol,
            use_cache=True,
            allow_remote_fetch=not use_cache_only,
        )
        self._emit_log(f"Fetching historical data with {self.max_workers} worker(s)")

        analyses: list[StockAnalysis] = []
        feature_histories: dict[str, pd.DataFrame] = {}
        latest_rows: dict[str, pd.Series] = {}
        sector_returns: dict[str, list[float]] = {}

        def process_history(ticker: str, history: pd.DataFrame) -> None:
            if history.empty:
                return

            features = self.feature_calculator.compute(history, benchmark_frame=benchmark)
            clean_features = self.feature_validator.clean(features)
            if clean_features.empty:
                return
            optimized_features = self._optimize_feature_frame(clean_features)
            feature_histories[ticker] = optimized_features
            latest_rows[ticker] = clean_features.iloc[-1].copy()
            sector = sector_by_ticker.get(ticker, "Unknown")
            recent_return = self._latest_short_run_return(clean_features)
            sector_returns.setdefault(sector, []).append(recent_return)

        self.market_data_service.get_histories(
            tickers=tickers,
            max_workers=self.max_workers,
            progress_callback=self._on_history_progress,
            on_history=process_history,
            allow_remote_fetch=not use_cache_only,
        )
        self._emit_log(f"History phase done: {len(feature_histories)} tickers with usable feature frames")
        self._emit_log("Computing sector strength baselines")

        sector_strength = {
            sector: (sum(values) / len(values)) if values else 0.0
            for sector, values in sector_returns.items()
        }

        self._emit_log("Starting latest-row scoring/classification phase")
        for ticker, latest_row in latest_rows.items():
            if ticker not in feature_histories:
                continue
            latest_ts = feature_histories[ticker].index[-1]
            sector = sector_by_ticker.get(ticker, "Unknown")
            latest_row["sector_strength"] = sector_strength.get(sector, 0.0)

            score = self.scorer.score_row(latest_row)
            classification = self.scorer.classify(score)
            buyability_status, buyability_reason = self.scorer.buyability(score)

            analyses.append(
                StockAnalysis(
                    ticker=ticker,
                    as_of_date=latest_ts.date(),
                    latest_close=round(float(latest_row["Close"]), 2),
                    score=score,
                    classification=classification,
                    score_debug=score.score_debug,
                    signal_flags=score.signal_flags,
                    buyability_status=buyability_status,
                    buyability_reason=buyability_reason,
                    sector=sector,
                    feature_snapshot={
                        "ma_20": round(float(latest_row["ma_20"]), 4),
                        "ma_50": round(float(latest_row["ma_50"]), 4),
                        "rsi_14": round(float(latest_row["rsi_14"]), 4),
                        "atr_14": round(float(latest_row["atr_14"]), 4),
                        "volatility_20": round(float(latest_row["volatility_20"]), 6),
                        "dist_from_52w_high": round(float(latest_row["dist_from_52w_high"]), 6),
                        "distance_from_ma20": round(float(latest_row["distance_from_ma20"]), 6),
                        "distance_from_ma50": round(float(latest_row["distance_from_ma50"]), 6),
                        "short_run_return_20": round(float(latest_row["short_run_return_20"]), 6),
                        "support_20": round(float(latest_row["support_20"]), 4),
                        "resistance_20": round(float(latest_row["resistance_20"]), 4),
                        "distance_to_support": round(float(latest_row["distance_to_support"]), 6),
                        "distance_to_resistance": round(float(latest_row["distance_to_resistance"]), 6),
                        "relative_strength": self._safe_round(latest_row.get("relative_strength"), 6),
                        "sector_strength": round(float(latest_row["sector_strength"]), 6),
                    },
                )
            )
            self._on_analysis_progress(len(analyses), len(feature_histories), ticker)
        self._emit_log(f"Analysis phase done: {len(analyses)} analyses built")

        self._on_phase_progress("planning", 0, 1, "start")
        self._emit_log("Starting trade-plan generation")
        self._emit_log("Sorting analyses by buyability, classification, entry timing, and score")
        analyses.sort(
            key=lambda item: (
                item.buyability_status.value == "BUYABLE_NOW",
                item.classification is SignalClassification.BUY,
                item.score.entry_timing_score,
                item.score.total_score,
            ),
            reverse=True,
        )
        plans = self.planner.build_plans(analyses)
        self._on_phase_progress("planning", 1, 1, "done")
        self._emit_log(f"Trade-plan generation done: {len(plans)} plans")

        self._on_phase_progress("simulation", 0, 1, "start")
        self._emit_log("Starting portfolio simulation")
        snapshots = self.simulator.run(
            feature_histories,
            progress_callback=self._on_simulation_progress,
            log_callback=self._emit_log,
        )
        self._on_phase_progress("simulation", 1, 1, "done")
        self._emit_log(f"Portfolio simulation done: {len(snapshots)} snapshots")

        self._emit_log("Building diagnostics and backtest summary")
        diagnostics = self._build_diagnostics(analyses)
        backtest_summary = self._build_backtest_summary(snapshots)

        # Keep a single persisted run on disk by overwriting the `latest` artifact set.
        run_id = "latest"
        result = AnalysisBatchResult(
            run_id=run_id,
            generated_at=datetime.utcnow(),
            analyses=analyses,
            trade_plans=plans,
            portfolio_snapshots=snapshots,
            diagnostics=diagnostics,
            backtest_summary=backtest_summary,
        )

        self._on_phase_progress("persistence", 0, 1, "start")
        if self.supabase_repository is not None and self.supabase_repository.enabled:
            self._emit_log("Persisting analysis batch to Supabase")
            self.supabase_repository.save_analysis_batch(result)
            self._emit_log("Supabase persistence completed")
        self._on_phase_progress("persistence", 1, 1, "done")
        self._emit_log(f"Run completed. run_id={run_id} analyses={len(analyses)}")

        return result

    def _on_history_progress(self, current: int, total: int, ticker: str) -> None:
        if self.progress_callback is not None:
            self.progress_callback("history", current, total, ticker)

    def _on_analysis_progress(self, current: int, total: int, ticker: str) -> None:
        if self.progress_callback is not None:
            self.progress_callback("analysis", current, total, ticker)

    def _on_simulation_progress(self, current: int, total: int, snapshot_date: str) -> None:
        if self.progress_callback is not None:
            self.progress_callback("simulation", current, total, snapshot_date)

    def _on_phase_progress(self, phase: str, current: int, total: int, label: str) -> None:
        if self.progress_callback is not None:
            self.progress_callback(phase, current, total, label)

    def _emit_log(self, message: str) -> None:
        if self.log_callback is not None:
            self.log_callback(message)

    def _build_diagnostics(self, analyses: list[StockAnalysis]) -> RunDiagnostics:
        buy_count = len([item for item in analyses if item.classification is SignalClassification.BUY])
        watch_count = len([item for item in analyses if item.classification is SignalClassification.WATCH])
        avoid_count = len([item for item in analyses if item.classification is SignalClassification.AVOID])

        scores = [item.score.total_score for item in analyses]
        average_score = round(sum(scores) / len(scores), 2) if scores else 0.0
        top_5 = [item.ticker for item in analyses[:5]]
        bottom_5 = [item.ticker for item in sorted(analyses, key=lambda item: item.score.total_score)[:5]]
        top_buys = [item for item in analyses if item.classification is SignalClassification.BUY][:5]
        top_buy_sectors = dict(Counter(item.sector for item in top_buys))
        sector_warning = None
        if top_buy_sectors and max(top_buy_sectors.values()) >= 3:
            sector_warning = "High sector concentration: multiple top BUY candidates come from the same sector."

        return RunDiagnostics(
            total_tickers_analyzed=len(analyses),
            buy_count=buy_count,
            watch_count=watch_count,
            avoid_count=avoid_count,
            average_total_score=average_score,
            top_5_tickers=top_5,
            bottom_5_tickers=bottom_5,
            sector_concentration_warning=sector_warning,
            top_buy_sectors=top_buy_sectors,
        )

    def _build_backtest_summary(self, snapshots: list[PortfolioSnapshot]) -> BacktestSummary:
        if not snapshots:
            return BacktestSummary(
                snapshot_count=0,
                first_snapshot_date=None,
                last_snapshot_date=None,
                initial_equity=None,
                final_equity=None,
                absolute_return=None,
                return_pct=None,
            )

        first = snapshots[0]
        last = snapshots[-1]
        absolute_return = last.equity - first.equity
        return_pct = (absolute_return / first.equity * 100.0) if first.equity > 0 else 0.0

        return BacktestSummary(
            snapshot_count=len(snapshots),
            first_snapshot_date=first.snapshot_date.isoformat(),
            last_snapshot_date=last.snapshot_date.isoformat(),
            initial_equity=round(first.equity, 2),
            final_equity=round(last.equity, 2),
            absolute_return=round(absolute_return, 2),
            return_pct=round(return_pct, 2),
        )

    @staticmethod
    def _safe_round(value: Any, digits: int) -> float | None:
        if value is None:
            return None
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if isnan(numeric) or isinf(numeric):
            return None
        return round(numeric, digits)

    @staticmethod
    def _latest_short_run_return(frame: pd.DataFrame) -> float:
        if "short_run_return_20" in frame.columns:
            latest = frame["short_run_return_20"].iloc[-1]
            if pd.notna(latest):
                return float(latest)
        if len(frame) < 21:
            return 0.0
        return float(frame["Close"].iloc[-1] / frame["Close"].iloc[-21] - 1.0)

    @staticmethod
    def _optimize_feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
        optimized = frame.copy()
        for column in optimized.columns:
            if pd.api.types.is_float_dtype(optimized[column]):
                optimized[column] = pd.to_numeric(optimized[column], downcast="float")
            elif pd.api.types.is_integer_dtype(optimized[column]):
                optimized[column] = pd.to_numeric(optimized[column], downcast="integer")
        return optimized
