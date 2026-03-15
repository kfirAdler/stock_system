from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

import pandas as pd

from config.settings import SimulationSettings
from domain.enums import SignalClassification
from domain.models import PortfolioSnapshot
from portfolio.portfolio import Portfolio
from portfolio.position import Position
from scoring.stock_scorer import StockScorer


@dataclass
class PortfolioSimulator:
    """Simple daily simulation based on BUY signals and risk constraints."""

    scorer: StockScorer
    settings: SimulationSettings

    def run(
        self,
        feature_histories: dict[str, pd.DataFrame],
        progress_callback: Callable[[int, int, str], None] | None = None,
        log_callback: Callable[[str], None] | None = None,
    ) -> list[PortfolioSnapshot]:
        portfolio = Portfolio(initial_capital=self.settings.initial_capital)
        snapshots: list[PortfolioSnapshot] = []
        if not feature_histories:
            return snapshots

        all_dates = sorted({dt for frame in feature_histories.values() for dt in frame.index})
        all_dates = [
            dt
            for dt in all_dates
            if self.settings.backtest_start_date <= dt.date() <= self.settings.backtest_end_date
        ]
        if not all_dates:
            return snapshots
        if log_callback is not None:
            log_callback(
                f"Simulation date range prepared: {len(all_dates)} sessions "
                f"({all_dates[0].date().isoformat()} -> {all_dates[-1].date().isoformat()})"
            )

        total = len(all_dates)
        for idx, ts in enumerate(all_dates, start=1):
            current_date = ts.date()
            prices = self._daily_prices(feature_histories, ts)
            self._apply_exits(portfolio, prices, current_date)
            self._apply_entries(portfolio, feature_histories, ts, current_date)
            snapshots.append(portfolio.snapshot(snapshot_date=current_date, prices=prices))
            if progress_callback is not None:
                progress_callback(idx, total, current_date.isoformat())
            if log_callback is not None and (idx == 1 or idx == total or idx % 60 == 0):
                log_callback(f"Simulation progress {idx}/{total} ({current_date.isoformat()})")

        return snapshots

    def _daily_prices(self, feature_histories: dict[str, pd.DataFrame], ts: pd.Timestamp) -> dict[str, float]:
        prices: dict[str, float] = {}
        for ticker, frame in feature_histories.items():
            if ts in frame.index:
                prices[ticker] = float(frame.loc[ts, "Close"])
        return prices

    def _apply_exits(self, portfolio: Portfolio, prices: dict[str, float], current_date: date) -> None:
        to_close: list[tuple[str, float]] = []
        for ticker, position in portfolio.open_positions.items():
            if ticker not in prices:
                continue
            current_price = prices[ticker]
            if position.should_exit(current_price):
                to_close.append((ticker, current_price))

        for ticker, current_price in to_close:
            portfolio.close_position(ticker=ticker, exit_date=current_date, exit_price=current_price)

    def _apply_entries(
        self,
        portfolio: Portfolio,
        feature_histories: dict[str, pd.DataFrame],
        ts: pd.Timestamp,
        current_date: date,
    ) -> None:
        slots_left = self.settings.max_open_positions - len(portfolio.open_positions)
        if slots_left <= 0:
            return

        candidates: list[tuple[str, float, float]] = []
        for ticker, frame in feature_histories.items():
            if ts not in frame.index:
                continue
            if not portfolio.can_open(ticker, self.settings.max_open_positions):
                continue

            row = frame.loc[ts]
            breakdown = self.scorer.score_row(row)
            classification = self.scorer.classify(breakdown)
            if classification is SignalClassification.BUY:
                candidates.append((ticker, breakdown.total_score, float(row["Close"])))

        candidates.sort(key=lambda item: item[1], reverse=True)

        for ticker, _, close_price in candidates[:slots_left]:
            budget = portfolio.cash * self.settings.max_position_fraction
            shares = int(budget / close_price)
            if shares <= 0:
                continue

            position = Position(
                ticker=ticker,
                shares=shares,
                entry_date=current_date,
                entry_price=close_price,
                stop_loss=close_price * (1 - self.settings.stop_loss_pct),
                take_profit=close_price * (1 + self.settings.take_profit_pct),
            )
            portfolio.open_position(position)
