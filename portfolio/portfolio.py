from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from domain.models import PortfolioSnapshot, PositionSnapshot
from portfolio.position import Position


@dataclass
class Portfolio:
    """Portfolio with cash and position management."""

    initial_capital: float
    cash: float = field(init=False)
    open_positions: dict[str, Position] = field(default_factory=dict)
    closed_positions: list[Position] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.cash = self.initial_capital

    def can_open(self, ticker: str, max_open_positions: int) -> bool:
        return ticker not in self.open_positions and len(self.open_positions) < max_open_positions

    def open_position(self, position: Position) -> bool:
        cost = position.shares * position.entry_price
        if cost <= 0 or cost > self.cash:
            return False

        self.cash -= cost
        self.open_positions[position.ticker] = position
        return True

    def close_position(self, ticker: str, exit_date: date, exit_price: float) -> None:
        position = self.open_positions.pop(ticker)
        position.close(exit_date=exit_date, exit_price=exit_price)
        proceeds = position.shares * exit_price
        self.cash += proceeds
        self.closed_positions.append(position)

    def snapshot(self, snapshot_date: date, prices: dict[str, float]) -> PortfolioSnapshot:
        position_snapshots: list[PositionSnapshot] = []
        equity = self.cash

        for ticker, position in self.open_positions.items():
            current_price = prices.get(ticker, position.entry_price)
            market_value = position.market_value(current_price)
            equity += market_value
            position_snapshots.append(
                PositionSnapshot(
                    ticker=ticker,
                    shares=position.shares,
                    entry_price=position.entry_price,
                    current_price=current_price,
                    market_value=market_value,
                    stop_loss=position.stop_loss,
                    take_profit=position.take_profit,
                )
            )

        return PortfolioSnapshot(
            snapshot_date=snapshot_date,
            cash=round(self.cash, 2),
            equity=round(equity, 2),
            open_positions=position_snapshots,
        )
