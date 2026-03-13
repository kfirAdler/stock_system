from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass
class Position:
    """Represents one open or closed position."""

    ticker: str
    shares: int
    entry_date: date
    entry_price: float
    stop_loss: float
    take_profit: float
    exit_date: date | None = None
    exit_price: float | None = None

    @property
    def is_open(self) -> bool:
        return self.exit_date is None

    def market_value(self, current_price: float) -> float:
        return self.shares * current_price

    def should_exit(self, current_price: float) -> bool:
        return current_price <= self.stop_loss or current_price >= self.take_profit

    def close(self, exit_date: date, exit_price: float) -> None:
        self.exit_date = exit_date
        self.exit_price = exit_price
