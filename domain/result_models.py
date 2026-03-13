from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime

from domain.models import PortfolioSnapshot, StockAnalysis, TradePlan


@dataclass(frozen=True)
class RunDiagnostics:
    """Compact diagnostic summary for one analysis run."""

    total_tickers_analyzed: int
    buy_count: int
    watch_count: int
    avoid_count: int
    average_total_score: float
    top_5_tickers: list[str]
    bottom_5_tickers: list[str]
    sector_concentration_warning: str | None
    top_buy_sectors: dict[str, int]


@dataclass(frozen=True)
class BacktestSummary:
    """Summary metrics for backtest output."""

    snapshot_count: int
    first_snapshot_date: str | None
    last_snapshot_date: str | None
    initial_equity: float | None
    final_equity: float | None
    absolute_return: float | None
    return_pct: float | None


@dataclass(frozen=True)
class AnalysisBatchResult:
    """Container for complete run outputs."""

    run_id: str
    generated_at: datetime
    analyses: list[StockAnalysis]
    trade_plans: list[TradePlan]
    portfolio_snapshots: list[PortfolioSnapshot]
    diagnostics: RunDiagnostics
    backtest_summary: BacktestSummary

    def to_dict(self) -> dict:
        """Return JSON-safe dictionary representation."""
        return {
            "run_id": self.run_id,
            "generated_at": self.generated_at.isoformat(),
            "analyses": [asdict(item) for item in self.analyses],
            "trade_plans": [asdict(item) for item in self.trade_plans],
            "portfolio_snapshots": [asdict(item) for item in self.portfolio_snapshots],
            "diagnostics": asdict(self.diagnostics),
            "backtest_summary": asdict(self.backtest_summary),
        }
