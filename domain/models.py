from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from domain.enums import BuyabilityStatus, SignalClassification


@dataclass(frozen=True)
class ScoreBreakdown:
    """Detailed deterministic scoring components for one stock/date."""

    total_score: float
    component_scores: dict[str, float]
    penalties: dict[str, float]
    score_debug: dict[str, float]
    signal_flags: dict[str, bool]
    reasons: list[str]
    setup_quality_score: float
    entry_timing_score: float
    reward_risk_ratio: float


@dataclass(frozen=True)
class StockAnalysis:
    """Represents the final analysis state for one ticker."""

    ticker: str
    as_of_date: date
    latest_close: float
    score: ScoreBreakdown
    classification: SignalClassification
    score_debug: dict[str, float] = field(default_factory=dict)
    signal_flags: dict[str, bool] = field(default_factory=dict)
    feature_snapshot: dict[str, Any] = field(default_factory=dict)
    buyability_status: BuyabilityStatus = BuyabilityStatus.AVOID
    buyability_reason: str = ""
    sector: str = "Unknown"


@dataclass(frozen=True)
class TradePlan:
    """Represents an action plan generated from analysis output."""

    ticker: str
    as_of_date: date
    latest_close: float
    score: float
    classification: SignalClassification
    suggested_entry: float
    suggested_stop_loss: float
    suggested_first_target: float
    reward_risk_ratio: float
    buyability_status: BuyabilityStatus
    buyability_reason: str
    sector: str
    reasons: list[str]


@dataclass(frozen=True)
class PositionSnapshot:
    """State snapshot for one open position."""

    ticker: str
    shares: int
    entry_price: float
    current_price: float
    market_value: float
    stop_loss: float
    take_profit: float


@dataclass(frozen=True)
class PortfolioSnapshot:
    """Point-in-time portfolio summary."""

    snapshot_date: date
    cash: float
    equity: float
    open_positions: list[PositionSnapshot]
