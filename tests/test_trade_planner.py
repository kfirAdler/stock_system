from __future__ import annotations

from datetime import date

from domain.enums import BuyabilityStatus, SignalClassification
from domain.models import ScoreBreakdown, StockAnalysis
from planning.trade_planner import TradePlanner


def test_trade_planner_generates_consistent_risk_levels() -> None:
    analysis = StockAnalysis(
        ticker="AAPL",
        as_of_date=date(2026, 1, 20),
        latest_close=200.0,
        score=ScoreBreakdown(
            total_score=81.5,
            component_scores={"trend_quality": 90.0},
            penalties={"weak_structure": -2.0},
            score_debug={"trend_quality_weighted": 18.0},
            signal_flags={"price_above_ma20": True},
            reasons=["trend_supportive"],
            setup_quality_score=80.0,
            entry_timing_score=78.0,
            reward_risk_ratio=2.0,
        ),
        classification=SignalClassification.BUY,
        score_debug={"trend_quality_weighted": 18.0},
        signal_flags={"price_above_ma20": True},
        feature_snapshot={},
        buyability_status=BuyabilityStatus.BUYABLE_NOW,
        buyability_reason="Clean setup",
        sector="Technology",
    )

    planner = TradePlanner(stop_loss_pct=0.1, target_pct=0.2, entry_buffer_pct=0.0)
    plan = planner.build_plan(analysis)

    assert plan.suggested_entry == 200.0
    assert plan.suggested_stop_loss == 180.0
    assert plan.suggested_first_target == 240.0
    assert plan.classification == SignalClassification.BUY


def test_trade_planner_excludes_avoid_from_actionable_plans() -> None:
    planner = TradePlanner()
    avoid_analysis = StockAnalysis(
        ticker="TSLA",
        as_of_date=date(2026, 1, 20),
        latest_close=180.0,
        score=ScoreBreakdown(
            total_score=22.0,
            component_scores={"trend_quality": 20.0},
            penalties={"weak_structure": -10.0},
            score_debug={"trend_quality_weighted": 4.0},
            signal_flags={"price_above_ma20": False},
            reasons=["mixed_signal"],
            setup_quality_score=24.0,
            entry_timing_score=30.0,
            reward_risk_ratio=0.9,
        ),
        classification=SignalClassification.AVOID,
        score_debug={"trend_quality_weighted": 4.0},
        signal_flags={"price_above_ma20": False},
        feature_snapshot={},
        buyability_status=BuyabilityStatus.AVOID,
        buyability_reason="Weak setup",
        sector="Consumer",
    )

    plans = planner.build_plans([avoid_analysis])
    assert plans == []


def test_trade_planner_uses_atr_based_stop_when_available() -> None:
    planner = TradePlanner(stop_loss_pct=0.1, target_pct=0.2, entry_buffer_pct=0.0)
    analysis = StockAnalysis(
        ticker="NVDA",
        as_of_date=date(2026, 1, 20),
        latest_close=120.0,
        score=ScoreBreakdown(
            total_score=75.0,
            component_scores={"trend_quality": 80.0},
            penalties={"weak_structure": -1.0},
            score_debug={"trend_quality_weighted": 16.0},
            signal_flags={"price_above_ma20": True},
            reasons=["trend_supportive"],
            setup_quality_score=77.0,
            entry_timing_score=72.0,
            reward_risk_ratio=1.9,
        ),
        classification=SignalClassification.BUY,
        score_debug={"trend_quality_weighted": 16.0},
        signal_flags={"price_above_ma20": True},
        feature_snapshot={"atr_14": 3.0},
        buyability_status=BuyabilityStatus.BUYABLE_NOW,
        buyability_reason="Clean setup",
        sector="Technology",
    )

    plan = planner.build_plan(analysis)
    assert plan.suggested_entry == 120.0
    assert plan.suggested_stop_loss == 114.0
