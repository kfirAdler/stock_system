from __future__ import annotations

import pandas as pd

from config.settings import ScoringThresholds
from domain.enums import SignalClassification
from scoring.score_weights import ScoreWeights
from scoring.scoring_rules import ScoringRules
from scoring.stock_scorer import StockScorer


def test_stock_scorer_returns_breakdown_and_classification() -> None:
    scorer = StockScorer(
        rules=ScoringRules(),
        weights=ScoreWeights(),
        thresholds=ScoringThresholds(buy_total=70, watch_total=50),
    )
    row = pd.Series(
        {
            "Close": 120.0,
            "ma_20": 115.0,
            "ma_50": 110.0,
            "rsi_14": 60.0,
            "Volume": 2_000_000.0,
            "avg_volume_20": 1_200_000.0,
            "volatility_20": 0.02,
            "dist_from_52w_high": -0.03,
            "distance_from_ma20": 0.02,
            "distance_to_resistance": 0.04,
            "daily_return": 0.01,
        }
    )

    breakdown = scorer.score_row(row)
    classification = scorer.classify(breakdown)

    assert 0 <= breakdown.total_score <= 100
    assert "trend_quality" in breakdown.component_scores
    assert "weak_structure" in breakdown.penalties
    assert "trend_quality_weighted" in breakdown.score_debug
    assert "price_above_ma20" in breakdown.signal_flags
    assert 0 <= breakdown.setup_quality_score <= 100
    assert 0 <= breakdown.entry_timing_score <= 100
    assert classification in {
        SignalClassification.BUY,
        SignalClassification.WATCH,
        SignalClassification.AVOID,
    }
