from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from config.settings import ScoringThresholds
from domain.enums import BuyabilityStatus, SignalClassification
from domain.models import ScoreBreakdown
from scoring.score_weights import ScoreWeights
from scoring.scoring_rules import ScoringRules


@dataclass
class StockScorer:
    """Calculates weighted deterministic scores and classifications."""

    rules: ScoringRules
    weights: ScoreWeights
    thresholds: ScoringThresholds

    def score_row(self, row: pd.Series) -> ScoreBreakdown:
        setup_components = {
            "trend_quality": self.rules.trend_quality(row),
            "momentum": self.rules.momentum(row),
            "volume_quality": self.rules.volume_quality(row),
            "volatility_suitability": self.rules.volatility_suitability(row),
            "proximity_to_highs": self.rules.proximity_to_highs(row),
            "relative_strength": self.rules.relative_strength(row),
            "sector_strength": self.rules.sector_strength(row),
        }
        entry_components = {
            "ma20_distance_quality": self.rules.ma20_distance_score(row),
            "ma50_distance_quality": self.rules.ma50_distance_score(row),
            "resistance_room_quality": self.rules.resistance_room_score(row),
            "support_quality": self.rules.support_quality_score(row),
            "volume_confirmation_quality": self.rules.volume_confirmation_score(row),
            "extension_quality": self.rules.extension_score(row),
            "reward_risk_quality": self.rules.reward_risk_score(row),
        }
        reward_risk_ratio = round(self.rules.reward_risk_ratio(row), 4)

        penalties_raw = {
            "weak_structure": self.rules.weak_structure_penalty(row),
            "ma20_extension": self.rules.ma20_extension_penalty(row),
            "resistance_proximity": self.rules.resistance_proximity_penalty(row),
            "parabolic_move": self.rules.parabolic_move_penalty(row),
        }

        setup_quality_score = round(
            self._weighted_average(
                values=[
                    (setup_components["trend_quality"], self.weights.trend_quality),
                    (setup_components["momentum"], self.weights.momentum),
                    (setup_components["volume_quality"], self.weights.volume_quality),
                    (setup_components["volatility_suitability"], self.weights.volatility_suitability),
                    (setup_components["proximity_to_highs"], self.weights.proximity_to_highs),
                    (setup_components["relative_strength"], self.weights.relative_strength),
                    (setup_components["sector_strength"], self.weights.sector_strength),
                ]
            ),
            2,
        )

        entry_timing = self._weighted_average(
            values=[
                (entry_components["ma20_distance_quality"], self.weights.ma20_distance),
                (entry_components["ma50_distance_quality"], self.weights.ma50_distance),
                (entry_components["resistance_room_quality"], self.weights.resistance_room),
                (entry_components["support_quality"], self.weights.support_quality),
                (entry_components["volume_confirmation_quality"], self.weights.volume_confirmation),
                (entry_components["extension_quality"], self.weights.extension),
                (entry_components["reward_risk_quality"], self.weights.reward_risk),
            ]
        )
        entry_penalty = (
            penalties_raw["ma20_extension"] * self.weights.ma20_extension_penalty
            + penalties_raw["resistance_proximity"] * self.weights.resistance_proximity_penalty
            + penalties_raw["parabolic_move"] * self.weights.parabolic_penalty
        )
        entry_timing_score = round(max(0.0, min(100.0, entry_timing - entry_penalty)), 2)

        score_debug = {
            "trend_quality_weighted": round(setup_components["trend_quality"] * self.weights.trend_quality, 4),
            "momentum_weighted": round(setup_components["momentum"] * self.weights.momentum, 4),
            "volume_quality_weighted": round(setup_components["volume_quality"] * self.weights.volume_quality, 4),
            "volatility_suitability_weighted": round(
                setup_components["volatility_suitability"] * self.weights.volatility_suitability,
                4,
            ),
            "proximity_to_highs_weighted": round(
                setup_components["proximity_to_highs"] * self.weights.proximity_to_highs,
                4,
            ),
            "relative_strength_weighted": round(
                setup_components["relative_strength"] * self.weights.relative_strength,
                4,
            ),
            "sector_strength_weighted": round(
                setup_components["sector_strength"] * self.weights.sector_strength,
                4,
            ),
            "ma20_distance_quality_weighted": round(
                entry_components["ma20_distance_quality"] * self.weights.ma20_distance,
                4,
            ),
            "ma50_distance_quality_weighted": round(
                entry_components["ma50_distance_quality"] * self.weights.ma50_distance,
                4,
            ),
            "resistance_room_quality_weighted": round(
                entry_components["resistance_room_quality"] * self.weights.resistance_room,
                4,
            ),
            "support_quality_weighted": round(
                entry_components["support_quality"] * self.weights.support_quality,
                4,
            ),
            "volume_confirmation_quality_weighted": round(
                entry_components["volume_confirmation_quality"] * self.weights.volume_confirmation,
                4,
            ),
            "extension_quality_weighted": round(
                entry_components["extension_quality"] * self.weights.extension,
                4,
            ),
            "reward_risk_quality_weighted": round(
                entry_components["reward_risk_quality"] * self.weights.reward_risk,
                4,
            ),
            "weak_structure_penalty_weighted": round(
                -penalties_raw["weak_structure"] * self.weights.weak_structure_penalty,
                4,
            ),
            "ma20_extension_penalty_weighted": round(
                -penalties_raw["ma20_extension"] * self.weights.ma20_extension_penalty,
                4,
            ),
            "resistance_proximity_penalty_weighted": round(
                -penalties_raw["resistance_proximity"] * self.weights.resistance_proximity_penalty,
                4,
            ),
            "parabolic_move_penalty_weighted": round(
                -penalties_raw["parabolic_move"] * self.weights.parabolic_penalty,
                4,
            ),
        }

        penalties = {
            "weak_structure": score_debug["weak_structure_penalty_weighted"],
            "ma20_extension": score_debug["ma20_extension_penalty_weighted"],
            "resistance_proximity": score_debug["resistance_proximity_penalty_weighted"],
            "parabolic_move": score_debug["parabolic_move_penalty_weighted"],
        }

        weighted_total = (setup_quality_score * 0.58) + (entry_timing_score * 0.42) + penalties["weak_structure"]
        total_score = round(max(0.0, min(100.0, weighted_total)), 2)
        signal_flags = self.rules.signal_flags(row)
        reasons = self._reasons(setup_components, penalties_raw, signal_flags, entry_timing_score, reward_risk_ratio)

        return ScoreBreakdown(
            total_score=total_score,
            component_scores={k: round(v, 4) for k, v in (setup_components | entry_components).items()},
            penalties=penalties,
            score_debug=score_debug,
            signal_flags=signal_flags,
            reasons=reasons,
            setup_quality_score=setup_quality_score,
            entry_timing_score=entry_timing_score,
            reward_risk_ratio=reward_risk_ratio,
        )

    def classify(self, score: ScoreBreakdown) -> SignalClassification:
        if score.signal_flags.get("strongly_extended_ma20", False) and score.setup_quality_score >= self.thresholds.watch_setup_min:
            return SignalClassification.WATCH
        if self._is_buy_candidate(score):
            return SignalClassification.BUY
        if (
            score.setup_quality_score >= self.thresholds.watch_setup_min
            and score.entry_timing_score >= self.thresholds.watch_entry_min
            and score.total_score >= self.thresholds.watch_total
        ):
            return SignalClassification.WATCH
        return SignalClassification.AVOID

    def buyability(self, score: ScoreBreakdown) -> tuple[BuyabilityStatus, str]:
        flags = score.signal_flags
        if self._is_buy_candidate(score):
            return BuyabilityStatus.BUYABLE_NOW, "Setup is clean and buyable now."

        if score.setup_quality_score >= self.thresholds.buy_setup_min:
            if flags.get("parabolic_move", False):
                return (
                    BuyabilityStatus.WAIT_FOR_PULLBACK,
                    "Stock is overheated after a sharp short-term move. Wait for pullback.",
                )
            if flags.get("strongly_extended_ma20", False):
                return (
                    BuyabilityStatus.WAIT_FOR_PULLBACK,
                    "Stock is strongly extended above MA20 and should cool off first.",
                )
            if not flags.get("price_not_extended_vs_ma20", True):
                return (
                    BuyabilityStatus.WAIT_FOR_PULLBACK,
                    "Setup is strong, but price is extended above MA20.",
                )
            if flags.get("too_close_20d_resistance", False) or flags.get("near_20d_resistance", False):
                return (
                    BuyabilityStatus.WAIT_FOR_BREAKOUT_CONFIRMATION,
                    "Setup is strong, but price is too close to 20-day resistance.",
                )
            if score.component_scores.get("volume_quality", 0.0) < 40 and (
                flags.get("within_15pct_of_52w_high", False) or flags.get("near_20d_resistance", False)
            ):
                return (
                    BuyabilityStatus.WATCH_ONLY,
                    "Price location is constructive, but volume confirmation is too weak for entry now.",
                )
            if score.reward_risk_ratio < self.thresholds.min_reward_risk_for_buy:
                return (
                    BuyabilityStatus.WATCH_ONLY,
                    "Setup is strong, but reward-to-risk is not attractive enough yet.",
                )
            return BuyabilityStatus.WATCH_ONLY, "Setup is strong, but timing is not clean enough yet."

        if score.setup_quality_score >= self.thresholds.watch_setup_min:
            return BuyabilityStatus.WATCH_ONLY, "Technical structure is mixed; monitor for improvement."

        return BuyabilityStatus.AVOID, "Setup quality and timing are not actionable."

    def _is_buy_candidate(self, score: ScoreBreakdown) -> bool:
        flags = score.signal_flags
        if score.setup_quality_score < self.thresholds.buy_setup_min:
            return False
        if score.entry_timing_score < self.thresholds.buy_entry_min:
            return False
        if score.total_score < self.thresholds.buy_total:
            return False
        if score.reward_risk_ratio < self.thresholds.min_reward_risk_for_buy:
            return False
        if score.reward_risk_ratio < self.thresholds.hard_min_reward_risk:
            return False
        if flags.get("parabolic_move", False):
            return False
        if flags.get("near_20d_resistance", False):
            return False
        if flags.get("too_close_20d_resistance", False):
            return False
        if flags.get("very_extended_ma20", False):
            return False
        if not flags.get("price_not_extended_vs_ma20", True):
            return False
        if score.component_scores.get("volume_quality", 0.0) < 40 and flags.get("within_15pct_of_52w_high", False):
            return False
        return True

    def _reasons(
        self,
        component_scores: dict[str, float],
        penalties_raw: dict[str, float],
        flags: dict[str, bool],
        entry_timing_score: float,
        reward_risk_ratio: float,
    ) -> list[str]:
        reasons: list[str] = []

        if flags["price_above_ma20"] and flags["ma20_above_ma50"]:
            reasons.append("trend_supportive")
        if component_scores["momentum"] >= 65:
            reasons.append("momentum_constructive")
        if flags["volume_above_20d_avg"]:
            reasons.append("volume_confirmation")
        if flags["within_15pct_of_52w_high"]:
            reasons.append("near_52w_high")
        if flags["relative_strength_positive"]:
            reasons.append("relative_strength_positive")
        if penalties_raw["weak_structure"] >= 60:
            reasons.append("weak_structure_penalty")
        if penalties_raw["ma20_extension"] >= 20:
            reasons.append("price_extended_from_ma20")
        if penalties_raw["resistance_proximity"] >= 25:
            reasons.append("too_close_to_resistance")
        if penalties_raw["parabolic_move"] >= 45:
            reasons.append("parabolic_move_penalty")
        if entry_timing_score < 55:
            reasons.append("entry_timing_weak")
        if reward_risk_ratio < 1.5:
            reasons.append("reward_risk_weak")
        if not reasons:
            reasons.append("mixed_signal")

        return reasons

    @staticmethod
    def _weighted_average(values: list[tuple[float, float]]) -> float:
        total_weight = sum(weight for _, weight in values)
        if total_weight <= 0:
            return 0.0
        weighted_sum = sum(value * weight for value, weight in values)
        return max(0.0, min(100.0, weighted_sum / total_weight))
