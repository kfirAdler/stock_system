from __future__ import annotations

from dataclasses import dataclass

from domain.enums import BuyabilityStatus, SignalClassification
from domain.models import StockAnalysis, TradePlan


@dataclass
class TradePlanner:
    """Builds simple deterministic trade plans from analyzed stocks."""

    stop_loss_pct: float = 0.07
    target_pct: float = 0.12
    entry_buffer_pct: float = 0.002

    def build_plan(self, analysis: StockAnalysis) -> TradePlan:
        close = analysis.latest_close
        entry = round(close * (1 + self.entry_buffer_pct), 2)
        atr_14 = analysis.feature_snapshot.get("atr_14")
        if isinstance(atr_14, (int, float)) and atr_14 > 0:
            stop_loss = round(entry - (2 * float(atr_14)), 2)
        else:
            stop_loss = round(entry * (1 - self.stop_loss_pct), 2)
        first_target = round(entry * (1 + self.target_pct), 2)
        risk = max(0.01, entry - stop_loss)
        reward = max(0.0, first_target - entry)
        reward_risk = round(reward / risk, 4)

        return TradePlan(
            ticker=analysis.ticker,
            as_of_date=analysis.as_of_date,
            latest_close=close,
            score=analysis.score.total_score,
            classification=analysis.classification,
            suggested_entry=entry,
            suggested_stop_loss=stop_loss,
            suggested_first_target=first_target,
            reward_risk_ratio=reward_risk,
            buyability_status=analysis.buyability_status,
            buyability_reason=analysis.buyability_reason,
            sector=analysis.sector,
            reasons=analysis.score.reasons,
        )

    def build_plans(self, analyses: list[StockAnalysis]) -> list[TradePlan]:
        sorted_items = sorted(analyses, key=lambda item: item.score.total_score, reverse=True)
        actionable = [
            item
            for item in sorted_items
            if item.classification is SignalClassification.BUY and item.buyability_status is BuyabilityStatus.BUYABLE_NOW
        ]
        return [self.build_plan(item) for item in actionable]
