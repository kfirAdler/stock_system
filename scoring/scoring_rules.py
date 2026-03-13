from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class ScoringRules:
    """Deterministic and semi-granular per-row scoring logic."""

    @staticmethod
    def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
        return max(low, min(high, value))

    @staticmethod
    def _scale(value: float, in_min: float, in_max: float) -> float:
        if in_max <= in_min:
            return 0.0
        normalized = (value - in_min) / (in_max - in_min)
        return ScoringRules._clamp(normalized * 100.0)

    def trend_quality(self, row: pd.Series) -> float:
        close = float(row["Close"])
        ma20 = float(row["ma_20"])
        ma50 = float(row["ma_50"])
        ma_spread = (ma20 / ma50) - 1.0 if ma50 > 0 else 0.0
        price_extension = (close / ma20) - 1.0 if ma20 > 0 else 0.0

        base = 35.0
        if close >= ma20:
            base += 25.0
        if ma20 >= ma50:
            base += 20.0

        spread_score = self._scale(ma_spread, -0.06, 0.12) * 0.12
        extension_score = self._scale(price_extension, -0.10, 0.12) * 0.08
        return self._clamp(base + spread_score + extension_score)

    def momentum(self, row: pd.Series) -> float:
        rsi = float(row["rsi_14"])
        centered = 100.0 - (abs(rsi - 58.0) / 42.0) * 100.0
        return self._clamp(centered)

    def volume_quality(self, row: pd.Series) -> float:
        volume = float(row["Volume"])
        avg_volume = float(row["avg_volume_20"])
        if avg_volume <= 0:
            return 0.0

        ratio = volume / avg_volume
        return self._scale(ratio, 0.4, 1.8)

    def volatility_suitability(self, row: pd.Series) -> float:
        vol = float(row["volatility_20"])
        ideal = 0.025
        distance = abs(vol - ideal)
        score = 100.0 - (distance / 0.05) * 100.0
        return self._clamp(score)

    def proximity_to_highs(self, row: pd.Series) -> float:
        distance = float(row["dist_from_52w_high"])
        return self._scale(distance, -0.50, 0.0)

    def relative_strength(self, row: pd.Series) -> float:
        raw = row.get("relative_strength")
        if raw is None or pd.isna(raw):
            return 50.0
        return self._scale(float(raw), -0.30, 0.30)

    def weak_structure_penalty(self, row: pd.Series) -> float:
        close = float(row["Close"])
        ma20 = float(row["ma_20"])
        ma50 = float(row["ma_50"])
        daily_return = float(row["daily_return"])

        penalty = 0.0
        if close < ma50:
            penalty += 40.0
        if ma20 < ma50:
            penalty += 25.0
        if daily_return < -0.03:
            penalty += 35.0
        return self._clamp(penalty)

    def ma20_extension_penalty(self, row: pd.Series) -> float:
        """Penalize entries that are too extended above MA20."""
        distance = float(row.get("distance_from_ma20", 0.0))
        if distance <= 0.03:
            return 0.0
        if distance <= 0.05:
            return 12.0
        if distance <= 0.08:
            return 38.0
        if distance <= 0.10:
            return 70.0
        return 92.0

    def resistance_proximity_penalty(self, row: pd.Series) -> float:
        """Penalize setups that are too close to 20-day resistance."""
        distance = float(row.get("distance_to_resistance", 0.10))
        if distance > 0.05:
            return 0.0
        if distance >= 0.03:
            return 10.0
        if distance >= 0.02:
            return 28.0
        if distance >= 0.015:
            return 55.0
        return 88.0

    def parabolic_move_penalty(self, row: pd.Series) -> float:
        short_run_return = float(row.get("short_run_return_20", 0.0))
        distance_from_ma20 = float(row.get("distance_from_ma20", 0.0))
        if short_run_return > 0.20:
            return 92.0
        if short_run_return > 0.15:
            return 72.0
        if short_run_return > 0.12 and distance_from_ma20 > 0.05:
            return 48.0
        return 0.0

    def signal_flags(self, row: pd.Series) -> dict[str, bool]:
        relative_strength = row.get("relative_strength")
        distance_from_ma20 = float(row.get("distance_from_ma20", 0.0))
        distance_to_resistance = float(row.get("distance_to_resistance", 1.0))
        distance_to_support = float(row.get("distance_to_support", 1.0))
        distance_from_ma50 = self.distance_from_ma50(row)
        reward_risk = self.reward_risk_ratio(row)
        short_run_return = float(row.get("short_run_return_20", 0.0))
        return {
            "price_above_ma20": bool(float(row["Close"]) > float(row["ma_20"])),
            "price_above_ma50": bool(float(row["Close"]) > float(row["ma_50"])),
            "ma20_above_ma50": bool(float(row["ma_20"]) > float(row["ma_50"])),
            "rsi_above_50": bool(float(row["rsi_14"]) > 50.0),
            "relative_strength_positive": bool(not pd.isna(relative_strength) and float(relative_strength) > 0.0),
            "within_15pct_of_52w_high": bool(float(row["dist_from_52w_high"]) >= -0.15),
            "volume_above_20d_avg": bool(float(row["Volume"]) > float(row["avg_volume_20"])),
            "price_not_extended_vs_ma20": bool(distance_from_ma20 <= 0.05),
            "very_extended_ma20": bool(distance_from_ma20 > 0.08),
            "strongly_extended_ma20": bool(distance_from_ma20 > 0.10),
            "near_20d_resistance": bool(distance_to_resistance < 0.02),
            "too_close_20d_resistance": bool(distance_to_resistance < 0.015),
            "too_close_to_support": bool(distance_to_support <= 0.01),
            "within_10pct_ma50": bool(abs(distance_from_ma50) <= 0.10),
            "reward_risk_ok": bool(reward_risk >= 1.5),
            "parabolic_move": bool(short_run_return > 0.15),
        }

    @staticmethod
    def distance_from_ma50(row: pd.Series) -> float:
        close = float(row["Close"])
        ma50 = float(row["ma_50"])
        if ma50 <= 0:
            return 0.0
        return (close - ma50) / ma50

    def ma20_distance_score(self, row: pd.Series) -> float:
        distance = float(row.get("distance_from_ma20", 0.0))
        if distance <= 0.03:
            return 95.0
        if distance <= 0.05:
            return 80.0
        if distance <= 0.08:
            return 52.0
        if distance <= 0.10:
            return 25.0
        return 5.0

    def ma50_distance_score(self, row: pd.Series) -> float:
        distance = abs(self.distance_from_ma50(row))
        if distance <= 0.06:
            return 90.0
        if distance <= 0.10:
            return 72.0
        if distance <= 0.16:
            return 45.0
        return 20.0

    def sector_strength(self, row: pd.Series) -> float:
        value = float(row.get("sector_strength", 0.0))
        return self._scale(value, -0.12, 0.12)

    def resistance_room_score(self, row: pd.Series) -> float:
        distance = float(row.get("distance_to_resistance", 0.0))
        if distance > 0.05:
            return 92.0
        if distance >= 0.03:
            return 74.0
        if distance >= 0.02:
            return 52.0
        if distance >= 0.015:
            return 24.0
        return 6.0

    def support_quality_score(self, row: pd.Series) -> float:
        distance = float(row.get("distance_to_support", 0.0))
        if 0.015 <= distance <= 0.08:
            return 88.0
        if 0.01 <= distance < 0.015 or 0.08 < distance <= 0.12:
            return 65.0
        if distance < 0.01:
            return 35.0
        return 50.0

    def extension_score(self, row: pd.Series) -> float:
        distance = float(row.get("distance_from_ma20", 0.0))
        if distance <= 0.03:
            return 92.0
        if distance <= 0.05:
            return 74.0
        if distance <= 0.08:
            return 45.0
        if distance <= 0.10:
            return 20.0
        return 0.0

    def reward_risk_ratio(self, row: pd.Series) -> float:
        close = float(row["Close"])
        atr = row.get("atr_14")
        atr_val = float(atr) if atr is not None and not pd.isna(atr) else 0.0
        entry = close * 1.002
        stop = entry - (2.0 * atr_val) if atr_val > 0 else entry * 0.93
        target = entry * 1.12
        risk = max(0.01, entry - stop)
        reward = max(0.0, target - entry)
        return reward / risk

    def reward_risk_score(self, row: pd.Series) -> float:
        ratio = self.reward_risk_ratio(row)
        if ratio >= 2.0:
            return 95.0
        if ratio >= 1.5:
            return 75.0
        if ratio >= 1.2:
            return 45.0
        return 10.0

    def volume_confirmation_score(self, row: pd.Series) -> float:
        volume = float(row["Volume"])
        avg = float(row["avg_volume_20"])
        if avg <= 0:
            return 0.0
        ratio = volume / avg
        if ratio >= 1.3:
            return 92.0
        if ratio >= 1.0:
            return 70.0
        if ratio >= 0.85:
            return 48.0
        return 22.0
