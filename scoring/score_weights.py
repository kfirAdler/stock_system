from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScoreWeights:
    """Weights for deterministic stock scoring components."""

    trend_quality: float = 0.22
    momentum: float = 0.20
    volume_quality: float = 0.13
    volatility_suitability: float = 0.15
    proximity_to_highs: float = 0.14
    relative_strength: float = 0.16
    sector_strength: float = 0.10
    weak_structure_penalty: float = 0.10
    ma20_extension_penalty: float = 0.07
    resistance_proximity_penalty: float = 0.08
    parabolic_penalty: float = 0.10
    ma20_distance: float = 0.24
    ma50_distance: float = 0.15
    resistance_room: float = 0.24
    support_quality: float = 0.10
    volume_confirmation: float = 0.08
    extension: float = 0.12
    reward_risk: float = 0.15
