from __future__ import annotations

from enum import Enum


class SignalClassification(str, Enum):
    """Classification labels produced by scoring."""

    BUY = "BUY"
    WATCH = "WATCH"
    AVOID = "AVOID"


class BuyabilityStatus(str, Enum):
    """Actionability labels for current entry quality."""

    BUYABLE_NOW = "BUYABLE_NOW"
    WAIT_FOR_PULLBACK = "WAIT_FOR_PULLBACK"
    WAIT_FOR_BREAKOUT_CONFIRMATION = "WAIT_FOR_BREAKOUT_CONFIRMATION"
    WATCH_ONLY = "WATCH_ONLY"
    AVOID = "AVOID"
