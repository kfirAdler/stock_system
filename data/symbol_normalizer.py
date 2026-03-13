from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SymbolNormalizer:
    """Normalizes symbols to provider-specific formats."""

    def to_stooq_symbol(self, ticker: str) -> str:
        """Convert generic ticker to Stooq ticker format for US symbols."""
        normalized = ticker.strip().lower().replace(".", "-")
        if normalized.endswith(".us"):
            return normalized
        return f"{normalized}.us"
