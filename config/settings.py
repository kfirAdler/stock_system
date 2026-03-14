from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path


def _env_bool(name: str, default: bool = False) -> bool:
    """Parse a boolean environment variable with tolerant truthy values."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class ScoringThresholds:
    """Thresholds used to classify scored stocks."""

    buy_total: float = 82.0
    watch_total: float = 54.0
    buy_setup_min: float = 76.0
    buy_entry_min: float = 72.0
    watch_setup_min: float = 60.0
    watch_entry_min: float = 42.0
    min_reward_risk_for_buy: float = 1.5
    hard_min_reward_risk: float = 1.2


@dataclass(frozen=True)
class SimulationSettings:
    """Configuration for portfolio simulation."""

    initial_capital: float = 100_000.0
    max_position_fraction: float = 0.20
    max_open_positions: int = 5
    take_profit_pct: float = 0.12
    stop_loss_pct: float = 0.07
    backtest_start_date: date = field(default_factory=lambda: date.today() - timedelta(days=365 * 3))
    backtest_end_date: date = field(default_factory=date.today)


@dataclass(frozen=True)
class AppSettings:
    """Application-level settings and file locations."""

    project_root: Path = field(default_factory=lambda: Path(__file__).resolve().parents[1])
    tickers_file: Path = field(init=False)
    output_dir: Path = field(init=False)
    raw_data_dir: Path = field(init=False)
    analyses_dir: Path = field(init=False)
    portfolios_dir: Path = field(init=False)
    benchmark_symbol: str = "SPY"
    universe_source: str = "sp500"
    universe_size: int = 500
    sp500_constituents_url: str = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
    data_provider_mode: str = field(default_factory=lambda: os.environ.get("DATA_PROVIDER_MODE", "auto").lower())
    stooq_interval: str = "d"
    cache_enabled: bool = field(default_factory=lambda: _env_bool("CACHE_ENABLED", True))
    force_refresh: bool = field(default_factory=lambda: _env_bool("FORCE_REFRESH", False))
    save_to_supabase: bool = field(default_factory=lambda: _env_bool("SAVE_TO_SUPABASE", False))
    supabase_db_url: str | None = field(default_factory=lambda: os.environ.get("SUPABASE_DB_URL"))
    min_history_rows: int = 120
    rolling_volume_window: int = 20
    ma_short_window: int = 20
    ma_long_window: int = 50
    rsi_window: int = 14
    volatility_window: int = 20
    high_window: int = 252
    relative_strength_window: int = 60
    thresholds: ScoringThresholds = field(default_factory=ScoringThresholds)
    simulation: SimulationSettings = field(default_factory=SimulationSettings)

    def __post_init__(self) -> None:
        output_dir_env = os.environ.get("OUTPUT_DIR", "").strip()
        output_root = Path(output_dir_env).expanduser() if output_dir_env else (self.project_root / "output")
        object.__setattr__(self, "tickers_file", self.project_root / "config" / "tickers.json")
        object.__setattr__(self, "output_dir", output_root)
        object.__setattr__(self, "raw_data_dir", self.output_dir / "raw_data")
        object.__setattr__(self, "analyses_dir", self.output_dir / "analyses")
        object.__setattr__(self, "portfolios_dir", self.output_dir / "portfolios")
        if self.save_to_supabase and not (self.supabase_db_url and self.supabase_db_url.strip()):
            raise ValueError("SAVE_TO_SUPABASE=true requires SUPABASE_DB_URL to be set")

    def ensure_directories(self) -> None:
        """Create runtime directories if they do not already exist."""
        if self.save_to_supabase:
            # Strict Supabase mode: no local analysis/raw-data directory requirement.
            return
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.analyses_dir.mkdir(parents=True, exist_ok=True)
        self.portfolios_dir.mkdir(parents=True, exist_ok=True)
