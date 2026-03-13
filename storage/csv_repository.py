from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from domain.models import PortfolioSnapshot


@dataclass
class AnalysisResultCsvRepository:
    """Persists tabular run artifacts as CSV files."""

    analyses_dir: Path
    portfolios_dir: Path

    def save_portfolio_snapshots(self, snapshots: list[PortfolioSnapshot], run_id: str) -> Path:
        run_dir = self._ensure_run_dir(self.portfolios_dir, run_id)
        rows: list[dict] = []

        for snapshot in snapshots:
            rows.append(
                {
                    "snapshot_date": snapshot.snapshot_date,
                    "cash": snapshot.cash,
                    "equity": snapshot.equity,
                    "open_positions": len(snapshot.open_positions),
                }
            )

        output_file = run_dir / "portfolio_snapshots.csv"
        pd.DataFrame(rows).to_csv(output_file, index=False)
        return output_file

    @staticmethod
    def _ensure_run_dir(root: Path, run_id: str) -> Path:
        run_dir = root / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir
