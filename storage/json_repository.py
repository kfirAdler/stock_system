from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from domain.result_models import AnalysisBatchResult


@dataclass
class AnalysisResultJsonRepository:
    """Persists structured run artifacts as separate JSON files."""

    analyses_dir: Path
    portfolios_dir: Path

    def save_run_artifacts(self, batch: AnalysisBatchResult) -> dict[str, Path]:
        analysis_dir = self._ensure_run_dir(self.analyses_dir, batch.run_id)
        portfolio_dir = self._ensure_run_dir(self.portfolios_dir, batch.run_id)

        current_analysis_path = analysis_dir / "current_analysis.json"
        trade_plans_path = analysis_dir / "trade_plans.json"
        diagnostics_path = analysis_dir / "run_diagnostics.json"
        backtest_summary_path = portfolio_dir / "backtest_summary.json"

        self._write_json(current_analysis_path, [asdict(item) for item in batch.analyses])
        self._write_json(trade_plans_path, [asdict(item) for item in batch.trade_plans])
        self._write_json(diagnostics_path, asdict(batch.diagnostics))
        self._write_json(backtest_summary_path, asdict(batch.backtest_summary))

        return {
            "current_analysis": current_analysis_path,
            "trade_plans": trade_plans_path,
            "run_diagnostics": diagnostics_path,
            "backtest_summary": backtest_summary_path,
        }

    @staticmethod
    def _ensure_run_dir(root: Path, run_id: str) -> Path:
        run_dir = root / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    @staticmethod
    def _write_json(path: Path, payload: object) -> None:
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, default=str)
