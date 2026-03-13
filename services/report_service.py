from __future__ import annotations

from dataclasses import dataclass

from domain.result_models import AnalysisBatchResult


@dataclass
class ReportService:
    """Formats short console output for run summaries."""

    def render(self, result: AnalysisBatchResult, top_n: int = 10) -> str:
        lines = [
            f"Run ID: {result.run_id}",
            f"Generated: {result.generated_at.isoformat()}",
            (
                "Diagnostics: "
                f"total={result.diagnostics.total_tickers_analyzed} "
                f"BUY={result.diagnostics.buy_count} "
                f"WATCH={result.diagnostics.watch_count} "
                f"AVOID={result.diagnostics.avoid_count} "
                f"avg_score={result.diagnostics.average_total_score:.2f}"
            ),
            f"Top 5: {', '.join(result.diagnostics.top_5_tickers) if result.diagnostics.top_5_tickers else '-'}",
            (
                f"Bottom 5: {', '.join(result.diagnostics.bottom_5_tickers) if result.diagnostics.bottom_5_tickers else '-'}"
            ),
            "Top opportunities:",
        ]

        for item in result.analyses[:top_n]:
            lines.append(
                f"- {item.ticker}: score={item.score.total_score:.2f} "
                f"classification={item.classification.value} close={item.latest_close:.2f}"
            )

        if result.portfolio_snapshots:
            final_snapshot = result.portfolio_snapshots[-1]
            lines.append(
                f"Final portfolio equity: {final_snapshot.equity:.2f} "
                f"(cash={final_snapshot.cash:.2f}, open_positions={len(final_snapshot.open_positions)})"
            )
            lines.append(
                "Backtest window: "
                f"{result.backtest_summary.first_snapshot_date} -> {result.backtest_summary.last_snapshot_date} "
                f"(snapshots={result.backtest_summary.snapshot_count})"
            )

        return "\n".join(lines)
