from __future__ import annotations

import logging

from config.settings import AppSettings
from config.universe_loader import TickerUniverseLoader
from data.market_data_service import MarketDataService
from data.stooq_provider import StooqDataProvider
from data.symbol_normalizer import SymbolNormalizer
from features.feature_calculator import FeatureCalculator
from features.feature_validator import FeatureValidator
from planning.trade_planner import TradePlanner
from portfolio.simulator import PortfolioSimulator
from scoring.score_weights import ScoreWeights
from scoring.scoring_rules import ScoringRules
from scoring.stock_scorer import StockScorer
from services.analysis_runner import AnalysisRunner
from services.report_service import ReportService
from storage.csv_repository import AnalysisResultCsvRepository
from storage.json_repository import AnalysisResultJsonRepository
from storage.supabase_postgres_repository import SupabasePostgresRepository


def build_runner(settings: AppSettings) -> AnalysisRunner:
    normalizer = SymbolNormalizer()
    provider = StooqDataProvider(normalizer=normalizer, interval=settings.stooq_interval)
    market_data_service = MarketDataService(
        provider=provider,
        raw_data_dir=settings.raw_data_dir,
        min_history_rows=settings.min_history_rows,
        cache_enabled=settings.cache_enabled,
        force_refresh=settings.force_refresh,
    )

    scorer = StockScorer(
        rules=ScoringRules(),
        weights=ScoreWeights(),
        thresholds=settings.thresholds,
    )

    return AnalysisRunner(
        universe_loader=TickerUniverseLoader(
            source_file=settings.tickers_file,
            universe_source=settings.universe_source,
            universe_size=settings.universe_size,
            sp500_constituents_url=settings.sp500_constituents_url,
        ),
        market_data_service=market_data_service,
        feature_calculator=FeatureCalculator(settings=settings),
        feature_validator=FeatureValidator(),
        scorer=scorer,
        planner=TradePlanner(
            stop_loss_pct=settings.simulation.stop_loss_pct,
            target_pct=settings.simulation.take_profit_pct,
        ),
        simulator=PortfolioSimulator(scorer=scorer, settings=settings.simulation),
        csv_repository=AnalysisResultCsvRepository(
            analyses_dir=settings.analyses_dir,
            portfolios_dir=settings.portfolios_dir,
        ),
        json_repository=AnalysisResultJsonRepository(
            analyses_dir=settings.analyses_dir,
            portfolios_dir=settings.portfolios_dir,
        ),
        supabase_repository=SupabasePostgresRepository(
            db_url=settings.supabase_db_url if settings.save_to_supabase else None,
        ),
        benchmark_symbol=settings.benchmark_symbol,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    settings = AppSettings()
    settings.ensure_directories()

    runner = build_runner(settings)
    result = runner.run()

    report = ReportService().render(result)
    print(report)


if __name__ == "__main__":
    main()
