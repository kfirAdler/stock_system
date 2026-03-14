from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from domain.result_models import AnalysisBatchResult


@dataclass
class SupabasePostgresRepository:
    """Persists and reads scanner/simulator data from Supabase Postgres."""

    db_url: str | None

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._sim_schema_ready = False

    @property
    def enabled(self) -> bool:
        return bool(self.db_url and self.db_url.strip())

    def healthcheck(self) -> None:
        """Raise if DB cannot be reached."""
        if not self.enabled:
            raise RuntimeError("Supabase repository is disabled")
        with psycopg.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("select 1")

    def save_analysis_batch(self, batch: AnalysisBatchResult) -> None:
        if not self.enabled:
            return

        with psycopg.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into scanner_runs (
                        run_id,
                        generated_at,
                        total_tickers_analyzed,
                        buy_count,
                        watch_count,
                        avoid_count,
                        average_total_score,
                        top_5_tickers,
                        bottom_5_tickers,
                        sector_concentration_warning,
                        top_buy_sectors,
                        backtest_summary
                    ) values (
                        %(run_id)s,
                        %(generated_at)s,
                        %(total_tickers_analyzed)s,
                        %(buy_count)s,
                        %(watch_count)s,
                        %(avoid_count)s,
                        %(average_total_score)s,
                        %(top_5_tickers)s::jsonb,
                        %(bottom_5_tickers)s::jsonb,
                        %(sector_concentration_warning)s,
                        %(top_buy_sectors)s::jsonb,
                        %(backtest_summary)s::jsonb
                    )
                    returning id
                    """,
                    {
                        "run_id": batch.run_id,
                        "generated_at": batch.generated_at,
                        "total_tickers_analyzed": batch.diagnostics.total_tickers_analyzed,
                        "buy_count": batch.diagnostics.buy_count,
                        "watch_count": batch.diagnostics.watch_count,
                        "avoid_count": batch.diagnostics.avoid_count,
                        "average_total_score": batch.diagnostics.average_total_score,
                        "top_5_tickers": json.dumps(batch.diagnostics.top_5_tickers),
                        "bottom_5_tickers": json.dumps(batch.diagnostics.bottom_5_tickers),
                        "sector_concentration_warning": batch.diagnostics.sector_concentration_warning,
                        "top_buy_sectors": json.dumps(batch.diagnostics.top_buy_sectors),
                        "backtest_summary": json.dumps(asdict(batch.backtest_summary), default=str),
                    },
                )
                run_pk = cur.fetchone()[0]

                for item in batch.analyses:
                    cur.execute(
                        """
                        insert into scanner_analyses (
                            run_pk,
                            ticker,
                            as_of_date,
                            latest_close,
                            total_score,
                            setup_quality_score,
                            entry_timing_score,
                            reward_risk_ratio,
                            classification,
                            buyability_status,
                            buyability_reason,
                            sector,
                            reasons,
                            signal_flags,
                            score_debug,
                            component_scores,
                            penalties,
                            feature_snapshot
                        ) values (
                            %(run_pk)s,
                            %(ticker)s,
                            %(as_of_date)s,
                            %(latest_close)s,
                            %(total_score)s,
                            %(setup_quality_score)s,
                            %(entry_timing_score)s,
                            %(reward_risk_ratio)s,
                            %(classification)s,
                            %(buyability_status)s,
                            %(buyability_reason)s,
                            %(sector)s,
                            %(reasons)s::jsonb,
                            %(signal_flags)s::jsonb,
                            %(score_debug)s::jsonb,
                            %(component_scores)s::jsonb,
                            %(penalties)s::jsonb,
                            %(feature_snapshot)s::jsonb
                        )
                        on conflict (run_pk, ticker) do update set
                            latest_close = excluded.latest_close,
                            total_score = excluded.total_score,
                            setup_quality_score = excluded.setup_quality_score,
                            entry_timing_score = excluded.entry_timing_score,
                            reward_risk_ratio = excluded.reward_risk_ratio,
                            classification = excluded.classification,
                            buyability_status = excluded.buyability_status,
                            buyability_reason = excluded.buyability_reason,
                            sector = excluded.sector,
                            reasons = excluded.reasons,
                            signal_flags = excluded.signal_flags,
                            score_debug = excluded.score_debug,
                            component_scores = excluded.component_scores,
                            penalties = excluded.penalties,
                            feature_snapshot = excluded.feature_snapshot
                        """,
                        {
                            "run_pk": run_pk,
                            "ticker": item.ticker,
                            "as_of_date": item.as_of_date,
                            "latest_close": item.latest_close,
                            "total_score": item.score.total_score,
                            "setup_quality_score": item.score.setup_quality_score,
                            "entry_timing_score": item.score.entry_timing_score,
                            "reward_risk_ratio": item.score.reward_risk_ratio,
                            "classification": item.classification.value,
                            "buyability_status": item.buyability_status.value,
                            "buyability_reason": item.buyability_reason,
                            "sector": item.sector,
                            "reasons": json.dumps(item.score.reasons),
                            "signal_flags": json.dumps(item.signal_flags),
                            "score_debug": json.dumps(item.score_debug),
                            "component_scores": json.dumps(item.score.component_scores),
                            "penalties": json.dumps(item.score.penalties),
                            "feature_snapshot": json.dumps(item.feature_snapshot, default=str),
                        },
                    )

                for item in batch.trade_plans:
                    cur.execute(
                        """
                        insert into scanner_trade_plans (
                            run_pk,
                            ticker,
                            as_of_date,
                            latest_close,
                            score,
                            classification,
                            suggested_entry,
                            suggested_stop_loss,
                            suggested_first_target,
                            reward_risk_ratio,
                            buyability_status,
                            buyability_reason,
                            sector,
                            reasons
                        ) values (
                            %(run_pk)s,
                            %(ticker)s,
                            %(as_of_date)s,
                            %(latest_close)s,
                            %(score)s,
                            %(classification)s,
                            %(suggested_entry)s,
                            %(suggested_stop_loss)s,
                            %(suggested_first_target)s,
                            %(reward_risk_ratio)s,
                            %(buyability_status)s,
                            %(buyability_reason)s,
                            %(sector)s,
                            %(reasons)s::jsonb
                        )
                        on conflict (run_pk, ticker) do update set
                            latest_close = excluded.latest_close,
                            score = excluded.score,
                            classification = excluded.classification,
                            suggested_entry = excluded.suggested_entry,
                            suggested_stop_loss = excluded.suggested_stop_loss,
                            suggested_first_target = excluded.suggested_first_target,
                            reward_risk_ratio = excluded.reward_risk_ratio,
                            buyability_status = excluded.buyability_status,
                            buyability_reason = excluded.buyability_reason,
                            sector = excluded.sector,
                            reasons = excluded.reasons
                        """,
                        {
                            "run_pk": run_pk,
                            "ticker": item.ticker,
                            "as_of_date": item.as_of_date,
                            "latest_close": item.latest_close,
                            "score": item.score,
                            "classification": item.classification.value,
                            "suggested_entry": item.suggested_entry,
                            "suggested_stop_loss": item.suggested_stop_loss,
                            "suggested_first_target": item.suggested_first_target,
                            "reward_risk_ratio": item.reward_risk_ratio,
                            "buyability_status": item.buyability_status.value,
                            "buyability_reason": item.buyability_reason,
                            "sector": item.sector,
                            "reasons": json.dumps(item.reasons),
                        },
                    )

                for snap in batch.portfolio_snapshots:
                    cur.execute(
                        """
                        insert into scanner_portfolio_snapshots (
                            run_pk,
                            snapshot_date,
                            cash,
                            equity,
                            open_positions_count,
                            open_positions
                        ) values (
                            %(run_pk)s,
                            %(snapshot_date)s,
                            %(cash)s,
                            %(equity)s,
                            %(open_positions_count)s,
                            %(open_positions)s::jsonb
                        )
                        on conflict (run_pk, snapshot_date) do update set
                            cash = excluded.cash,
                            equity = excluded.equity,
                            open_positions_count = excluded.open_positions_count,
                            open_positions = excluded.open_positions
                        """,
                        {
                            "run_pk": run_pk,
                            "snapshot_date": snap.snapshot_date,
                            "cash": snap.cash,
                            "equity": snap.equity,
                            "open_positions_count": len(snap.open_positions),
                            "open_positions": json.dumps([asdict(pos) for pos in snap.open_positions], default=str),
                        },
                    )

            conn.commit()

    def load_latest_run_payload(self) -> dict[str, Any] | None:
        if not self.enabled:
            return None

        with psycopg.connect(self.db_url, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("select * from scanner_runs order by generated_at desc limit 1")
                run = cur.fetchone()
                if run is None:
                    return None

                run_pk = run["id"]
                cur.execute(
                    "select * from scanner_analyses where run_pk = %s order by total_score desc",
                    (run_pk,),
                )
                analyses = cur.fetchall()

                cur.execute("select * from scanner_trade_plans where run_pk = %s", (run_pk,))
                plans = cur.fetchall()

                return {
                    "run_id": run.get("run_id") or "latest",
                    "generated_at": run["generated_at"].isoformat() if isinstance(run["generated_at"], datetime) else str(run["generated_at"]),
                    "analyses": analyses,
                    "plans": plans,
                    "diagnostics": {
                        "total_tickers_analyzed": run.get("total_tickers_analyzed", 0),
                        "buy_count": run.get("buy_count", 0),
                        "watch_count": run.get("watch_count", 0),
                        "avoid_count": run.get("avoid_count", 0),
                        "average_total_score": float(run.get("average_total_score") or 0.0),
                        "top_5_tickers": run.get("top_5_tickers") or [],
                        "bottom_5_tickers": run.get("bottom_5_tickers") or [],
                        "sector_concentration_warning": run.get("sector_concentration_warning"),
                        "top_buy_sectors": run.get("top_buy_sectors") or {},
                    },
                    "backtest": run.get("backtest_summary") or {},
                }

    def save_simulator_action(self, payload: dict[str, Any]) -> None:
        if not self.enabled:
            return
        self._ensure_simulator_schema()
        with psycopg.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into simulator_actions (
                        action_time,
                        ticker,
                        action_code,
                        action_text,
                        quantity,
                        execution_price,
                        reason,
                        cash_balance,
                        portfolio_value,
                        payload
                    ) values (
                        %(action_time)s,
                        %(ticker)s,
                        %(action_code)s,
                        %(action_text)s,
                        %(quantity)s,
                        %(execution_price)s,
                        %(reason)s,
                        %(cash_balance)s,
                        %(portfolio_value)s,
                        %(payload)s::jsonb
                    )
                    """,
                    {
                        "action_time": payload.get("timestamp"),
                        "ticker": payload.get("ticker"),
                        "action_code": payload.get("action"),
                        "action_text": payload.get("action_display") or payload.get("action"),
                        "quantity": payload.get("quantity"),
                        "execution_price": payload.get("execution_price"),
                        "reason": payload.get("reason"),
                        "cash_balance": payload.get("cash_balance"),
                        "portfolio_value": payload.get("portfolio_value"),
                        "payload": json.dumps(payload, default=str),
                    },
                )
            conn.commit()

    def save_simulator_equity(self, payload: dict[str, Any]) -> None:
        if not self.enabled:
            return
        self._ensure_simulator_schema()
        with psycopg.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into simulator_equity_curve (
                        point_time,
                        equity,
                        cash,
                        unrealized_pnl,
                        realized_pnl,
                        open_positions_count,
                        payload
                    ) values (
                        %(point_time)s,
                        %(equity)s,
                        %(cash)s,
                        %(unrealized_pnl)s,
                        %(realized_pnl)s,
                        %(open_positions_count)s,
                        %(payload)s::jsonb
                    )
                    """,
                    {
                        "point_time": payload.get("timestamp"),
                        "equity": payload.get("equity"),
                        "cash": payload.get("cash"),
                        "unrealized_pnl": payload.get("unrealized_pnl"),
                        "realized_pnl": payload.get("realized_pnl"),
                        "open_positions_count": payload.get("open_positions"),
                        "payload": json.dumps(payload, default=str),
                    },
                )
            conn.commit()

    def load_simulator_state(self) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        self._ensure_simulator_schema()
        with psycopg.connect(self.db_url, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select payload
                    from simulator_state
                    where state_key = 'default'
                    """
                )
                row = cur.fetchone()
        return row["payload"] if row else None

    def save_simulator_state(self, payload: dict[str, Any]) -> None:
        if not self.enabled:
            return
        self._ensure_simulator_schema()
        with psycopg.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into simulator_state (state_key, payload, updated_at)
                    values ('default', %(payload)s::jsonb, now())
                    on conflict (state_key) do update set
                        payload = excluded.payload,
                        updated_at = now()
                    """,
                    {"payload": json.dumps(payload, default=str)},
                )
            conn.commit()

    def load_recent_simulator_actions(self, limit: int = 180) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        self._ensure_simulator_schema()
        with psycopg.connect(self.db_url, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select
                        action_time as timestamp,
                        ticker,
                        action_code as action,
                        quantity,
                        execution_price,
                        reason,
                        cash_balance,
                        portfolio_value
                    from simulator_actions
                    order by action_time desc
                    limit %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
        return [dict(row) for row in rows]

    def load_simulator_equity_curve(self, limit: int = 500) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        self._ensure_simulator_schema()
        with psycopg.connect(self.db_url, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select
                        point_time as timestamp,
                        equity
                    from simulator_equity_curve
                    order by point_time desc
                    limit %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
        points = [dict(row) for row in rows]
        points.reverse()
        return points

    def _ensure_simulator_schema(self) -> None:
        if self._sim_schema_ready or not self.enabled:
            return
        with psycopg.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    create table if not exists simulator_state (
                        state_key text primary key,
                        payload jsonb not null,
                        updated_at timestamptz not null default now()
                    )
                    """
                )
                cur.execute(
                    """
                    create table if not exists simulator_actions (
                        id bigserial primary key,
                        action_time timestamptz not null,
                        ticker text not null,
                        action_code text not null,
                        action_text text,
                        quantity integer not null default 0,
                        execution_price numeric(14,6) not null default 0,
                        reason text,
                        cash_balance numeric(18,4) not null default 0,
                        portfolio_value numeric(18,4) not null default 0,
                        payload jsonb not null,
                        created_at timestamptz not null default now()
                    )
                    """
                )
                cur.execute(
                    """
                    create table if not exists simulator_equity_curve (
                        id bigserial primary key,
                        point_time timestamptz not null,
                        equity numeric(18,4) not null,
                        cash numeric(18,4) not null,
                        unrealized_pnl numeric(18,4) not null default 0,
                        realized_pnl numeric(18,4) not null default 0,
                        open_positions_count integer not null default 0,
                        payload jsonb not null,
                        created_at timestamptz not null default now()
                    )
                    """
                )
            conn.commit()
        self._sim_schema_ready = True
