from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from config.settings import AppSettings
from dashboard.data_service import DashboardDataService


@dataclass(frozen=True)
class SimulatorConfig:
    """Configurable guardrails for the local paper-trading simulator."""

    initial_capital: float = 10_000.0
    max_position_fraction: float = 0.20
    max_open_positions: int = 3
    max_sector_positions: int = 2
    min_reward_risk: float = 1.5


@dataclass
class DashboardSimulatorService:
    """Maintains a persistent paper-trading state and action history."""

    settings: AppSettings
    data_service: DashboardDataService
    config: SimulatorConfig = field(default_factory=SimulatorConfig)

    _ACTION_MESSAGES: dict[str, str] = field(
        default_factory=lambda: {
            "BUY": "Position opened because setup was BUYABLE_NOW",
            "HOLD": "Holding current position",
            "STOP_LOSS_TRIGGERED": "Position sold because stop loss was reached",
            "TARGET_HIT": "Position sold because first target was reached",
            "PARTIAL_SELL": "Partial profit taken",
            "SKIPPED_BUY_OUTSIDE_MARKET_HOURS": "Buy skipped because the US market is currently closed",
            "SKIPPED_BUY_NOT_BUYABLE_NOW": "Buy skipped because the setup is not actionable right now",
        }
    )

    def __post_init__(self) -> None:
        self._tz = ZoneInfo("America/New_York")
        self._dir = self.settings.output_dir / "simulator"
        self._dir.mkdir(parents=True, exist_ok=True)
        self._state_file = self._dir / "state.json"
        self._actions_file = self._dir / "actions.jsonl"
        self._equity_file = self._dir / "equity_curve.csv"

    def tick(self) -> dict[str, Any]:
        """Advance the simulator by one cycle using latest scanner outputs."""
        state = self._load_state()
        now = datetime.now(self._tz)
        market_open = self._is_market_open(now)
        run = self.data_service.load_latest_run(lang="en", sort_by="score", widget_tf="d")

        if run is None:
            state["status"] = "idle"
            state["last_tick"] = now.isoformat()
            self._save_state(state)
            return self.snapshot()

        rows = run.get("rows", [])
        prices = {row["ticker"]: float(row.get("latest_close") or 0.0) for row in rows}

        state["status"] = "running"
        self._apply_exits(state, prices, now)
        self._apply_entries(state, rows, prices, now, market_open)
        self._log_holds(state, prices, now)
        self._write_equity_point(state, prices, now)

        state["status"] = "completed"
        state["last_tick"] = now.isoformat()
        state["last_run_id"] = run.get("run_id")
        self._save_state(state)
        return self.snapshot()

    def snapshot(self) -> dict[str, Any]:
        """Return current simulator metrics for dashboard rendering."""
        state = self._load_state()
        now = datetime.now(self._tz)
        market_open = self._is_market_open(now)

        latest = self.data_service.load_latest_run(lang="en", sort_by="score", widget_tf="d")
        prices = {}
        if latest is not None:
            prices = {row["ticker"]: float(row.get("latest_close") or 0.0) for row in latest.get("rows", [])}

        open_positions = list(state["open_positions"].values())
        total_market_value = 0.0
        for pos in open_positions:
            current = float(prices.get(pos["ticker"], pos["entry_price"]))
            market_value = current * pos["quantity"]
            pnl = (current - pos["entry_price"]) * pos["quantity"]
            cost = max(0.01, pos["entry_price"] * pos["quantity"])
            pos["current_price"] = round(current, 2)
            pos["market_value"] = round(market_value, 2)
            pos["unrealized_pnl"] = round(pnl, 2)
            pos["unrealized_pnl_pct"] = round((pnl / cost) * 100.0, 2)
            pos["status"] = self._position_status(pos, current)
            total_market_value += market_value

        equity = round(float(state["cash"]) + total_market_value, 2)
        unrealized = round(sum(float(pos["unrealized_pnl"]) for pos in open_positions), 2)
        realized = round(float(state["realized_pnl"]), 2)

        equity_curve = self._load_equity_curve(limit=500)
        if not equity_curve:
            self._seed_equity_if_needed(state=state, timestamp=now)
            equity_curve = self._load_equity_curve(limit=500)

        actions = self._load_recent_actions(limit=180)
        waiting_market = not market_open and len(open_positions) == 0

        return {
            "status": state.get("status", "idle"),
            "market_open": market_open,
            "market_status": "OPEN" if market_open else "CLOSED",
            "cash": round(float(state["cash"]), 2),
            "realized_pnl": realized,
            "unrealized_pnl": unrealized,
            "total_equity": equity,
            "open_positions": open_positions,
            "open_position_count": len(open_positions),
            "actions": actions,
            "equity_curve": equity_curve,
            "last_tick": state.get("last_tick"),
            "last_tick_display": self._format_et(state.get("last_tick")),
            "last_run_id": state.get("last_run_id"),
            "now_display": self._format_et(now.isoformat()),
            "waiting_market": waiting_market,
        }

    def _apply_exits(self, state: dict[str, Any], prices: dict[str, float], now: datetime) -> None:
        to_close: list[tuple[str, str]] = []
        for ticker, position in state["open_positions"].items():
            current = prices.get(ticker)
            if current is None:
                continue
            if current <= position["stop_loss"]:
                to_close.append((ticker, "STOP_LOSS_TRIGGERED"))
            elif current >= position["first_target"]:
                to_close.append((ticker, "TARGET_HIT"))

        for ticker, action in to_close:
            position = state["open_positions"].pop(ticker)
            price = float(prices.get(ticker, position["entry_price"]))
            proceeds = position["quantity"] * price
            pnl = (price - position["entry_price"]) * position["quantity"]
            state["cash"] += proceeds
            state["realized_pnl"] += pnl
            self._append_action(
                now=now,
                action=action,
                ticker=ticker,
                quantity=position["quantity"],
                execution_price=price,
                reason=self._ACTION_MESSAGES[action],
                state=state,
            )

    def _apply_entries(
        self,
        state: dict[str, Any],
        rows: list[dict[str, Any]],
        prices: dict[str, float],
        now: datetime,
        market_open: bool,
    ) -> None:
        slots_left = self.config.max_open_positions - len(state["open_positions"])
        if slots_left <= 0:
            return

        candidates = sorted(
            rows,
            key=lambda row: (
                str(row.get("buyability_status", "")).upper() == "BUYABLE_NOW",
                str(row.get("classification", "")).upper() == "BUY",
                float(row.get("entry_timing_score") or 0.0),
                float(row.get("score") or 0.0),
            ),
            reverse=True,
        )
        sector_counts = self._sector_counts(state)

        for row in candidates:
            ticker = str(row.get("ticker", "")).upper()
            if not ticker or ticker in state["open_positions"]:
                continue

            classification = str(row.get("classification", "")).upper()
            buyability = str(row.get("buyability_status", "AVOID")).upper()
            rr = float(row.get("reward_risk_ratio") or 0.0)
            sector = str(row.get("sector", "Unknown"))
            dedupe_prefix = f"{state.get('last_run_id', 'na')}:{ticker}"

            if classification != "BUY":
                continue

            if buyability != "BUYABLE_NOW":
                self._log_once(
                    state,
                    dedupe_key=f"{dedupe_prefix}:not_buyable",
                    now=now,
                    action="SKIPPED_BUY_NOT_BUYABLE_NOW",
                    ticker=ticker,
                    quantity=0,
                    execution_price=prices.get(ticker, 0.0),
                    reason=str(row.get("buyability_reason") or self._ACTION_MESSAGES["SKIPPED_BUY_NOT_BUYABLE_NOW"]),
                )
                continue

            if rr < self.config.min_reward_risk:
                self._log_once(
                    state,
                    dedupe_key=f"{dedupe_prefix}:rr",
                    now=now,
                    action="SKIPPED_BUY_NOT_BUYABLE_NOW",
                    ticker=ticker,
                    quantity=0,
                    execution_price=prices.get(ticker, 0.0),
                    reason="Entry rejected because reward-to-risk is below minimum",
                )
                continue

            if not market_open:
                self._log_once(
                    state,
                    dedupe_key=f"{dedupe_prefix}:closed",
                    now=now,
                    action="SKIPPED_BUY_OUTSIDE_MARKET_HOURS",
                    ticker=ticker,
                    quantity=0,
                    execution_price=prices.get(ticker, 0.0),
                    reason=self._ACTION_MESSAGES["SKIPPED_BUY_OUTSIDE_MARKET_HOURS"],
                )
                continue

            if sector_counts.get(sector, 0) >= self.config.max_sector_positions:
                self._log_once(
                    state,
                    dedupe_key=f"{dedupe_prefix}:sector_limit",
                    now=now,
                    action="SKIPPED_BUY_NOT_BUYABLE_NOW",
                    ticker=ticker,
                    quantity=0,
                    execution_price=prices.get(ticker, 0.0),
                    reason="Entry rejected because sector concentration limit was reached",
                )
                continue

            entry = float(row.get("suggested_entry") or row.get("latest_close") or 0.0)
            if entry <= 0:
                continue

            budget = float(state["cash"]) * self.config.max_position_fraction
            quantity = int(budget / entry)
            if quantity <= 0:
                continue

            cost = quantity * entry
            if cost > float(state["cash"]):
                continue

            stop_loss = float(row.get("suggested_stop_loss") or entry * 0.93)
            first_target = float(row.get("suggested_first_target") or entry * 1.12)

            state["cash"] -= cost
            state["open_positions"][ticker] = {
                "ticker": ticker,
                "quantity": quantity,
                "entry_price": round(entry, 2),
                "stop_loss": round(stop_loss, 2),
                "first_target": round(first_target, 2),
                "sector": sector,
                "opened_at": now.isoformat(),
            }
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
            slots_left -= 1

            self._append_action(
                now=now,
                action="BUY",
                ticker=ticker,
                quantity=quantity,
                execution_price=entry,
                reason=self._ACTION_MESSAGES["BUY"],
                state=state,
            )

            if slots_left <= 0:
                break

    def _log_holds(self, state: dict[str, Any], prices: dict[str, float], now: datetime) -> None:
        for ticker, pos in state["open_positions"].items():
            self._log_once(
                state,
                dedupe_key=f"hold:{now.strftime('%Y%m%d%H%M')}:{ticker}",
                now=now,
                action="HOLD",
                ticker=ticker,
                quantity=pos["quantity"],
                execution_price=prices.get(ticker, pos["entry_price"]),
                reason=self._ACTION_MESSAGES["HOLD"],
            )

    def _position_status(self, position: dict[str, Any], current_price: float) -> str:
        if current_price <= position["stop_loss"]:
            return "Stop Risk"
        if current_price >= position["first_target"]:
            return "Target Zone"
        return "Active"

    def _write_equity_point(self, state: dict[str, Any], prices: dict[str, float], now: datetime) -> None:
        self._seed_equity_if_needed(state=state, timestamp=now)
        open_positions = list(state["open_positions"].values())
        unrealized = self._unrealized_pnl(open_positions, prices)
        equity = float(state["cash"]) + sum(
            pos["quantity"] * prices.get(pos["ticker"], pos["entry_price"]) for pos in open_positions
        )
        row = {
            "timestamp": now.isoformat(),
            "equity": round(equity, 2),
            "cash": round(float(state["cash"]), 2),
            "unrealized_pnl": round(unrealized, 2),
            "realized_pnl": round(float(state["realized_pnl"]), 2),
            "open_positions": len(open_positions),
        }
        with self._equity_file.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
            writer.writerow(row)

    def _seed_equity_if_needed(self, state: dict[str, Any], timestamp: datetime) -> None:
        if self._equity_file.exists() and self._equity_file.stat().st_size > 0:
            return
        row = {
            "timestamp": timestamp.isoformat(),
            "equity": round(self.config.initial_capital, 2),
            "cash": round(self.config.initial_capital, 2),
            "unrealized_pnl": 0.0,
            "realized_pnl": 0.0,
            "open_positions": 0,
        }
        with self._equity_file.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
            writer.writeheader()
            writer.writerow(row)

    def _load_state(self) -> dict[str, Any]:
        if self._state_file.exists():
            with self._state_file.open("r", encoding="utf-8") as handle:
                state = json.load(handle)
            state.setdefault("dedupe_keys", [])
            return state

        now = datetime.now(self._tz)
        state = {
            "cash": self.config.initial_capital,
            "realized_pnl": 0.0,
            "open_positions": {},
            "status": "idle",
            "last_tick": now.isoformat(),
            "last_run_id": None,
            "dedupe_keys": [],
        }
        self._save_state(state)
        self._seed_equity_if_needed(state=state, timestamp=now)
        return state

    def _save_state(self, state: dict[str, Any]) -> None:
        state["dedupe_keys"] = state.get("dedupe_keys", [])[-4000:]
        with self._state_file.open("w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2)

    def _append_action(
        self,
        now: datetime,
        action: str,
        ticker: str,
        quantity: int,
        execution_price: float,
        reason: str,
        state: dict[str, Any],
    ) -> None:
        open_positions = list(state["open_positions"].values())
        portfolio_value = float(state["cash"]) + sum(pos["quantity"] * pos["entry_price"] for pos in open_positions)
        payload = {
            "timestamp": now.isoformat(),
            "ticker": ticker,
            "action": action,
            "quantity": quantity,
            "execution_price": round(float(execution_price), 4),
            "reason": reason,
            "cash_balance": round(float(state["cash"]), 2),
            "portfolio_value": round(portfolio_value, 2),
        }
        with self._actions_file.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload) + "\n")

    def _log_once(
        self,
        state: dict[str, Any],
        dedupe_key: str,
        now: datetime,
        action: str,
        ticker: str,
        quantity: int,
        execution_price: float,
        reason: str,
    ) -> None:
        if dedupe_key in state["dedupe_keys"]:
            return
        state["dedupe_keys"].append(dedupe_key)
        self._append_action(
            now=now,
            action=action,
            ticker=ticker,
            quantity=quantity,
            execution_price=execution_price,
            reason=reason,
            state=state,
        )

    @staticmethod
    def _sector_counts(state: dict[str, Any]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for pos in state["open_positions"].values():
            sector = pos.get("sector", "Unknown")
            counts[sector] = counts.get(sector, 0) + 1
        return counts

    @staticmethod
    def _unrealized_pnl(open_positions: list[dict[str, Any]], prices: dict[str, float]) -> float:
        return sum(
            (prices.get(pos["ticker"], pos["entry_price"]) - pos["entry_price"]) * pos["quantity"]
            for pos in open_positions
        )

    def _load_recent_actions(self, limit: int = 180) -> list[dict[str, Any]]:
        if not self._actions_file.exists():
            return []
        with self._actions_file.open("r", encoding="utf-8") as handle:
            lines = handle.readlines()[-limit:]

        actions: list[dict[str, Any]] = []
        for line in reversed(lines):
            if not line.strip():
                continue
            payload = json.loads(line)
            code = str(payload.get("action", ""))
            payload["action_display"] = self._ACTION_MESSAGES.get(code, code.replace("_", " ").title())
            payload["timestamp_display"] = self._format_et(payload.get("timestamp"))
            payload["action_level"] = self._action_level(code)
            actions.append(payload)
        return actions

    def _load_equity_curve(self, limit: int = 500) -> list[dict[str, Any]]:
        if not self._equity_file.exists():
            return []

        points: list[dict[str, Any]] = []
        with self._equity_file.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                points.append(
                    {
                        "timestamp": row["timestamp"],
                        "timestamp_display": self._format_et(row["timestamp"]),
                        "equity": float(row["equity"]),
                    }
                )
        return points[-limit:]

    @staticmethod
    def _action_level(action: str) -> str:
        if action in {"BUY", "TARGET_HIT"}:
            return "positive"
        if action in {"STOP_LOSS_TRIGGERED", "SKIPPED_BUY_OUTSIDE_MARKET_HOURS", "SKIPPED_BUY_NOT_BUYABLE_NOW"}:
            return "negative"
        if action in {"HOLD"}:
            return "neutral"
        return "neutral"

    def _format_et(self, value: str | None) -> str:
        if not value:
            return "-"
        text = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        et = dt.astimezone(self._tz)
        return et.strftime("%Y-%m-%d %I:%M:%S %p ET")

    def _is_market_open(self, now: datetime) -> bool:
        if now.weekday() >= 5:
            return False
        return time(hour=9, minute=30) <= now.time() <= time(hour=16, minute=0)
