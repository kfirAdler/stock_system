from __future__ import annotations

import io
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from config.settings import AppSettings
from storage.supabase_postgres_repository import SupabasePostgresRepository


@dataclass
class DashboardDataService:
    """Loads latest scanner outputs and prepares dashboard view models."""

    settings: AppSettings
    supabase_repository: SupabasePostgresRepository | None = None

    def latest_run_id(self) -> str | None:
        if self.supabase_repository is not None and self.supabase_repository.enabled:
            try:
                payload = self.supabase_repository.load_latest_run_payload()
                if payload is not None:
                    return str(payload.get("run_id") or "latest")
            except Exception:  # noqa: BLE001
                pass

        analyses_dir = self.settings.analyses_dir
        if not analyses_dir.exists():
            return None

        latest_dir = analyses_dir / "latest"
        if latest_dir.exists() and latest_dir.is_dir():
            return "latest"

        run_pattern = re.compile(r"^\d{8}_\d{6}$")
        run_ids = [
            entry.name
            for entry in analyses_dir.iterdir()
            if entry.is_dir() and run_pattern.match(entry.name)
        ]
        if not run_ids:
            return None
        return sorted(run_ids)[-1]

    def load_run(
        self,
        run_id: str,
        lang: str = "en",
        sort_by: str = "score",
        widget_tf: str = "d",
    ) -> dict[str, Any] | None:
        if self.supabase_repository is not None and self.supabase_repository.enabled:
            try:
                payload = self.supabase_repository.load_latest_run_payload()
                if payload is not None:
                    return self._build_view_from_db_payload(payload=payload, lang=lang, sort_by=sort_by, widget_tf=widget_tf)
            except Exception:  # noqa: BLE001
                pass

        analysis_file = self.settings.analyses_dir / run_id / "current_analysis.json"
        diagnostics_file = self.settings.analyses_dir / run_id / "run_diagnostics.json"
        plans_file = self.settings.analyses_dir / run_id / "trade_plans.json"
        backtest_file = self.settings.portfolios_dir / run_id / "backtest_summary.json"

        if not analysis_file.exists() or not diagnostics_file.exists():
            return None

        analyses = self._load_json(analysis_file)
        diagnostics = self._load_json(diagnostics_file)
        plans = self._load_json(plans_file) if plans_file.exists() else []
        backtest = self._load_json(backtest_file) if backtest_file.exists() else {}

        plan_map = {str(item.get("ticker", "")).upper(): item for item in plans}
        rows: list[dict[str, Any]] = []

        for item in analyses:
            ticker = str(item.get("ticker", "")).upper()
            score = item.get("score", {})
            components = score.get("component_scores", {})
            features = item.get("feature_snapshot", {})
            plan = plan_map.get(ticker, {})

            row = {
                "ticker": ticker,
                "tradingview_url": f"https://www.tradingview.com/chart/rDOZV85G/?symbol=NYSE%3A{ticker}",
                "latest_close": item.get("latest_close"),
                "score": score.get("total_score"),
                "setup_quality_score": score.get("setup_quality_score"),
                "entry_timing_score": score.get("entry_timing_score"),
                "reward_risk_ratio": score.get("reward_risk_ratio"),
                "classification": item.get("classification", ""),
                "buyability_status": item.get("buyability_status", "AVOID"),
                "buyability_reason": item.get("buyability_reason", ""),
                "sector": item.get("sector", "Unknown"),
                "suggested_entry": plan.get("suggested_entry"),
                "suggested_stop_loss": plan.get("suggested_stop_loss"),
                "suggested_first_target": plan.get("suggested_first_target"),
                "plan_reward_risk_ratio": plan.get("reward_risk_ratio"),
                "reasons": score.get("reasons", []),
                "component_scores": components,
                "penalties": score.get("penalties", {}),
                "signal_flags": item.get("signal_flags", score.get("signal_flags", {})),
                "feature_snapshot": features,
                "momentum_score": float(components.get("momentum", 0.0) or 0.0),
                "relative_strength_score": float(components.get("relative_strength", 0.0) or 0.0),
                "distance_from_highs": float(features.get("dist_from_52w_high", 0.0) or 0.0),
            }
            row["reasons_display"] = self._readable_reasons(row["reasons"], lang)

            row["score_bars"] = self._score_bars(components, lang)
            row["visual_signals"] = self._visual_signals(row, lang)
            row["explanation"] = self._explanation(row, lang)
            rows.append(row)

        rows = self._sort_rows(rows, sort_by)
        generated_at = datetime.fromtimestamp(diagnostics_file.stat().st_mtime, tz=UTC).isoformat()

        return {
            "run_id": run_id,
            "generated_at": generated_at,
            "diagnostics": diagnostics,
            "backtest": backtest,
            "rows": rows,
            "market_widgets": self._load_market_widgets(widget_tf=widget_tf),
            "widget_tf": widget_tf,
        }

    def _build_view_from_db_payload(
        self,
        payload: dict[str, Any],
        lang: str,
        sort_by: str,
        widget_tf: str,
    ) -> dict[str, Any]:
        analyses = payload.get("analyses", [])
        diagnostics = payload.get("diagnostics", {})
        plans = payload.get("plans", [])
        backtest = payload.get("backtest", {})
        generated_at = payload.get("generated_at", "")
        run_id = payload.get("run_id", "latest")

        plan_map = {str(item.get("ticker", "")).upper(): item for item in plans}
        rows: list[dict[str, Any]] = []

        for item in analyses:
            ticker = str(item.get("ticker", "")).upper()
            plan = plan_map.get(ticker, {})
            components = self._ensure_dict(item.get("component_scores"))
            features = self._ensure_dict(item.get("feature_snapshot"))
            reasons = self._ensure_list(item.get("reasons"))

            row = {
                "ticker": ticker,
                "tradingview_url": f"https://www.tradingview.com/chart/rDOZV85G/?symbol=NYSE%3A{ticker}",
                "latest_close": item.get("latest_close"),
                "score": item.get("total_score"),
                "setup_quality_score": item.get("setup_quality_score"),
                "entry_timing_score": item.get("entry_timing_score"),
                "reward_risk_ratio": item.get("reward_risk_ratio"),
                "classification": item.get("classification", ""),
                "buyability_status": item.get("buyability_status", "AVOID"),
                "buyability_reason": item.get("buyability_reason", ""),
                "sector": item.get("sector", "Unknown"),
                "suggested_entry": plan.get("suggested_entry"),
                "suggested_stop_loss": plan.get("suggested_stop_loss"),
                "suggested_first_target": plan.get("suggested_first_target"),
                "plan_reward_risk_ratio": plan.get("reward_risk_ratio"),
                "reasons": reasons,
                "component_scores": components,
                "penalties": self._ensure_dict(item.get("penalties")),
                "signal_flags": self._ensure_dict(item.get("signal_flags")),
                "feature_snapshot": features,
                "momentum_score": float(components.get("momentum", 0.0) or 0.0),
                "relative_strength_score": float(components.get("relative_strength", 0.0) or 0.0),
                "distance_from_highs": float(features.get("dist_from_52w_high", 0.0) or 0.0),
            }
            row["reasons_display"] = self._readable_reasons(row["reasons"], lang)
            row["score_bars"] = self._score_bars(components, lang)
            row["visual_signals"] = self._visual_signals(row, lang)
            row["explanation"] = self._explanation(row, lang)
            rows.append(row)

        rows = self._sort_rows(rows, sort_by)
        return {
            "run_id": run_id,
            "generated_at": generated_at,
            "diagnostics": diagnostics,
            "backtest": backtest,
            "rows": rows,
            "market_widgets": self._load_market_widgets(widget_tf=widget_tf),
            "widget_tf": widget_tf,
        }

    @staticmethod
    def _ensure_dict(value: object) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                return {}
        return {}

    @staticmethod
    def _ensure_list(value: object) -> list[Any]:
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                return []
        return []

    @staticmethod
    def _readable_reasons(reasons: list[str], lang: str) -> list[str]:
        labels = {
            "en": {
                "trend_supportive": "Trend alignment is supportive",
                "momentum_constructive": "Momentum is constructive",
                "volume_confirmation": "Volume confirms the move",
                "near_52w_high": "Trading near 52-week high",
                "relative_strength_positive": "Relative strength is positive",
                "weak_structure_penalty": "Weak structure penalty applied",
                "price_extended_from_ma20": "Price is extended above MA20",
                "too_close_to_resistance": "Price is too close to resistance",
                "entry_timing_weak": "Entry timing quality is weak",
                "reward_risk_weak": "Reward-to-risk is weak",
                "parabolic_move_penalty": "Recent move is parabolic and overheated",
                "mixed_signal": "Signals are mixed",
            },
            "he": {
                "trend_supportive": "יישור המגמה תומך",
                "momentum_constructive": "המומנטום חיובי",
                "volume_confirmation": "נפח המסחר מאשר את המהלך",
                "near_52w_high": "המחיר קרוב לשיא שנתי",
                "relative_strength_positive": "החוזק היחסי חיובי",
                "weak_structure_penalty": "הופעלה ענישת מבנה חלש",
                "price_extended_from_ma20": "המחיר מתוח מעל MA20",
                "too_close_to_resistance": "המחיר קרוב מדי להתנגדות",
                "entry_timing_weak": "איכות התזמון חלשה",
                "reward_risk_weak": "יחס סיכוי/סיכון חלש",
                "parabolic_move_penalty": "מהלך פרבולי וחם מדי בטווח הקצר",
                "mixed_signal": "איתותים מעורבים",
            },
        }
        local = labels["he" if lang == "he" else "en"]
        return [local.get(reason, reason.replace("_", " ")) for reason in reasons]

    def load_latest_run(
        self,
        lang: str = "en",
        sort_by: str = "score",
        widget_tf: str = "d",
    ) -> dict[str, Any] | None:
        run_id = self.latest_run_id()
        if run_id is None:
            return None
        return self.load_run(run_id, lang=lang, sort_by=sort_by, widget_tf=widget_tf)

    @staticmethod
    def _load_json(path: Path) -> Any:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    @staticmethod
    def _run_id_to_datetime(run_id: str) -> str:
        try:
            return datetime.strptime(run_id, "%Y%m%d_%H%M%S").isoformat()
        except ValueError:
            return run_id

    def _sort_rows(self, rows: list[dict[str, Any]], sort_by: str) -> list[dict[str, Any]]:
        key_map = {
            "score": lambda row: float(row.get("score") or 0.0),
            "setup_quality": lambda row: float(row.get("setup_quality_score") or 0.0),
            "entry_timing": lambda row: float(row.get("entry_timing_score") or 0.0),
            "momentum": lambda row: float(row.get("momentum_score") or 0.0),
            "relative_strength": lambda row: float(row.get("relative_strength_score") or 0.0),
            "distance_from_highs": lambda row: float(row.get("distance_from_highs") or -1.0),
        }
        key_fn = key_map.get(sort_by, key_map["score"])
        return sorted(rows, key=key_fn, reverse=True)

    def _visual_signals(self, row: dict[str, Any], lang: str) -> list[dict[str, str]]:
        components = row.get("component_scores", {})
        features = row.get("feature_snapshot", {})

        trend = float(components.get("trend_quality", 0.0) or 0.0)
        momentum = float(components.get("momentum", 0.0) or 0.0)
        volume = float(components.get("volume_quality", 0.0) or 0.0)
        rel = float(components.get("relative_strength", 0.0) or 0.0)
        dist_ma20 = float(features.get("distance_from_ma20", 0.0) or 0.0)
        dist_res = float(features.get("distance_to_resistance", 1.0) or 1.0)
        dist_sup = float(features.get("distance_to_support", 1.0) or 1.0)
        vol_confirm = float(components.get("volume_confirmation_quality", volume) or 0.0)

        signals = [
            self._qual_signal("trend", trend, [(70, "green"), (50, "yellow"), (0, "red")], lang),
            self._qual_signal("momentum", momentum, [(70, "green"), (50, "yellow"), (0, "red")], lang),
            self._qual_signal("volume", volume, [(65, "green"), (45, "yellow"), (0, "red")], lang),
            self._qual_signal("volume_confirmation", vol_confirm, [(70, "green"), (45, "yellow"), (0, "red")], lang),
            self._qual_signal(
                "relative_strength",
                rel,
                [(60, "green"), (45, "yellow"), (0, "red")],
                lang,
            ),
        ]

        if dist_ma20 <= 0.03:
            signals.append(self._signal("ma20_distance", "green", "ideal", lang))
        elif dist_ma20 <= 0.05:
            signals.append(self._signal("ma20_distance", "yellow", "acceptable", lang))
        elif dist_ma20 <= 0.08:
            signals.append(self._signal("ma20_distance", "orange", "extended", lang))
        else:
            signals.append(self._signal("ma20_distance", "red", "late", lang))

        if dist_res < 0.015:
            signals.append(self._signal("resistance", "red", "near", lang))
        elif dist_res < 0.02:
            signals.append(self._signal("resistance", "orange", "near", lang))
        elif dist_res < 0.03:
            signals.append(self._signal("resistance", "yellow", "moderate", lang))
        else:
            signals.append(self._signal("resistance", "green", "room", lang))

        if dist_sup < 0.02:
            signals.append(self._signal("support", "green", "near", lang))
        elif dist_sup < 0.06:
            signals.append(self._signal("support", "yellow", "mid", lang))
        else:
            signals.append(self._signal("support", "orange", "far", lang))

        return signals

    def _score_bars(self, components: dict[str, Any], lang: str) -> list[dict[str, Any]]:
        labels = {
            "en": {
                "trend_quality": "Trend",
                "momentum": "Momentum",
                "volume_quality": "Volume",
                "relative_strength": "Relative Strength",
                "volatility_suitability": "Volatility",
                "sector_strength": "Sector Strength",
                "ma20_distance_quality": "MA20 Timing",
                "resistance_room_quality": "Resistance Room",
                "reward_risk_quality": "Reward/Risk",
            },
            "he": {
                "trend_quality": "מגמה",
                "momentum": "מומנטום",
                "volume_quality": "נפח",
                "relative_strength": "חוזק יחסי",
                "volatility_suitability": "תנודתיות",
                "sector_strength": "חוזק סקטור",
                "ma20_distance_quality": "תזמון MA20",
                "resistance_room_quality": "מרווח להתנגדות",
                "reward_risk_quality": "יחס סיכוי/סיכון",
            },
        }
        local = labels["he" if lang == "he" else "en"]

        keys = [
            "trend_quality",
            "momentum",
            "volume_quality",
            "relative_strength",
            "volatility_suitability",
            "sector_strength",
            "ma20_distance_quality",
            "resistance_room_quality",
            "reward_risk_quality",
        ]
        bars: list[dict[str, Any]] = []
        for key in keys:
            value = float(components.get(key, 0.0) or 0.0)
            bars.append({"label": local[key], "value": round(value, 2)})
        return bars

    def _explanation(self, row: dict[str, Any], lang: str) -> str:
        classification = str(row.get("classification", "")).upper()
        components = row.get("component_scores", {})
        features = row.get("feature_snapshot", {})
        buyability = str(row.get("buyability_status", "AVOID"))
        buyability_reason = str(row.get("buyability_reason", ""))

        trend_score = components.get("trend_quality")
        momentum_score = components.get("momentum")
        volume_score = components.get("volume_quality")
        rs_score = components.get("relative_strength")
        setup_score = row.get("setup_quality_score")
        entry_score = row.get("entry_timing_score")
        rr = row.get("reward_risk_ratio")

        dist_high = features.get("dist_from_52w_high")
        dist_ma20 = features.get("distance_from_ma20")
        dist_res = features.get("distance_to_resistance")
        dist_sup = features.get("distance_to_support")

        if lang == "he":
            header_map = {
                "BUY": "מניה זו קיבלה דירוג BUY כי רוב האינדיקציות תומכות בעסקה.",
                "WATCH": "מניה זו קיבלה דירוג WATCH כי קיימים סימנים מעורבים.",
                "AVOID": "מניה זו קיבלה דירוג AVOID כי איכות הסטאפ חלשה יחסית.",
            }
            header = header_map.get(classification, "דירוג המניה נקבע לפי שילוב אינדיקציות טכניות.")
            return (
                f"{header} איכות הסטאפ={setup_score}, תזמון כניסה={entry_score}, יחס סיכוי/סיכון={rr}. "
                f"מגמה={trend_score}, מומנטום={momentum_score}, נפח={volume_score}, חוזק יחסי={rs_score}. "
                f"מרחק מהשיא השנתי={dist_high}, מרחק מ-MA20={dist_ma20}, מרחק מהתנגדות={dist_res}, מרחק מתמיכה={dist_sup}. "
                f"סטטוס כניסה: {buyability}. {buyability_reason}"
            )

        header_map_en = {
            "BUY": "This stock is classified as BUY because trend alignment, momentum, and confirmation signals are supportive.",
            "WATCH": "This stock is classified as WATCH because the setup has mixed strength and risk factors.",
            "AVOID": "This stock is classified as AVOID because the setup lacks enough high-quality confirmation.",
        }
        header_en = header_map_en.get(classification, "This stock classification is based on combined technical signals.")
        return (
            f"{header_en} Setup quality={setup_score}, entry timing={entry_score}, reward/risk={rr}. "
            f"Trend={trend_score}, momentum={momentum_score}, volume={volume_score}, relative strength={rs_score}. "
            f"Distance from 52-week high={dist_high}, distance from MA20={dist_ma20}, "
            f"distance to resistance={dist_res}, distance to support={dist_sup}. "
            f"Buyability: {buyability}. {buyability_reason}"
        )

    def _load_market_widgets(self, widget_tf: str = "d") -> list[dict[str, Any]]:
        symbols = [
            {"name": "S&P 500 (SPX)", "key": "SPX", "symbol": "^spx"},
            {"name": "NASDAQ", "key": "NASDAQ", "symbol": "^ndq"},
            {"name": "Russell 2000", "key": "RUSSELL", "symbol": "iwm.us"},
            {"name": "Bitcoin", "key": "BTC", "symbol": "btcusd"},
        ]

        widgets: list[dict[str, Any]] = []
        tf = widget_tf if widget_tf in {"d", "w", "m"} else "d"
        for item in symbols:
            frame = self._load_symbol_with_cache(symbol=item["symbol"], cache_key=item["key"])
            frame_tf = self._to_timeframe(frame, tf)
            if frame_tf.empty or len(frame_tf) < 2:
                continue

            last = frame_tf.iloc[-1]
            prev = frame_tf.iloc[-2]
            last_close = float(last["Close"])
            prev_close = float(prev["Close"])
            daily_change_pct = ((last_close / prev_close) - 1.0) * 100.0 if prev_close > 0 else 0.0

            candles = self._mini_candles(frame_tf.tail(12))
            widgets.append(
                {
                    "name": item["name"],
                    "timeframe": tf,
                    "price": round(last_close, 2),
                    "change_pct": round(daily_change_pct, 2),
                    "is_up": daily_change_pct >= 0,
                    "candles": candles,
                }
            )

        return widgets

    @staticmethod
    def _to_timeframe(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        if frame.empty or timeframe == "d":
            return frame

        if timeframe == "w":
            rule = "W-FRI"
        elif timeframe == "m":
            rule = "ME"
        else:
            return frame

        aggregated = frame.resample(rule).agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
            }
        )
        return aggregated.dropna().sort_index()

    def _load_symbol_with_cache(self, symbol: str, cache_key: str) -> pd.DataFrame:
        cache_file = self.settings.raw_data_dir / f"index_{cache_key}.csv"
        today = date.today()
        yesterday = today - timedelta(days=1)

        cached = self._read_cached(cache_file)
        if not cached.empty:
            last_cached = cached.index.max().date()
            if last_cached >= yesterday:
                return cached

            incremental = self._fetch_stooq(symbol=symbol, start_date=last_cached + timedelta(days=1), end_date=today)
            if incremental.empty:
                return cached

            merged = pd.concat([cached, incremental], axis=0)
            merged = merged[~merged.index.duplicated(keep="last")].sort_index()
            merged.to_csv(cache_file)
            return merged

        fresh = self._fetch_stooq(symbol=symbol)
        if not fresh.empty:
            fresh.to_csv(cache_file)
        return fresh

    @staticmethod
    def _read_cached(path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        try:
            frame = pd.read_csv(path, parse_dates=["date"]).set_index("date")
            frame.index.name = "date"
            return frame.sort_index()
        except Exception:  # noqa: BLE001
            return pd.DataFrame()

    @staticmethod
    def _fetch_stooq(symbol: str, start_date: date | None = None, end_date: date | None = None) -> pd.DataFrame:
        url = "https://stooq.com/q/d/l/"
        params: dict[str, str] = {"s": symbol, "i": "d"}
        if start_date is not None:
            params["d1"] = start_date.strftime("%Y%m%d")
        if end_date is not None:
            params["d2"] = end_date.strftime("%Y%m%d")

        try:
            response = requests.get(url, params=params, timeout=12)
            response.raise_for_status()
        except requests.RequestException:
            return pd.DataFrame()

        if not response.text.strip() or "No data" in response.text:
            return pd.DataFrame()

        frame = pd.read_csv(io.StringIO(response.text))
        required = ["Date", "Open", "High", "Low", "Close"]
        if any(col not in frame.columns for col in required):
            return pd.DataFrame()

        cleaned = frame.copy()
        cleaned["Date"] = pd.to_datetime(cleaned["Date"], errors="coerce")
        for col in ["Open", "High", "Low", "Close"]:
            cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")
        cleaned = cleaned.dropna().sort_values("Date")
        cleaned = cleaned.set_index("Date")
        cleaned.index.name = "date"
        return cleaned

    @staticmethod
    def _mini_candles(frame: pd.DataFrame) -> list[dict[str, Any]]:
        lows = frame["Low"].astype(float)
        highs = frame["High"].astype(float)
        low_min = float(lows.min())
        high_max = float(highs.max())
        span = max(0.0001, high_max - low_min)

        candles: list[dict[str, Any]] = []
        for _, row in frame.iterrows():
            open_price = float(row["Open"])
            close_price = float(row["Close"])
            high_price = float(row["High"])
            low_price = float(row["Low"])

            high_top = 100.0 * (1.0 - ((high_price - low_min) / span))
            low_top = 100.0 * (1.0 - ((low_price - low_min) / span))
            open_top = 100.0 * (1.0 - ((open_price - low_min) / span))
            close_top = 100.0 * (1.0 - ((close_price - low_min) / span))

            wick_top = min(high_top, low_top)
            wick_height = max(2.0, abs(low_top - high_top))
            body_top = min(open_top, close_top)
            body_height = max(2.0, abs(close_top - open_top))

            candles.append(
                {
                    "up": close_price >= open_price,
                    "high_top": round(wick_top, 2),
                    "wick_height": round(wick_height, 2),
                    "body_top": round(body_top, 2),
                    "body_height": round(body_height, 2),
                }
            )

        return candles

    def _qual_signal(
        self,
        key: str,
        value: float,
        thresholds: list[tuple[float, str]],
        lang: str,
    ) -> dict[str, str]:
        for threshold, color in thresholds:
            if value >= threshold:
                if color == "green":
                    level = "strong"
                elif color == "yellow":
                    level = "neutral"
                elif color == "orange":
                    level = "warning"
                else:
                    level = "weak"
                return self._signal(key, color, level, lang)
        return self._signal(key, "red", "weak", lang)

    def _signal(self, key: str, color: str, level: str, lang: str) -> dict[str, str]:
        labels = {
            "en": {
                "trend": "Trend",
                "momentum": "Momentum",
                "volume": "Volume",
                "volume_confirmation": "Volume Confirmation",
                "relative_strength": "Relative Strength",
                "ma20_distance": "Distance from MA20",
                "resistance": "Distance from Resistance",
                "support": "Distance from Support",
                "strong": "Strong",
                "neutral": "Neutral",
                "warning": "Warning",
                "weak": "Weak",
                "ideal": "Ideal Entry",
                "acceptable": "Acceptable",
                "extended": "Slightly Extended",
                "late": "Likely Late Entry",
                "near": "Near",
                "moderate": "Moderate",
                "room": "Room To Upside",
                "mid": "Mid Range",
                "far": "Far",
            },
            "he": {
                "trend": "מגמה",
                "momentum": "מומנטום",
                "volume": "נפח",
                "volume_confirmation": "אישור נפח",
                "relative_strength": "חוזק יחסי",
                "ma20_distance": "מרחק מ-MA20",
                "resistance": "מרחק מהתנגדות",
                "support": "מרחק מתמיכה",
                "strong": "חזק",
                "neutral": "ניטרלי",
                "warning": "אזהרה",
                "weak": "חלש",
                "ideal": "כניסה אידאלית",
                "acceptable": "סביר",
                "extended": "מתוחה מעט",
                "late": "כניסה מאוחרת",
                "near": "קרוב",
                "moderate": "בינוני",
                "room": "יש מקום לעלייה",
                "mid": "טווח ביניים",
                "far": "רחוק",
            },
        }
        local = labels["he" if lang == "he" else "en"]
        return {
            "label": local.get(key, key),
            "level": local.get(level, level),
            "color": color,
        }
