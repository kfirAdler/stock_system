from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd
import psycopg
from psycopg.rows import dict_row


@dataclass
class SupabaseMarketDataCache:
    """Supabase-backed cache for raw OHLCV history per ticker/date."""

    db_url: str | None

    def __post_init__(self) -> None:
        self._schema_ready = False

    @property
    def enabled(self) -> bool:
        return bool(self.db_url and self.db_url.strip())

    def load_history(self, ticker: str) -> pd.DataFrame:
        if not self.enabled:
            return pd.DataFrame()
        self._ensure_schema()
        with psycopg.connect(self.db_url, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select trade_date, open, high, low, close, volume
                    from market_raw_data
                    where ticker = %s
                    order by trade_date asc
                    """,
                    (ticker.upper(),),
                )
                rows = cur.fetchall()

        if not rows:
            return pd.DataFrame()

        frame = pd.DataFrame(rows)
        frame = frame.rename(
            columns={
                "trade_date": "date",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
            }
        )
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame = frame.dropna().set_index("date").sort_index()
        frame.index.name = "date"
        return frame

    def save_history(self, ticker: str, frame: pd.DataFrame) -> None:
        if not self.enabled or frame.empty:
            return
        self._ensure_schema()
        symbol = ticker.upper()

        rows: list[tuple] = []
        for idx, row in frame.iterrows():
            rows.append(
                (
                    symbol,
                    idx.date(),
                    float(row["Open"]),
                    float(row["High"]),
                    float(row["Low"]),
                    float(row["Close"]),
                    float(row["Volume"]),
                )
            )

        with psycopg.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    insert into market_raw_data (
                        ticker, trade_date, open, high, low, close, volume
                    ) values (%s, %s, %s, %s, %s, %s, %s)
                    on conflict (ticker, trade_date) do update set
                        open = excluded.open,
                        high = excluded.high,
                        low = excluded.low,
                        close = excluded.close,
                        volume = excluded.volume
                    """,
                    rows,
                )
            conn.commit()

    def latest_trade_date(self, ticker: str) -> date | None:
        if not self.enabled:
            return None
        self._ensure_schema()
        with psycopg.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "select max(trade_date) from market_raw_data where ticker = %s",
                    (ticker.upper(),),
                )
                value = cur.fetchone()[0]
        return value

    def _ensure_schema(self) -> None:
        if self._schema_ready or not self.enabled:
            return
        with psycopg.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    create table if not exists market_raw_data (
                        ticker text not null,
                        trade_date date not null,
                        open numeric(14,6) not null,
                        high numeric(14,6) not null,
                        low numeric(14,6) not null,
                        close numeric(14,6) not null,
                        volume numeric(20,4) not null,
                        created_at timestamptz not null default now(),
                        primary key (ticker, trade_date)
                    )
                    """
                )
            conn.commit()
        self._schema_ready = True

