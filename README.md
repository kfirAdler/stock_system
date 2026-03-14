# Stock System MVP

Deterministic stock analysis and portfolio simulation project in clean OOP Python.

## Features

- Config-driven ticker universe loading
- Stooq historical OHLCV download with local CSV caching
- Technical feature engineering (returns, MA20/MA50, RSI14, volatility20, distance from 52-week high, relative strength)
- Weighted deterministic scoring and BUY/WATCH/AVOID classification
- Trade plan generation (entry, stop, target, reasons)
- Daily portfolio simulation with position limits, max position sizing, stop-loss, and take-profit
- CSV and JSON persistence for analysis and portfolio snapshots

## Project layout

See the `stock_system/` folders and modules for domain-separated responsibilities.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run analysis

```bash
cd stock_system
python -m app.main
```

Outputs are written under:

- `output/raw_data/`
- `output/analyses/`
- `output/portfolios/`

## Run tests

```bash
cd stock_system
pytest
```

## Run local dashboard

```bash
cd stock_system
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m dashboard.app
```

Open `http://127.0.0.1:5000` in your browser.

## Docker (EC2-ready)

Build and run with Docker Compose:

```bash
cp .env.example .env
docker compose up --build -d
```

App endpoints:

- `http://<server-ip>:5000/`
- `http://<server-ip>:5000/simulator`
- `http://<server-ip>:5000/health`

### Supabase mode

When `SAVE_TO_SUPABASE=true` and `SUPABASE_DB_URL` is set:

- raw OHLCV cache is saved in Supabase table `market_raw_data`
- scanner runs are saved to Supabase tables
- simulator actions and equity points are saved to Supabase
- dashboard loads latest run from Supabase first (falls back to local JSON if unavailable)
