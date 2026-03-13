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

## Deploy On Render

This project is ready for Render Web Service deployment.

### Option A: Blueprint (recommended)

1. Push this repo to GitHub.
2. In Render: `New` -> `Blueprint`.
3. Select this repository.
4. Render will use [`render.yaml`](/Users/kfiradler/projects/stock-api/stock_system/render.yaml).

### Option B: Manual Web Service

Use these settings:

- Environment: `Python`
- Build Command: `pip install -r requirements.txt`
- Start Command: `gunicorn dashboard.wsgi:app --workers 2 --threads 4 --timeout 120`

Set environment variables:

- `FLASK_SECRET_KEY` = any strong random value
- `OUTPUT_DIR` = `/var/data/output` (recommended when using Render Disk)
- `DATA_PROVIDER_MODE` = `auto` (recommended), `stooq`, or `yahoo`
- `SAVE_TO_SUPABASE` = `true` (if you want DB persistence)
- `SUPABASE_DB_URL` = Supabase Postgres connection URL

### Persistent data

The app writes cache/results under `OUTPUT_DIR`.
On Render, attach a persistent disk (example mount path `/var/data`) and set:

- `OUTPUT_DIR=/var/data/output`

### Supabase mode

When `SAVE_TO_SUPABASE=true` and `SUPABASE_DB_URL` is set:

- raw OHLCV cache is saved in Supabase table `market_raw_data`
- scanner runs are saved to Supabase tables
- simulator actions and equity points are saved to Supabase
- dashboard loads latest run from Supabase first (falls back to local JSON if unavailable)
