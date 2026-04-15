# GridFlow

GridFlow is a full-stack energy analytics platform focused on monthly U.S. power-market conditions. It brings together public energy and weather data, builds a unified modeling dataset, detects operating regimes, and exposes forecasting results through a FastAPI backend and a React frontend.

The project is structured as an end-to-end workflow rather than a single script:

- `scripts/` fetch raw data from EIA and NOAA, then merge it into a monthly feature table.
- `exploration/` runs analytics, clustering, simulations, and model training.
- `backend/` serves the dataset, analytics outputs, and prediction endpoints.
- `frontend/` provides a dashboard for exploration and prediction.

## What The Project Does

GridFlow is designed to help answer questions such as:

- How are fuel prices, storage, renewable generation, and weather moving together?
- What market regimes show up in the historical data?
- When do conditions look stable versus volatile or chaotic?
- How far can natural gas prices be forecast using recent energy and weather signals?

The current implementation combines:

- EIA electricity generation, natural gas pricing, storage, and petroleum pricing data
- NOAA weather inputs such as precipitation and wind speed
- monthly aggregation and feature engineering
- descriptive analytics and temporal analysis
- KMeans-based regime classification
- natural gas price forecasting
- a browsable API and lightweight frontend for inspection

## Architecture

```text
GridFlow/
├── backend/            # FastAPI app, routes, models, runtime config
├── data/
│   ├── raw/            # Raw source CSVs fetched from public APIs
│   └── merged/         # Merged and engineered monthly datasets
├── exploration/        # Analytics pipeline, regime analysis, model training
├── frontend/           # React + Vite web client
├── scripts/            # Ingestion and data merge entry points
└── docker-compose.yml  # Full-stack local container orchestration
```

## Core Workflow

The main workflow is:

1. Fetch raw energy and weather series into `data/raw/`.
2. Merge raw files into `data/merged/energy_monthly.csv`.
3. Run analytics to generate summary JSON outputs and labeled datasets.
4. Train and export the regime and forecasting models.
5. Serve the outputs through the API and frontend.

This means the backend depends on generated artifacts. If the merged dataset or trained models do not exist yet, the API will fail at startup until the pipeline has been run.

## Data Sources

GridFlow currently uses public datasets from:

- U.S. Energy Information Administration (EIA)
- National Oceanic and Atmospheric Administration (NOAA)

Examples of merged signals include:

- natural gas price
- natural gas storage
- WTI crude oil price
- electricity generation by fuel type
- wind speed
- precipitation

These are transformed to a common `YYYY-MM` monthly grain before analytics and modeling.

## Features

### Data Ingestion

- `scripts/fetch_eia_data.py` downloads energy market series from EIA
- `scripts/fetch_noaa_data.py` downloads weather series from NOAA
- `scripts/merge_data.py` standardizes and joins the raw files into one monthly dataset

### Analytics

The analytics pipeline produces reusable outputs for the API and frontend, including:

- descriptive statistics
- correlation analysis
- temporal and seasonal patterns
- regime statistics
- simulation summaries
- findings summaries
- forecast metrics

### Modeling

The project currently exposes two predictive capabilities:

- regime classification: labels market conditions as `STABLE`, `VOLATILE`, or `CHAOTIC`
- price forecasting: predicts natural gas price several months ahead from engineered monthly features

### Backend API

The FastAPI service loads:

- merged monthly data from `data/merged/`
- trained models from `backend/models/`
- analytics JSON outputs from `exploration/output/`

It then exposes data retrieval, analytics, and prediction endpoints.

### Frontend

The React frontend includes pages for:

- dashboard views
- data exploration
- prediction workflows

## Prerequisites

- Python 3.10+
- Node.js 18+
- Docker and Docker Compose optional for containerized runs
- API keys for EIA and NOAA

## Environment Setup

Create and activate the Python environment from the repo root:

```bash
python3 -m venv backend/.venv
source backend/.venv/bin/activate
pip install -r backend/requirements.txt
```

Create `backend/.env`:

```env
EIA_API_KEY=your_eia_key
NOAA_API_KEY=your_noaa_key
```

For frontend development, create `frontend/.env` if needed:

```env
VITE_API_BASE_URL=http://localhost:8000
```

## Running The Pipeline

Run all scripts from the repository root so relative paths resolve correctly.

### 1. Fetch EIA Data

```bash
python scripts/fetch_eia_data.py --start 2020-01-01 --end 2024-12-31
```

### 2. Fetch NOAA Data

```bash
python scripts/fetch_noaa_data.py --start 2020-01-01 --end 2024-12-31
```

### 3. Merge The Monthly Dataset

```bash
python scripts/merge_data.py --start 2020-01 --end 2024-12
```

This produces the main merged table used by downstream analytics and models.

### 4. Generate Analytics Outputs

```bash
python exploration/run_analytics.py
```

### 5. Train Models

```bash
python exploration/train_models.py
```

This step writes trained artifacts used by the backend prediction endpoints.

## Running The Backend

```bash
source backend/.venv/bin/activate
cd backend
uvicorn main:app --reload
```

Default backend URL:

```text
http://localhost:8000
```

Health check:

```bash
curl http://localhost:8000/health
```

Swagger docs:

```text
http://localhost:8000/docs
```

ReDoc:

```text
http://localhost:8000/redoc
```

## Running The Frontend

```bash
cd frontend
npm install
npm run dev
```

Default frontend URL:

```text
http://localhost:5173
```

## API Overview

### Health

- `GET /health`

### Data

- `GET /data`
- `GET /data/columns`

### Analytics

- `GET /analytics/stats`
- `GET /analytics/correlations`
- `GET /analytics/regimes`
- `GET /analytics/temporal`
- `GET /analytics/simulation`
- `GET /analytics/findings`
- `GET /analytics/forecast-metrics`

### Predictions

- `POST /predict/regime`
- `POST /predict/price`
- `GET /predict/price/latest`
- `GET /predict/price/{year_month}`

## Docker

Create a root `.env` for Docker Compose:

```env
CORS_ORIGINS=http://localhost:8080
FRONTEND_PORT=8080
PORT=8080
API_UPSTREAM=http://backend:8000
```

Start both services:

```bash
docker compose up --build
```

Service URLs:

- frontend: `http://localhost:8080`
- backend: `http://localhost:8000`

## Development Notes

- Keep API keys in `backend/.env`; do not commit secrets.
- Treat generated data and model artifacts as build outputs unless the team explicitly wants them versioned.
- If you change ingestion, analytics, or model code, rerun the relevant pipeline steps before validating the API.

## Quick Validation

Use a narrow date range for fast checks:

- run the fetch scripts and confirm files appear in `data/raw/`
- run `scripts/merge_data.py` and confirm `data/merged/energy_monthly.csv` exists
- run analytics and training to generate outputs and models
- start the backend and confirm `/health` returns `status: ok`
- start the frontend and verify it loads backend-driven content
