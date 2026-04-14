# GridFlow API — Phase 8
# Serves:
#   GET /data             - merged monthly dataset
#   GET /analytics/*      - analytics outputs (phases 2-6)
#   POST /predict/regime  - regime classifier (phase 3)
#   POST /predict/price   - price forecaster (phase 7)

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

import joblib
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from state import _state
from routes import analytics, data, predict

ROOT_DIR = Path(__file__).resolve().parent.parent
MERGED_DIR = ROOT_DIR / "data" / "merged"
MODEL_DIR = ROOT_DIR / "backend" / "models"
OUTPUT_DIR = ROOT_DIR / "exploration" / "output"


def _load_json(name: str) -> dict:
    path = OUTPUT_DIR / f"{name}.json"
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


@asynccontextmanager
async def lifespan(app: FastAPI):
    regime_path = MODEL_DIR / "regime_classifier.pkl"
    forecast_path = MODEL_DIR / "price_forecaster.pkl"
    data_path = MERGED_DIR / "energy_monthly.csv"

    if not regime_path.exists() or not forecast_path.exists():
        raise RuntimeError(
            "Model files not found. Run `python exploration/train_models.py` first."
        )
    if not data_path.exists():
        raise RuntimeError(
            "Merged dataset not found. Run `python scripts/merge_data.py` first."
        )

    _state["regime"] = joblib.load(regime_path)
    _state["forecast"] = joblib.load(forecast_path)
    _state["df"] = pd.read_csv(data_path)

    for key in (
        "descriptive_statistics",
        "correlation_analysis",
        "temporal_patterns",
        "regime_statistics",
        "simulation_summary",
        "findings",
        "forecast_metrics",
    ):
        _state[key] = _load_json(key)

    yield
    _state.clear()


app = FastAPI(
    title="GridFlow API",
    description="Monthly energy market analytics and price forecasting.",
    version="1.0.0",
    lifespan=lifespan,
)

# Configure CORS for the frontend origins
origins = os.getenv("CORS_ORIGINS", "")
origins = [o.strip() for o in origins.split(",") if o]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "models_loaded": "regime" in _state and "forecast" in _state,
        "dataset_rows": len(_state.get("df", [])),
    }

# Register route modules
app.include_router(data.router)
app.include_router(analytics.router)
app.include_router(predict.router)