import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from state import _state

router = APIRouter(prefix="/predict", tags=["predict"])


class RegimeInput(BaseModel):
    natural_gas_price_mcf: float = Field(..., description="Natural gas price ($/MCF)")
    wti_crude_price_bbl: float = Field(..., description="WTI crude oil spot price ($/bbl)")
    renewable_share: float = Field(..., ge=0.0, le=1.0, description="Renewable fraction of total generation")
    avg_wind_speed_ms: float = Field(..., description="Average wind speed (m/s)")
    natural_gas_storage_bcf: float = Field(..., description="Working natural gas storage (BCF)")
    ng_price_vs_avg: float = Field(..., description="Natural gas price minus historical mean")


class RegimePrediction(BaseModel):
    regime_label: str
    regime_id: int
    confidence: float = Field(..., description="Softmax confidence of nearest centroid (0–1)")
    distances_to_centroids: dict[str, float]


class ForecastInput(BaseModel):
    natural_gas_storage_bcf: float
    wti_crude_price_bbl: float
    renewable_share: float = Field(..., ge=0.0, le=1.0)
    avg_wind_speed_ms: float
    avg_precipitation_mm: float
    month: int = Field(..., ge=1, le=12)
    ng_price_lag_1m: float = Field(..., description="Natural gas price one month ago")
    ng_price_lag_3m: float = Field(..., description="Natural gas price three months ago")
    ng_price_lag_12m: float = Field(..., description="Natural gas price twelve months ago")
    ng_price_volatility_3m: float = Field(..., description="3-month rolling price std dev")
    ng_price_change_rate: float = Field(..., description="Month-over-month price change")


class PriceForecast(BaseModel):
    model_config = {"protected_namespaces": ()}

    predicted_price_mcf: float = Field(..., description="Predicted natural gas price ($/MCF) in 3 months")
    horizon_months: int = 3
    model_test_mae: float


def _forecast_from_row(row: pd.Series) -> PriceForecast:
    """Run the price forecaster on a single engineered row."""
    bundle = _state["forecast"]
    model = bundle["model"]
    features: list[str] = bundle["features"]
    metrics: dict = bundle["metrics"]

    input_df = pd.DataFrame([row[features]])
    predicted = float(model.predict(input_df)[0])
    return PriceForecast(
        predicted_price_mcf=round(predicted, 4),
        horizon_months=bundle["horizon_months"],
        model_test_mae=round(metrics.get("test_mae", float("nan")), 4),
    )


@router.get("/price/latest", response_model=PriceForecast)
def predict_price_latest() -> PriceForecast:
    """
    Forecast natural gas price 3 months ahead using the most recent row in the
    dataset. No inputs required — features are pulled from the loaded history.
    """
    engineered_df = _state.get("engineered_df")
    if engineered_df is None:
        raise HTTPException(
            status_code=503,
            detail="Engineered dataset not loaded. Run `python exploration/train_models.py` first.",
        )
    return _forecast_from_row(engineered_df.iloc[-1])


@router.get("/price/{year_month}", response_model=PriceForecast)
def predict_price_by_date(year_month: str) -> PriceForecast:
    """
    Forecast natural gas price 3 months ahead using features from a specific
    historical month (format: YYYY-MM). Features are pulled from the loaded dataset.
    """
    engineered_df = _state.get("engineered_df")
    if engineered_df is None:
        raise HTTPException(
            status_code=503,
            detail="Engineered dataset not loaded. Run `python exploration/train_models.py` first.",
        )
    matches = engineered_df[engineered_df["year_month"] == year_month]
    if matches.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for {year_month}. Available range: "
            f"{engineered_df['year_month'].min()} to {engineered_df['year_month'].max()}.",
        )
    return _forecast_from_row(matches.iloc[0])


@router.post("/regime", response_model=RegimePrediction)
def predict_regime(body: RegimeInput) -> RegimePrediction:
    """
    Classify a set of monthly energy market conditions into a regime
    (STABLE / VOLATILE / CHAOTIC) using the trained KMeans classifier.
    """
    bundle = _state["regime"]
    scaler = bundle["scaler"]
    model = bundle["model"]
    label_map: dict[int, str] = bundle["label_map"]
    centroids: list[list[float]] = bundle["centroids"]
    features: list[str] = bundle["features"]

    row = np.array([[getattr(body, f) for f in features]])
    scaled = scaler.transform(row)

    cluster_id = int(model.predict(scaled)[0])
    regime_label = label_map[cluster_id]
    regime_id = {"STABLE": 0, "VOLATILE": 1, "CHAOTIC": 2}[regime_label]

    dists = {
        label_map[i]: float(np.linalg.norm(scaled[0] - np.array(c)))
        for i, c in enumerate(centroids)
    }
    neg_dists = np.array([-dists[label_map[i]] for i in range(len(centroids))])
    softmax = np.exp(neg_dists - neg_dists.max())
    softmax /= softmax.sum()
    confidence = float(softmax[cluster_id])

    return RegimePrediction(
        regime_label=regime_label,
        regime_id=regime_id,
        confidence=round(confidence, 4),
        distances_to_centroids={k: round(v, 4) for k, v in dists.items()},
    )


@router.post("/price", response_model=PriceForecast)
def predict_price(body: ForecastInput) -> PriceForecast:
    """
    Forecast natural gas price ($/MCF) three months ahead using the trained
    HistGradientBoostingRegressor.
    """
    bundle = _state["forecast"]
    model = bundle["model"]
    features: list[str] = bundle["features"]
    metrics: dict = bundle["metrics"]

    row = np.array([[getattr(body, f) for f in features]])
    predicted = float(model.predict(row)[0])

    return PriceForecast(
        predicted_price_mcf=round(predicted, 4),
        horizon_months=bundle["horizon_months"],
        model_test_mae=round(metrics.get("test_mae", float("nan")), 4),
    )
