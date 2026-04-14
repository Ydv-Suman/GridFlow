from fastapi import APIRouter, HTTPException

from state import _state

router = APIRouter(prefix="/analytics", tags=["analytics"])

_NOT_FOUND = "Run exploration/run_analytics.py first."


@router.get("/stats")
def get_stats() -> dict:
    """Descriptive statistics (phase 2)."""
    data = _state.get("descriptive_statistics")
    if not data:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    return data


@router.get("/correlations")
def get_correlations() -> dict:
    """Correlation analysis (phase 2)."""
    data = _state.get("correlation_analysis")
    if not data:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    return data


@router.get("/regimes")
def get_regimes() -> dict:
    """Regime statistics from KMeans clustering (phase 3)."""
    data = _state.get("regime_statistics")
    if not data:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    return data


@router.get("/temporal")
def get_temporal() -> dict:
    """Monthly and seasonal temporal patterns (phase 4)."""
    data = _state.get("temporal_patterns")
    if not data:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    return data


@router.get("/simulation")
def get_simulation() -> dict:
    """Procurement shifting simulation results (phase 5/6)."""
    data = _state.get("simulation_summary")
    if not data:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    return data


@router.get("/findings")
def get_findings() -> dict:
    """Key findings summary (phases 2-7)."""
    data = _state.get("findings")
    if not data:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    return data


@router.get("/forecast-metrics")
def get_forecast_metrics() -> dict:
    """Price forecaster train/test metrics (phase 7)."""
    data = _state.get("forecast_metrics")
    if not data:
        raise HTTPException(status_code=404, detail="Run exploration/train_models.py first.")
    return data
