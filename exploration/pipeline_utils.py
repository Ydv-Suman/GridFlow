from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import StandardScaler

ROOT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT_DIR / "exploration" / "output"
MODEL_DIR = ROOT_DIR / "backend" / "models"
MERGED_DIR = ROOT_DIR / "data" / "merged"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
MODEL_DIR.mkdir(parents=True, exist_ok=True)
MERGED_DIR.mkdir(parents=True, exist_ok=True)

REGIME_FEATURES = [
    "natural_gas_price_mcf",
    "wti_crude_price_bbl",
    "renewable_share",
    "avg_wind_speed_ms",
    "natural_gas_storage_bcf",
    "ng_price_vs_avg",
]
FORECAST_FEATURES = [
    "natural_gas_storage_bcf",
    "wti_crude_price_bbl",
    "renewable_share",
    "avg_wind_speed_ms",
    "avg_precipitation_mm",
    "month",
    "ng_price_lag_1m",
    "ng_price_lag_3m",
    "ng_price_lag_12m",
    "ng_price_volatility_3m",
    "ng_price_change_rate",
]


@dataclass
class AnalyticsArtifacts:
    labeled_data: pd.DataFrame
    descriptive_statistics: dict
    correlation_analysis: dict
    temporal_patterns: dict
    regime_statistics: dict
    simulation_summary: dict
    findings: dict


def load_merged_dataset(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["year_month"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return _engineer_features(df)


def _engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().sort_values("timestamp").reset_index(drop=True)

    df["month"] = df["timestamp"].dt.month
    df["year"] = df["timestamp"].dt.year
    df["quarter"] = df["timestamp"].dt.quarter
    df["is_winter"] = df["month"].isin([12, 1, 2]).astype(int)
    df["is_summer"] = df["month"].isin([6, 7, 8]).astype(int)

    df["natural_gas_storage_bcf"] = (
        df["natural_gas_storage_bcf"].interpolate(method="linear").bfill().ffill()
    )

    df["ng_price_lag_1m"] = df["natural_gas_price_mcf"].shift(1)
    df["ng_price_lag_3m"] = df["natural_gas_price_mcf"].shift(3)
    df["ng_price_lag_12m"] = df["natural_gas_price_mcf"].shift(12)
    df["ng_price_change_rate"] = df["natural_gas_price_mcf"].diff()
    df["ng_price_vs_avg"] = df["natural_gas_price_mcf"] - df["natural_gas_price_mcf"].mean()
    df["ng_price_volatility_3m"] = (
        df["natural_gas_price_mcf"].rolling(window=3, min_periods=1).std()
    )
    df["storage_vs_avg"] = (
        df["natural_gas_storage_bcf"] - df["natural_gas_storage_bcf"].mean()
    )
    df["renewable_share"] = (
        df["gen_renewables_mwh"] / df["gen_all_fuels_mwh"].replace(0, np.nan)
    ).fillna(0.0)

    for col in ("ng_price_lag_1m", "ng_price_lag_3m", "ng_price_lag_12m"):
        df[col] = df[col].fillna(df["natural_gas_price_mcf"])
    df["ng_price_change_rate"] = df["ng_price_change_rate"].fillna(0.0)
    df["ng_price_volatility_3m"] = df["ng_price_volatility_3m"].fillna(0.0)

    return df


def descriptive_statistics(df: pd.DataFrame) -> dict:
    price_series = df["natural_gas_price_mcf"]
    wind_series = df["avg_wind_speed_ms"]
    monthly_profile = (
        df.groupby("month")[["natural_gas_price_mcf", "wti_crude_price_bbl"]]
        .mean()
        .round(3)
        .reset_index()
    )
    seasonal_price = (
        df.groupby("quarter")["natural_gas_price_mcf"].mean().round(3).to_dict()
    )
    return {
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "timestamp_start": df["timestamp"].min().isoformat(),
        "timestamp_end": df["timestamp"].max().isoformat(),
        "price": {
            "mean": float(price_series.mean()),
            "median": float(price_series.median()),
            "std": float(price_series.std()),
            "min": float(price_series.min()),
            "max": float(price_series.max()),
            "range": float(price_series.max() - price_series.min()),
        },
        "wind": {
            "mean": float(wind_series.mean()),
            "median": float(wind_series.median()),
            "std": float(wind_series.std()),
            "min": float(wind_series.min()),
            "max": float(wind_series.max()),
        },
        "monthly_profile": monthly_profile.to_dict(orient="records"),
        "seasonal_price_profile": {str(k): float(v) for k, v in seasonal_price.items()},
        "missing_cells": int(df.isna().sum().sum()),
    }


def correlation_analysis(df: pd.DataFrame) -> dict:
    safe_columns = [
        "natural_gas_price_mcf",
        "wti_crude_price_bbl",
        "natural_gas_storage_bcf",
        "avg_wind_speed_ms",
        "avg_precipitation_mm",
        "renewable_share",
        "ng_price_lag_1m",
        "ng_price_lag_12m",
        "ng_price_vs_avg",
    ]
    corr = df[safe_columns].corr(numeric_only=True)
    return {
        "price_vs_wti": float(corr.loc["natural_gas_price_mcf", "wti_crude_price_bbl"]),
        "price_vs_storage": float(corr.loc["natural_gas_price_mcf", "natural_gas_storage_bcf"]),
        "price_vs_wind": float(corr.loc["natural_gas_price_mcf", "avg_wind_speed_ms"]),
        "price_vs_price_lag_1m": float(corr.loc["natural_gas_price_mcf", "ng_price_lag_1m"]),
        "price_vs_price_lag_12m": float(corr.loc["natural_gas_price_mcf", "ng_price_lag_12m"]),
        "price_vs_renewable_share": float(
            corr.loc["natural_gas_price_mcf", "renewable_share"]
        ),
        "correlation_matrix": corr.round(4).to_dict(),
    }


def assign_regime_labels(summary_by_cluster: pd.DataFrame) -> dict[int, str]:
    stable_cluster = (
        summary_by_cluster.assign(
            score=summary_by_cluster["avg_price"] + summary_by_cluster["avg_volatility"]
        )
        .sort_values("score")
        .iloc[0]["cluster_id"]
    )
    remaining = summary_by_cluster[summary_by_cluster["cluster_id"] != stable_cluster]
    volatile_cluster = (
        remaining.assign(score=remaining["avg_price"] + remaining["avg_wti"])
        .sort_values("score", ascending=False)
        .iloc[0]["cluster_id"]
    )
    chaotic_cluster = next(
        cluster_id
        for cluster_id in summary_by_cluster["cluster_id"].tolist()
        if cluster_id not in {stable_cluster, volatile_cluster}
    )
    return {
        int(stable_cluster): "STABLE",
        int(volatile_cluster): "VOLATILE",
        int(chaotic_cluster): "CHAOTIC",
    }


def regime_discovery(df: pd.DataFrame) -> tuple[pd.DataFrame, dict, dict]:
    features = df[REGIME_FEATURES]
    scaler = StandardScaler()
    scaled = scaler.fit_transform(features)

    model = KMeans(n_clusters=3, random_state=42, n_init=20)
    clusters = model.fit_predict(scaled)

    labeled = df.copy()
    labeled["cluster_id"] = clusters

    cluster_summary = (
        labeled.groupby("cluster_id")
        .agg(
            count=("timestamp", "size"),
            avg_price=("natural_gas_price_mcf", "mean"),
            avg_wti=("wti_crude_price_bbl", "mean"),
            avg_volatility=("ng_price_volatility_3m", "mean"),
        )
        .reset_index()
    )

    label_map = assign_regime_labels(cluster_summary)
    labeled["regime_label"] = labeled["cluster_id"].map(label_map)
    labeled["regime_id"] = labeled["regime_label"].map(
        {"STABLE": 0, "VOLATILE": 1, "CHAOTIC": 2}
    )

    regime_stats: dict = {}
    total_rows = len(labeled)
    for regime_label, group in labeled.groupby("regime_label"):
        top_months = (
            group["month"].value_counts().sort_values(ascending=False).head(4).index.tolist()
        )
        regime_stats[regime_label] = {
            "months": int(len(group)),
            "share_pct": float(len(group) / total_rows * 100),
            "avg_ng_price_mcf": float(group["natural_gas_price_mcf"].mean()),
            "price_std_mcf": float(group["natural_gas_price_mcf"].std()),
            "avg_wti_price_bbl": float(group["wti_crude_price_bbl"].mean()),
            "avg_price_volatility_3m": float(group["ng_price_volatility_3m"].mean()),
            "typical_months": [int(m) for m in top_months],
        }

    model_bundle = {
        "scaler": scaler,
        "model": model,
        "features": REGIME_FEATURES,
        "label_map": label_map,
        "centroids": model.cluster_centers_.tolist(),
    }
    return labeled, regime_stats, model_bundle


def temporal_patterns(df: pd.DataFrame) -> dict:
    monthly = (
        df.groupby("month")[
            ["natural_gas_price_mcf", "ng_price_volatility_3m", "avg_wind_speed_ms"]
        ]
        .mean()
        .round(3)
        .reset_index()
    )
    regime_monthly = (
        df.groupby(["month", "regime_label"])
        .size()
        .reset_index(name="count")
        .sort_values(["month", "count"], ascending=[True, False])
    )
    peak_month = int(
        monthly.sort_values("natural_gas_price_mcf", ascending=False).iloc[0]["month"]
    )
    lowest_month = int(
        monthly.sort_values("natural_gas_price_mcf", ascending=True).iloc[0]["month"]
    )
    winter_avg = float(df.loc[df["is_winter"] == 1, "natural_gas_price_mcf"].mean())
    non_winter_avg = float(df.loc[df["is_winter"] == 0, "natural_gas_price_mcf"].mean())
    winter_premium_pct = (
        (winter_avg - non_winter_avg) / non_winter_avg * 100 if non_winter_avg else 0.0
    )
    return {
        "monthly_averages": monthly.to_dict(orient="records"),
        "dominant_regime_by_month": (
            regime_monthly.groupby("month").first().reset_index().to_dict(orient="records")
        ),
        "peak_month": peak_month,
        "lowest_price_month": lowest_month,
        "winter_premium_pct": float(winter_premium_pct),
    }


def _build_simulation_jobs(df: pd.DataFrame) -> pd.DataFrame:
    jobs: list[dict] = []
    for row in df.itertuples(index=False):
        ts = row.timestamp
        jobs.append(
            {
                "job_type": "interactive",
                "requested_at": ts,
                "baseline_at": ts,
                "window_months": 0,
                "energy_mcf": 5.0,
            }
        )
        jobs.append(
            {
                "job_type": "flexible",
                "requested_at": ts,
                "baseline_at": ts,
                "window_months": 2,
                "energy_mcf": 12.0,
            }
        )
        if row.month in (1, 4, 7, 10):
            jobs.append(
                {
                    "job_type": "offline",
                    "requested_at": ts,
                    "baseline_at": ts,
                    "window_months": 3,
                    "energy_mcf": 30.0,
                }
            )
    return pd.DataFrame(jobs)


def _choose_optimized_slot(job: pd.Series, df: pd.DataFrame) -> pd.Series:
    if job["window_months"] == 0:
        match = df.loc[df["timestamp"] == job["baseline_at"]]
        return match.iloc[0] if not match.empty else df.iloc[0]

    start = job["requested_at"] - pd.DateOffset(months=int(job["window_months"]))
    end = job["requested_at"] + pd.DateOffset(months=int(job["window_months"]))
    candidates = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)].copy()
    if candidates.empty:
        candidates = df

    if job["job_type"] == "flexible":
        stable = candidates[candidates["regime_label"] != "CHAOTIC"]
        if not stable.empty:
            candidates = stable

    return (
        candidates.sort_values(
            ["natural_gas_price_mcf", "ng_price_volatility_3m", "timestamp"],
            ascending=[True, True, True],
        )
        .iloc[0]
    )


def simulation_summary(df: pd.DataFrame) -> dict:
    jobs = _build_simulation_jobs(df)
    baseline_cost = 0.0
    optimized_cost = 0.0
    job_rows = []

    for job in jobs.to_dict(orient="records"):
        job_series = pd.Series(job)
        baseline_match = df.loc[df["timestamp"] == job_series["baseline_at"]]
        if baseline_match.empty:
            continue
        baseline_slot = baseline_match.iloc[0]
        optimized_slot = _choose_optimized_slot(job_series, df)

        baseline = float(baseline_slot["natural_gas_price_mcf"] * job_series["energy_mcf"])
        optimized = float(optimized_slot["natural_gas_price_mcf"] * job_series["energy_mcf"])

        baseline_cost += baseline
        optimized_cost += optimized
        job_rows.append(
            {
                "job_type": job_series["job_type"],
                "requested_at": str(job_series["requested_at"]),
                "scheduled_at": str(optimized_slot["timestamp"]),
                "baseline_cost": baseline,
                "optimized_cost": optimized,
                "savings": baseline - optimized,
            }
        )

    jobs_df = pd.DataFrame(job_rows)
    summary_by_type = (
        jobs_df.groupby("job_type")[["baseline_cost", "optimized_cost", "savings"]]
        .sum()
        .round(3)
        .reset_index()
        .to_dict(orient="records")
    )
    total_savings = baseline_cost - optimized_cost
    return {
        "job_count": int(len(jobs_df)),
        "baseline_cost": round(baseline_cost, 3),
        "optimized_cost": round(optimized_cost, 3),
        "total_savings": round(total_savings, 3),
        "savings_pct": round((total_savings / baseline_cost) * 100, 3) if baseline_cost else 0.0,
        "by_job_type": summary_by_type,
    }


def build_findings(
    stats: dict,
    correlations: dict,
    temporal: dict,
    regimes: dict,
    simulation: dict,
) -> dict:
    return {
        "finding_1": {
            "title": "Monthly energy prices separate into three market regimes",
            "evidence": {
                "regime_shares_pct": {
                    regime: round(values["share_pct"], 2) for regime, values in regimes.items()
                }
            },
        },
        "finding_2": {
            "title": "WTI crude is a material driver of natural gas price",
            "evidence": {"price_vs_wti_correlation": round(correlations["price_vs_wti"], 4)},
        },
        "finding_3": {
            "title": "Storage levels have an inverse relationship with price",
            "evidence": {
                "price_vs_storage_correlation": round(correlations["price_vs_storage"], 4)
            },
        },
        "finding_4": {
            "title": "Procurement shifting reduces modeled spend",
            "evidence": {
                "baseline_cost": simulation["baseline_cost"],
                "optimized_cost": simulation["optimized_cost"],
                "savings_pct": simulation["savings_pct"],
            },
        },
        "finding_5": {
            "title": "Lagged prices are strongly predictive of current price",
            "evidence": {
                "price_vs_lag_1m_correlation": round(correlations["price_vs_price_lag_1m"], 4),
                "price_vs_lag_12m_correlation": round(correlations["price_vs_price_lag_12m"], 4),
            },
        },
        "finding_6": {
            "title": "Winter months carry a price premium over non-winter months",
            "evidence": {"winter_premium_pct": round(temporal["winter_premium_pct"], 3)},
        },
        "finding_7": {
            "title": "The merged dataset is suitable for downstream modeling",
            "evidence": {
                "rows": stats["row_count"],
                "columns": stats["column_count"],
                "missing_cells": stats["missing_cells"],
            },
        },
    }


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)


def run_analytics(merged_path: Path) -> AnalyticsArtifacts:
    df = load_merged_dataset(merged_path)
    labeled, regime_stats, regime_model_bundle = regime_discovery(df)
    stats = descriptive_statistics(labeled)
    correlations = correlation_analysis(labeled)
    temporal = temporal_patterns(labeled)
    simulation = simulation_summary(labeled)
    findings = build_findings(stats, correlations, temporal, regime_stats, simulation)

    labeled.to_csv(MERGED_DIR / "energy_monthly_regimes.csv", index=False)
    save_json(OUTPUT_DIR / "descriptive_statistics.json", stats)
    save_json(OUTPUT_DIR / "correlation_analysis.json", correlations)
    save_json(OUTPUT_DIR / "temporal_patterns.json", temporal)
    save_json(OUTPUT_DIR / "regime_statistics.json", regime_stats)
    save_json(OUTPUT_DIR / "simulation_summary.json", simulation)
    save_json(OUTPUT_DIR / "findings.json", findings)
    joblib.dump(regime_model_bundle, MODEL_DIR / "regime_classifier.pkl")

    return AnalyticsArtifacts(
        labeled_data=labeled,
        descriptive_statistics=stats,
        correlation_analysis=correlations,
        temporal_patterns=temporal,
        regime_statistics=regime_stats,
        simulation_summary=simulation,
        findings=findings,
    )


def train_price_forecaster(df: pd.DataFrame) -> dict:
    training_df = df.copy()
    training_df["target_price_3m"] = training_df["natural_gas_price_mcf"].shift(-3)
    training_df = training_df.dropna(subset=["target_price_3m"]).reset_index(drop=True)

    split_index = max(int(len(training_df) * 0.8), 1)
    train_df = training_df.iloc[:split_index]
    test_df = training_df.iloc[split_index:]

    model = HistGradientBoostingRegressor(random_state=42)
    model.fit(train_df[FORECAST_FEATURES], train_df["target_price_3m"])

    train_predictions = model.predict(train_df[FORECAST_FEATURES])
    metrics = {
        "train_mae": float(mean_absolute_error(train_df["target_price_3m"], train_predictions)),
        "train_rmse": float(
            np.sqrt(mean_squared_error(train_df["target_price_3m"], train_predictions))
        ),
    }

    if not test_df.empty:
        test_predictions = model.predict(test_df[FORECAST_FEATURES])
        metrics["test_mae"] = float(
            mean_absolute_error(test_df["target_price_3m"], test_predictions)
        )
        metrics["test_rmse"] = float(
            np.sqrt(mean_squared_error(test_df["target_price_3m"], test_predictions))
        )

    bundle = {
        "model": model,
        "features": FORECAST_FEATURES,
        "target": "target_price_3m",
        "horizon_months": 3,
        "metrics": metrics,
    }
    joblib.dump(bundle, MODEL_DIR / "price_forecaster.pkl")
    save_json(OUTPUT_DIR / "forecast_metrics.json", metrics)
    return metrics
