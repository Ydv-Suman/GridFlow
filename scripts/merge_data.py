"""
Merge raw EIA and NOAA CSVs into a single monthly dataset.

Reads from data/raw/ and writes data/merged/energy_monthly.csv.
All datasets are aggregated to YYYY-MM granularity and outer-joined on
year_month.

Usage:
    python scripts/merge_data.py
    python scripts/merge_data.py --start 2020-01 --end 2024-12
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT_DIR / "data" / "raw"
MERGED_DIR = ROOT_DIR / "data" / "merged"

# Key fuel types to pivot from generation data (US national)
GENERATION_FUELS = {
    "ALL": "gen_all_fuels_mwh",
    "NG":  "gen_natural_gas_mwh",
    "NUC": "gen_nuclear_mwh",
    "COW": "gen_coal_mwh",
    "WND": "gen_wind_mwh",
    "TSN": "gen_solar_mwh",
    "HYC": "gen_hydro_mwh",
    "REN": "gen_renewables_mwh",
}

# Lower-48 total working underground storage series
NG_STORAGE_SERIES = "NW2_EPG0_SWO_R48_BCF"

# US national commercial natural gas price
NG_PRICE_SERIES = "N3020US3"

# WTI crude oil spot price
WTI_SERIES = "RWTC"


def to_year_month(series: pd.Series) -> pd.Series:
    """Convert a date-like series to YYYY-MM strings."""
    return pd.to_datetime(series).dt.to_period("M").astype(str)


def load_generation(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df[df["location"] == "US"].copy()
    df = df[df["fueltypeid"].isin(GENERATION_FUELS)].copy()
    df["year_month"] = to_year_month(df["period"])
    df["generation"] = pd.to_numeric(df["generation"], errors="coerce")

    pivoted = (
        df.groupby(["year_month", "fueltypeid"])["generation"]
        .mean()
        .unstack("fueltypeid")
        .rename(columns=GENERATION_FUELS)
        .reset_index()
    )
    # Only keep columns that actually exist in the data
    keep = ["year_month"] + [c for c in GENERATION_FUELS.values() if c in pivoted.columns]
    return pivoted[keep]


def load_natural_gas_prices(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df[df["series"] == NG_PRICE_SERIES].copy()
    df["year_month"] = to_year_month(df["period"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    result = (
        df.groupby("year_month")["value"]
        .mean()
        .reset_index()
        .rename(columns={"value": "natural_gas_price_mcf"})
    )
    return result


def load_natural_gas_storage(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df[df["series"] == NG_STORAGE_SERIES].copy()
    df["year_month"] = to_year_month(df["period"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    result = (
        df.groupby("year_month")["value"]
        .mean()
        .reset_index()
        .rename(columns={"value": "natural_gas_storage_bcf"})
    )
    return result


def load_petroleum_prices(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df[df["series"] == WTI_SERIES].copy()
    df["year_month"] = to_year_month(df["period"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    result = (
        df.groupby("year_month")["value"]
        .mean()
        .reset_index()
        .rename(columns={"value": "wti_crude_price_bbl"})
    )
    return result


def load_noaa(path: Path, col_name: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["year_month"] = to_year_month(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    result = (
        df.groupby("year_month")["value"]
        .mean()
        .reset_index()
        .rename(columns={"value": col_name})
    )
    return result


def merge_all(start: str | None, end: str | None) -> pd.DataFrame:
    frames = [
        load_generation(RAW_DIR / "electricity_generation.csv"),
        load_natural_gas_prices(RAW_DIR / "natural_gas_prices.csv"),
        load_natural_gas_storage(RAW_DIR / "natural_gas_storage.csv"),
        load_petroleum_prices(RAW_DIR / "petroleum_prices.csv"),
        load_noaa(RAW_DIR / "noaa_monthly_precipitation.csv", "avg_precipitation_mm"),
        load_noaa(RAW_DIR / "noaa_wind_speed.csv", "avg_wind_speed_ms"),
    ]

    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on="year_month", how="outer")

    merged = merged.sort_values("year_month").reset_index(drop=True)

    if start:
        merged = merged[merged["year_month"] >= start]
    if end:
        merged = merged[merged["year_month"] <= end]

    return merged


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge EIA and NOAA raw CSVs into a monthly energy dataset."
    )
    parser.add_argument("--start", default=None, help="First year-month to include (YYYY-MM).")
    parser.add_argument("--end", default=None, help="Last year-month to include (YYYY-MM).")
    parser.add_argument(
        "--output",
        type=Path,
        default=MERGED_DIR / "energy_monthly.csv",
        help="Destination path for the merged CSV.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    merged = merge_all(args.start, args.end)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.output, index=False)

    print(f"Saved {len(merged):,} rows x {len(merged.columns)} columns -> {args.output}")
    print(f"Coverage: {merged['year_month'].min()} to {merged['year_month'].max()}")
    print(f"Columns: {merged.columns.tolist()}")
    null_counts = merged.isna().sum()
    if null_counts.any():
        print(f"Null cells per column:\n{null_counts[null_counts > 0].to_string()}")


if __name__ == "__main__":
    main()
