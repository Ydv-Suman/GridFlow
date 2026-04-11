"""
Merge all raw EIA and NOAA CSVs into a single monthly feature table.

Output: data/merged/energy_monthly.csv

Columns produced
----------------
year_month                  : YYYY-MM (monthly index)
gen_total_MWh               : U.S. national total electricity generation (all fuels), thousand MWh
gen_natural_gas_MWh         : natural gas generation, thousand MWh
gen_coal_MWh                : all coal products, thousand MWh
gen_nuclear_MWh             : nuclear, thousand MWh
gen_wind_MWh                : wind, thousand MWh
gen_solar_MWh               : estimated total solar (utility + distributed), thousand MWh
gen_hydro_MWh               : conventional hydroelectric, thousand MWh
gen_petroleum_MWh           : petroleum, thousand MWh
natgas_price_usd_mcf        : U.S. national average natural-gas citygate price, $/MCF
natgas_storage_lower48_bcf  : end-of-month Lower-48 working gas in underground storage, BCF
wti_price_usd_bbl           : monthly average WTI crude spot price, $/BBL
brent_price_usd_bbl         : monthly average Brent crude spot price, $/BBL
avg_precipitation           : average monthly precipitation across 8 NOAA stations, inches
avg_wind_speed              : average monthly wind speed across 8 NOAA stations, mph

Usage:
    python scripts/merge_data.py
    python scripts/merge_data.py --start 2020-01 --end 2024-12
"""

import argparse
from pathlib import Path

import pandas as pd

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
MERGED_DIR = Path(__file__).parent.parent / "data" / "merged"
MERGED_DIR.mkdir(parents=True, exist_ok=True)

# Non-overlapping fuel type IDs to extract from electricity_generation.csv
FUEL_MAP = {
    "ALL": "gen_total_MWh",
    "NG":  "gen_natural_gas_MWh",
    "COW": "gen_coal_MWh",
    "NUC": "gen_nuclear_MWh",
    "WND": "gen_wind_MWh",
    "TSN": "gen_solar_MWh",
    "HYC": "gen_hydro_MWh",
    "PET": "gen_petroleum_MWh",
}


def load_electricity_generation(start: str, end: str) -> pd.DataFrame:
    """
    Sum national generation by fuel type per month.

    Only 2-letter state location codes are included to avoid double-counting
    with the numeric region aggregates (e.g. "90" = Pacific).
    """
    print("Processing electricity_generation.csv ...")
    df = pd.read_csv(RAW_DIR / "electricity_generation.csv", low_memory=False)
    df["period"] = pd.to_datetime(df["period"])

    # Keep state-level rows only (2-letter codes)
    df = df[df["location"].str.len() == 2]
    # Keep only the fuel types we care about
    df = df[df["fueltypeid"].isin(FUEL_MAP)]
    # Apply date filter
    df = df[(df["period"] >= start) & (df["period"] <= end)]

    df["generation"] = pd.to_numeric(df["generation"], errors="coerce")
    df["year_month"] = df["period"].dt.to_period("M").astype(str)

    # Sum across all states for each fuel type + month
    agg = (
        df.groupby(["year_month", "fueltypeid"])["generation"]
        .sum()
        .reset_index()
    )
    # Pivot fuel types into columns
    wide = agg.pivot(index="year_month", columns="fueltypeid", values="generation")
    wide = wide.rename(columns=FUEL_MAP)
    wide.columns.name = None
    return wide.reset_index()


def load_natural_gas_prices(start: str, end: str) -> pd.DataFrame:
    """National average natural-gas citygate price per month."""
    print("Processing natural_gas_prices.csv ...")
    df = pd.read_csv(RAW_DIR / "natural_gas_prices.csv", low_memory=False)
    df["period"] = pd.to_datetime(df["period"])
    df = df[(df["period"] >= start) & (df["period"] <= end)]

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["year_month"] = df["period"].dt.to_period("M").astype(str)

    agg = (
        df.groupby("year_month")["value"]
        .mean()
        .reset_index()
        .rename(columns={"value": "natgas_price_usd_mcf"})
    )
    return agg


def load_natural_gas_storage(start: str, end: str) -> pd.DataFrame:
    """
    End-of-month Lower-48 working gas in underground storage.

    Filters to duoarea == R48 and process == SWO (total working gas).
    The last weekly reading within each calendar month is used as the
    end-of-month snapshot.
    """
    print("Processing natural_gas_storage.csv ...")
    df = pd.read_csv(RAW_DIR / "natural_gas_storage.csv", low_memory=False)
    df["period"] = pd.to_datetime(df["period"])
    df = df[(df["period"] >= start) & (df["period"] <= end)]

    # Lower-48 total working gas only
    df = df[(df["duoarea"] == "R48") & (df["process"] == "SWO")]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["year_month"] = df["period"].dt.to_period("M").astype(str)

    # Last reading of the month
    agg = (
        df.sort_values("period")
        .groupby("year_month")["value"]
        .last()
        .reset_index()
        .rename(columns={"value": "natgas_storage_lower48_bcf"})
    )
    return agg


def load_petroleum_prices(start: str, end: str) -> pd.DataFrame:
    """Monthly average WTI and Brent crude prices."""
    print("Processing petroleum_prices.csv ...")
    df = pd.read_csv(RAW_DIR / "petroleum_prices.csv", low_memory=False)
    df["period"] = pd.to_datetime(df["period"])
    df = df[(df["period"] >= start) & (df["period"] <= end)]

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["year_month"] = df["period"].dt.to_period("M").astype(str)

    # Monthly mean per crude type
    agg = (
        df.groupby(["year_month", "series"])["value"]
        .mean()
        .reset_index()
    )
    wide = agg.pivot(index="year_month", columns="series", values="value")
    wide.columns.name = None
    wide = wide.rename(columns={"RWTC": "wti_price_usd_bbl", "RBRTE": "brent_price_usd_bbl"})
    return wide.reset_index()


def load_noaa_precipitation(start: str, end: str) -> pd.DataFrame:
    """Average monthly precipitation across all NOAA stations."""
    print("Processing noaa_monthly_precipitation.csv ...")
    df = pd.read_csv(RAW_DIR / "noaa_monthly_precipitation.csv", low_memory=False)
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= start) & (df["date"] <= end)]

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["year_month"] = df["date"].dt.to_period("M").astype(str)

    agg = (
        df.groupby("year_month")["value"]
        .mean()
        .reset_index()
        .rename(columns={"value": "avg_precipitation"})
    )
    return agg


def load_noaa_wind_speed(start: str, end: str) -> pd.DataFrame:
    """Average monthly wind speed across all NOAA stations."""
    print("Processing noaa_wind_speed.csv ...")
    df = pd.read_csv(RAW_DIR / "noaa_wind_speed.csv", low_memory=False)
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= start) & (df["date"] <= end)]

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["year_month"] = df["date"].dt.to_period("M").astype(str)

    agg = (
        df.groupby("year_month")["value"]
        .mean()
        .reset_index()
        .rename(columns={"value": "avg_wind_speed"})
    )
    return agg


def merge(start: str, end: str) -> pd.DataFrame:
    """Join all monthly feature tables on year_month."""
    frames = [
        load_electricity_generation(start, end),
        load_natural_gas_prices(start, end),
        load_natural_gas_storage(start, end),
        load_petroleum_prices(start, end),
        load_noaa_precipitation(start, end),
        load_noaa_wind_speed(start, end),
    ]

    merged = frames[0]
    for df in frames[1:]:
        merged = merged.merge(df, on="year_month", how="outer")

    merged = merged.sort_values("year_month").reset_index(drop=True)

    # Reorder columns for readability
    col_order = [
        "year_month",
        "gen_total_MWh",
        "gen_natural_gas_MWh",
        "gen_coal_MWh",
        "gen_nuclear_MWh",
        "gen_wind_MWh",
        "gen_solar_MWh",
        "gen_hydro_MWh",
        "gen_petroleum_MWh",
        "natgas_price_usd_mcf",
        "natgas_storage_lower48_bcf",
        "wti_price_usd_bbl",
        "brent_price_usd_bbl",
        "avg_precipitation",
        "avg_wind_speed",
    ]
    # Keep any extra columns that were not anticipated
    extra = [c for c in merged.columns if c not in col_order]
    merged = merged[col_order + extra]

    return merged


def parse_args():
    parser = argparse.ArgumentParser(description="Merge raw energy + weather CSVs into a monthly feature table")
    parser.add_argument("--start", default="2018-01-01", help="Start date (YYYY-MM-DD or YYYY-MM)")
    parser.add_argument("--end",   default="2024-12-31", help="End date (YYYY-MM-DD or YYYY-MM)")
    return parser.parse_args()


def main():
    args = parse_args()
    # Normalise to full dates
    start = args.start if len(args.start) == 10 else f"{args.start}-01"
    end   = args.end   if len(args.end)   == 10 else f"{args.end}-28"

    print(f"Merging data from {start} to {end}\n")
    df = merge(start, end)

    out = MERGED_DIR / "energy_monthly.csv"
    df.to_csv(out, index=False)

    print(f"\nMerged {len(df)} monthly rows x {len(df.columns)} columns -> {out}")
    print(f"Date range: {df['year_month'].min()} to {df['year_month'].max()}")
    missing = df.isnull().sum()
    missing = missing[missing > 0]
    if not missing.empty:
        print("\nMissing values per column:")
        print(missing.to_string())


if __name__ == "__main__":
    main()
