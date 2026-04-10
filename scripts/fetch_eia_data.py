"""
Fetch energy data from the EIA API v2.
Usage:
    python fetch_eia_data.py
    python fetch_eia_data.py --start 2020-01-01 --end 2024-12-31
    python fetch_eia_data.py --start 2020-01 --end 2024-12
    python fetch_eia_data.py --series electricity natural_gas
"""

import argparse
import calendar
from datetime import datetime
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# Load API key from backend/.env
ENV_PATH = Path(__file__).parent.parent / "backend" / ".env"
load_dotenv(ENV_PATH)
API_KEY = os.getenv("EIA_API_KEY")
if not API_KEY:
    sys.exit("EIA_API_KEY not found. Check backend/.env")

BASE_URL = "https://api.eia.gov/v2"
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_START_DATE = "2018-01-01"
DEFAULT_END_DATE = "2024-12-31"


def normalize_date(value: str, *, is_end: bool = False) -> str:
    """Accept YYYY-MM or YYYY-MM-DD and return a full YYYY-MM-DD date."""
    if len(value) == 7:
        year, month = map(int, value.split("-"))
        day = calendar.monthrange(year, month)[1] if is_end else 1
        return f"{year:04d}-{month:02d}-{day:02d}"

    datetime.strptime(value, "%Y-%m-%d")
    return value


def eia_month(value: str) -> str:
    return value[:7]


def eia_year(value: str) -> str:
    return value[:4]


def eia_hour_start(value: str) -> str:
    return f"{value}T00"


def eia_hour_end(value: str) -> str:
    return f"{value}T23"


def eia_get(route: str, params: dict) -> dict:
    """GET request to EIA API v2 with automatic pagination."""
    params["api_key"] = API_KEY
    params.setdefault("length", 5000)
    url = f"{BASE_URL}/{route}/data/"
    all_data = []
    offset = 0
    page = 1

    while True:
        params["offset"] = offset
        print(f"    Requesting page {page} at offset {offset:,}...")
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        rows = body.get("response", {}).get("data", [])
        all_data.extend(rows)
        total = int(body.get("response", {}).get("total", 0))
        print(f"    Received {len(rows):,} rows ({len(all_data):,}/{total:,} total)")
        offset += len(rows)
        if offset >= total or not rows:
            break
        page += 1
        time.sleep(0.2)  # stay within rate limits

    return all_data


def to_df(rows: list, date_col: str = "period") -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values(date_col).reset_index(drop=True)
    return df


# Individual fetchers

def fetch_electricity_demand(start: str, end: str) -> pd.DataFrame:
    """Hourly U.S. electricity demand by region (EIA-930)."""
    print("  Fetching electricity demand (EIA-930)...")
    rows = eia_get(
        "electricity/rto/region-data",
        {
            "frequency": "hourly",
            "data[0]": "value",
            "facets[type][]": "D",
            "start": eia_hour_start(start),
            "end": eia_hour_end(end),
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
        },
    )
    return to_df(rows)


def fetch_electricity_generation(start: str, end: str) -> pd.DataFrame:
    """Monthly net electricity generation by fuel type."""
    print("  Fetching electricity generation by fuel type...")
    rows = eia_get(
        "electricity/electric-power-operational-data",
        {
            "frequency": "monthly",
            "data[0]": "generation",
            "facets[sectorid][]": "99",
            "start": eia_month(start),
            "end": eia_month(end),
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
        },
    )
    return to_df(rows)


def fetch_natural_gas_prices(start: str, end: str) -> pd.DataFrame:
    """Monthly natural gas citygate prices by state."""
    print("  Fetching natural gas prices...")
    rows = eia_get(
        "natural-gas/pri/sum",
        {
            "frequency": "monthly",
            "data[0]": "value",
            "facets[process][]": "PCS",     # Citygate
            "start": eia_month(start),
            "end": eia_month(end),
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
        },
    )
    return to_df(rows)


def fetch_petroleum_prices(start: str, end: str) -> pd.DataFrame:
    """Weekly WTI and Brent crude oil spot prices."""
    print("  Fetching petroleum prices (WTI/Brent)...")
    rows = eia_get(
        "petroleum/pri/spt",
        {
            "frequency": "weekly",
            "data[0]": "value",
            "facets[series][]": ["RWTC", "RBRTE"],  # WTI and Brent
            "start": start,
            "end": end,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
        },
    )
    return to_df(rows)


def fetch_natural_gas_storage(start: str, end: str) -> pd.DataFrame:
    """Weekly U.S. natural gas storage (working gas in underground storage)."""
    print("  Fetching natural gas storage...")
    rows = eia_get(
        "natural-gas/stor/wkly",
        {
            "frequency": "weekly",
            "data[0]": "value",
            "start": start,
            "end": end,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
        },
    )
    return to_df(rows)


def fetch_coal_production(start: str, end: str) -> pd.DataFrame:
    """Annual U.S. coal production by mining method."""
    print("  Fetching coal production...")
    rows = eia_get(
        "coal/production/mines",  # NOTE: verify route against EIA API explorer; "coal/production/annual" is not a valid v2 path
        {
            "frequency": "annual",
            "data[0]": "production",
            "start": eia_year(start),       # annual uses YYYY
            "end": eia_year(end),
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
        },
    )
    return to_df(rows)


# Registry

SERIES = {
    "electricity_demand":     fetch_electricity_demand,
    "electricity_generation": fetch_electricity_generation,
    "natural_gas_prices":     fetch_natural_gas_prices,
    "petroleum_prices":       fetch_petroleum_prices,
    "natural_gas_storage":    fetch_natural_gas_storage,
    "coal_production":        fetch_coal_production,
}



def parse_args():
    parser = argparse.ArgumentParser(description="Fetch EIA energy data")
    parser.add_argument("--start", default=DEFAULT_START_DATE, help="Start date (YYYY-MM-DD or YYYY-MM)")
    parser.add_argument("--end",   default=DEFAULT_END_DATE, help="End date (YYYY-MM-DD or YYYY-MM)")
    parser.add_argument(
        "--series",
        nargs="+",
        choices=list(SERIES.keys()),
        default=list(SERIES.keys()),
        help="Which series to fetch (default: all)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    start = normalize_date(args.start)
    end = normalize_date(args.end, is_end=True)

    print(f"Fetching EIA data from {start} to {end}")
    print(f"Series: {', '.join(args.series)}\n")

    results = {}
    errors = []

    for name in args.series:
        try:
            df = SERIES[name](start, end)
            out = DATA_DIR / f"{name}.csv"
            df.to_csv(out, index=False)
            print(f"  Saved {len(df):,} rows -> {out.relative_to(Path.cwd()) if out.is_relative_to(Path.cwd()) else out}\n")
            results[name] = df
        except requests.HTTPError as e:
            print(f"  [WARN] {name}: HTTP {e.response.status_code} - {e.response.text[:200]}\n")
            errors.append(name)
        except Exception as e:
            print(f"  [WARN] {name}: {e}\n")
            errors.append(name)

    print(f"Done. {len(results)} series saved to {DATA_DIR}/")
    if errors:
        print(f"Failed: {', '.join(errors)}")

    return results


if __name__ == "__main__":
    main()
