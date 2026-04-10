"""
Fetch weather data from the NOAA Climate Data Online (CDO) API v2.

Usage:
    python fetch_noaa_data.py
    python fetch_noaa_data.py --start 2018-01-01 --end 2024-12-31
    python fetch_noaa_data.py --series daily_temps heating_cooling_days
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# Load API key from backend/.env
ENV_PATH = Path(__file__).parent.parent / "backend" / ".env"
load_dotenv(ENV_PATH)
API_KEY = os.getenv("NOAA_API_KEY")
if not API_KEY:
    sys.exit("NOAA_API_KEY not found. Check backend/.env")

BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {"token": API_KEY}
PAGE_LIMIT = 1000
DAILY_MAX_DAYS = 365

# Major U.S. climate stations covering key energy demand regions
STATIONS = {
    "NYC":     "GHCND:USW00094728",   # Central Park, NY
    "Chicago": "GHCND:USW00094846",   # O'Hare, IL
    "Houston": "GHCND:USW00012960",   # Houston Hobby, TX
    "LA":      "GHCND:USW00023174",   # Los Angeles Intl, CA
    "Atlanta": "GHCND:USW00013874",   # Atlanta Hartsfield, GA
    "Denver":  "GHCND:USW00094038",   # Denver Intl, CO
    "Seattle": "GHCND:USW00024233",   # Seattle-Tacoma Intl, WA
    "Miami":   "GHCND:USW00012839",   # Miami Intl, FL
}


def noaa_get(endpoint: str, params: dict) -> list:
    """GET from NOAA CDO API with automatic pagination (max 1000/page)."""
    params = {**params, "limit": PAGE_LIMIT}
    url = f"{BASE_URL}/{endpoint}"
    all_results = []
    offset = 1  # NOAA uses 1-based offset

    while True:
        params["offset"] = offset
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)

        if resp.status_code == 429:
            print("    Rate limited — waiting 30s...")
            time.sleep(30)
            continue

        resp.raise_for_status()
        body = resp.json()

        results = body.get("results", [])
        all_results.extend(results)

        meta = body.get("metadata", {}).get("resultset", {})
        count = int(meta.get("count", 0))
        limit = int(meta.get("limit", PAGE_LIMIT))
        offset_cur = int(meta.get("offset", offset))

        if offset_cur + limit - 1 >= count or not results:
            break

        offset = offset_cur + limit
        time.sleep(0.2)  # stay within 5 req/s rate limit

    return all_results


def date_chunks(start: str, end: str, max_days: int) -> list[tuple[str, str]]:
    """Split date ranges so NOAA CDO daily requests stay under API limits."""
    start_dt = datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end, "%Y-%m-%d").date()
    chunks = []
    current = start_dt

    while current <= end_dt:
        chunk_end = min(current + timedelta(days=max_days - 1), end_dt)
        chunks.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end + timedelta(days=1)

    return chunks


def to_df(rows: list, date_col: str = "date") -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values(date_col).reset_index(drop=True)
    return df


def _fetch_by_station(
    dataset: str,
    datatypes: str,
    start: str,
    end: str,
    *,
    max_days: int | None = None,
) -> list:
    """Shared helper: fetch one or more datatypes across all stations."""
    all_rows = []
    chunks = date_chunks(start, end, max_days) if max_days else [(start, end)]

    for city, station_id in STATIONS.items():
        print(f"    {city}...")
        for chunk_start, chunk_end in chunks:
            if len(chunks) > 1:
                print(f"      {chunk_start} to {chunk_end}...")
            try:
                rows = noaa_get(
                    "data",
                    {
                        "datasetid": dataset,
                        "stationid": station_id,
                        "datatypeid": datatypes.split(","),  # repeated params, not comma-encoded
                        "startdate": chunk_start,
                        "enddate": chunk_end,
                        "units": "standard",
                    },
                )
                for r in rows:
                    r["city"] = city
                all_rows.extend(rows)
            except requests.HTTPError as e:
                detail = e.response.text[:200].replace("\n", " ")
                print(f"      [WARN] HTTP {e.response.status_code}: {detail}")
        time.sleep(0.2)  # avoid bursting rate limit between stations
    return all_rows



# Individual fetchers
def fetch_daily_temps(start: str, end: str) -> pd.DataFrame:
    """Daily max/min/avg temperature from key U.S. stations."""
    print("  Fetching daily temperatures (TMAX, TMIN, TAVG)...")
    rows = _fetch_by_station("GHCND", "TMAX,TMIN,TAVG", start, end, max_days=DAILY_MAX_DAYS)
    df = to_df(rows)
    if not df.empty and "datatype" in df.columns:
        df = df.pivot_table(
            index=["date", "city", "station"],
            columns="datatype",
            values="value",
            aggfunc="first",
        ).reset_index()
        df.columns.name = None
        if {"TMAX", "TMIN"}.issubset(df.columns):
            if "TAVG" not in df.columns:
                df["TAVG"] = (df["TMAX"] + df["TMIN"]) / 2
            else:
                df["TAVG"] = df["TAVG"].fillna((df["TMAX"] + df["TMIN"]) / 2)
    return df


def fetch_heating_cooling_days(start: str, end: str) -> pd.DataFrame:
    """Monthly heating degree days (HDD) and cooling degree days (CDD)."""
    print("  Fetching heating/cooling degree days (HDD, CDD)...")
    rows = _fetch_by_station("GSOM", "HTDD,CLDD", start, end)
    df = to_df(rows)
    if not df.empty and "datatype" in df.columns:
        df = df.pivot_table(
            index=["date", "city", "station"],
            columns="datatype",
            values="value",
            aggfunc="first",
        ).reset_index()
        df.columns.name = None
        df = df.rename(columns={"HTDD": "HDD", "CLDD": "CDD"})
    return df


def fetch_monthly_precipitation(start: str, end: str) -> pd.DataFrame:
    """Monthly total precipitation by station."""
    print("  Fetching monthly precipitation (PRCP)...")
    rows = _fetch_by_station("GSOM", "PRCP", start, end)
    return to_df(rows)


def fetch_wind_speed(start: str, end: str) -> pd.DataFrame:
    """Monthly average wind speed by station (relevant for wind energy output)."""
    print("  Fetching monthly wind speed (AWND)...")
    rows = _fetch_by_station("GSOM", "AWND", start, end)
    return to_df(rows)



# Registry
SERIES = {
    "daily_temps":           fetch_daily_temps,
    "heating_cooling_days":  fetch_heating_cooling_days,
    "monthly_precipitation": fetch_monthly_precipitation,
    "wind_speed":            fetch_wind_speed,
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Fetch NOAA weather data")
    parser.add_argument("--start", default="2018-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end",   default="2024-12-31", help="End date (YYYY-MM-DD)")
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
    print(f"Fetching NOAA data from {args.start} to {args.end}")
    print(f"Stations: {', '.join(STATIONS.keys())}")
    print(f"Series: {', '.join(args.series)}\n")

    results = {}
    errors = []

    for name in args.series:
        try:
            df = SERIES[name](args.start, args.end)
            out = DATA_DIR / f"noaa_{name}.csv"
            df.to_csv(out, index=False)
            print(f"  Saved {len(df):,} rows -> {out}\n")
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
