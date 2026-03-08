"""
ingestion/census_api.py

Pulls ACS 5-year estimates from the Census Bureau API for all 15 Barnstable County towns.

Census variables we care about:
    B25001_001E  — Total housing units
    B25002_002E  — Occupied housing units
    B25002_003E  — Vacant housing units
    B25004_006E  — Vacant: for seasonal/recreational/occasional use
    B25003_002E  — Owner-occupied units
    B25003_003E  — Renter-occupied units
    B19013_001E  — Median household income
    B25077_001E  — Median value of owner-occupied housing units
    B01003_001E  — Total population

Docs: https://api.census.gov/data/2022/acs/acs5/variables.json
FIPS: Massachusetts = 25, Barnstable County = 001
"""

import logging
import os
from datetime import date

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

CENSUS_API_BASE = "https://api.census.gov/data"
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "")

# All 15 Barnstable County towns with their FIPS place codes
BARNSTABLE_TOWNS = {
    "Barnstable":    "03690",
    "Bourne":        "07175",
    "Brewster":      "07980",
    "Chatham":       "12995",
    "Dennis":        "16775",
    "Eastham":       "19295",
    "Falmouth":      "23105",
    "Harwich":       "29020",
    "Mashpee":       "39100",
    "Orleans":       "51440",
    "Provincetown":  "55500",
    "Sandwich":      "59735",
    "Truro":         "70605",
    "Wellfleet":     "74385",
    "Yarmouth":      "82525",
}

CENSUS_VARIABLES = {
    "B25001_001E": "total_housing_units",
    "B25002_002E": "occupied_units",
    "B25002_003E": "vacant_units",
    "B25004_006E": "seasonal_units",
    "B25003_002E": "owner_occupied",
    "B25003_003E": "renter_occupied",
    "B19013_001E": "median_household_income",
    "B25077_001E": "median_home_value",
    "B01003_001E": "population",
}

# Sentinel value Census uses for missing/suppressed data
CENSUS_NULL_VALUES = {-666666666, -999999999, -888888888}


def fetch_acs_for_year(year: int) -> pd.DataFrame:
    """
    Fetch ACS 5-year estimates for all Barnstable County towns for a given year.

    Args:
        year: Survey year (e.g. 2022 = 2018-2022 5-year ACS)

    Returns:
        DataFrame with one row per town
    """
    variables = ",".join(CENSUS_VARIABLES.keys())
    url = f"{CENSUS_API_BASE}/{year}/acs/acs5"

    params = {
        "get": f"NAME,{variables}",
        "for": "county subdivision:*",
        "in": "state:25 county:001",  # Massachusetts
        "key": CENSUS_API_KEY,
    }

    logger.info(f"Fetching Census ACS {year} for Barnstable County...")

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Census API request failed for year {year}: {e}")
        raise

    data = resp.json()
    headers = data[0]
    rows = data[1:]

    if not rows:
        logger.warning(f"No data returned from Census API for year {year}")
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=headers)

    # Filter to only our 15 towns using FIPS place codes
    df = df[df["county subdivision"].isin(BARNSTABLE_TOWNS.values())].copy()

    # Build reverse lookup: FIPS code -> town name
    fips_to_town = {v: k for k, v in BARNSTABLE_TOWNS.items()}
    df["town"] = df["county subdivision"].map(fips_to_town)

    # Rename Census variable codes to readable column names
    df = df.rename(columns=CENSUS_VARIABLES)

    # Cast numeric columns — Census returns everything as strings
    numeric_cols = list(CENSUS_VARIABLES.values())
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        # Replace Census sentinel null values with NaN
        df[col] = df[col].apply(
            lambda x: None if x in CENSUS_NULL_VALUES else x
        )

    # Add metadata columns
    df["survey_year"] = year
    df["county"] = "Barnstable"
    df["ingested_at"] = pd.Timestamp.now()

    # Select and order final columns to match raw.census_acs schema
    final_cols = [
        "ingested_at",
        "survey_year",
        "county subdivision",
        "town",
        "county",
        "total_housing_units",
        "occupied_units",
        "vacant_units",
        "seasonal_units",
        "owner_occupied",
        "renter_occupied",
        "median_household_income",
        "median_home_value",
        "population",
    ]
    df = df[final_cols].rename(columns={"county subdivision": "geo_id"})

    logger.info(f"Fetched {len(df)} towns for survey year {year}")
    return df


def fetch_all_available_years(start_year: int = 2015) -> pd.DataFrame:
    """
    Fetch ACS data for all available years from start_year to most recent.
    ACS 5-year estimates are published ~1 year after the survey year.

    Args:
        start_year: First year to fetch (default 2015)

    Returns:
        Combined DataFrame with all years stacked
    """
    current_year = date.today().year
    # ACS lags by ~1 year; 2023 data became available in late 2024
    latest_available = current_year - 1

    all_dfs = []
    for year in range(start_year, latest_available + 1):
        try:
            df = fetch_acs_for_year(year)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            logger.warning(f"Skipping year {year}: {e}")
            continue

    if not all_dfs:
        logger.error("No Census data fetched for any year")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    logger.info(
        f"Fetched {len(combined)} total rows across "
        f"{combined['survey_year'].nunique()} years"
    )
    return combined


def run(years: list = None) -> pd.DataFrame:
    """
    Main entry point. Fetches Census data and loads into DuckDB.

    Args:
        years: Optional list of specific years. If None, fetches all available.

    Returns:
        DataFrame of all rows loaded
    """
    from load.loader import load_dataframe, upsert_dataframe

    if years:
        dfs = []
        for year in years:
            df = fetch_acs_for_year(year)
            if not df.empty:
                dfs.append(df)
        combined = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    else:
        combined = fetch_all_available_years()

    if combined.empty:
        logger.warning("No data to load")
        return combined

    # Load raw — always append, we keep full history
    load_dataframe(combined, "raw.census_acs", mode="append")

    # Load staging — upsert on (survey_year, town) so reruns are safe
    staging_cols = [c for c in combined.columns if c not in ["geo_id","ingested_at", "county"]]
    staging_df = combined[staging_cols].copy()
    upsert_dataframe(staging_df, "staging.stg_census_acs", primary_keys=["survey_year", "town"])

    return combined


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    result = run()
    if not result.empty:
        print(f"\nLoaded {len(result)} rows")
        print(result[["survey_year", "town", "total_housing_units", "seasonal_units"]].to_string(index=False))
