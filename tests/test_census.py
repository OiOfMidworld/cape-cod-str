"""
tests/test_census.py

Sprint 1 tests for Census API ingestion.
Run with: pytest tests/test_census.py -v
"""

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from ingestion.census_api import (
    fetch_acs_for_year,
    BARNSTABLE_TOWNS,
    CENSUS_VARIABLES,
    CENSUS_NULL_VALUES,
)

ALL_15_TOWNS = set(BARNSTABLE_TOWNS.keys())

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_mock_census_response(year: int = 2022) -> list:
    """Build a minimal mock Census API response for all 15 towns."""
    headers = ["NAME"] + list(CENSUS_VARIABLES.keys()) + ["state", "county", "county subdivision"]
    rows = []
    for town, fips in BARNSTABLE_TOWNS.items():
        row = [
            f"{town} town, Barnstable County, Massachusetts",
            "5000",   # B25001_001E total_housing_units
            "3000",   # B25002_002E occupied_units
            "2000",   # B25002_003E vacant_units
            "1500",   # B25004_006E seasonal_units
            "2000",   # B25003_002E owner_occupied
            "1000",   # B25003_003E renter_occupied
            "75000",  # B19013_001E median_household_income
            "450000", # B25077_001E median_home_value
            "12000",  # B01003_001E population
            "25",     # state
            "001",    # county
            fips,     # county subdivision
        ]
        rows.append(row)
    return [headers] + rows


# ---------------------------------------------------------------------------
# Unit tests — no network calls, uses mocked responses
# ---------------------------------------------------------------------------

class TestFetchAcsForYear:

    @patch("ingestion.census_api.requests.get")
    def test_returns_15_towns(self, mock_get):
        """Should return exactly one row per Barnstable County town."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = make_mock_census_response(2022)
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        df = fetch_acs_for_year(2022)
        assert len(df) == 15, f"Expected 15 towns, got {len(df)}"

    @patch("ingestion.census_api.requests.get")
    def test_all_15_town_names_present(self, mock_get):
        """Every Barnstable town should appear exactly once."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = make_mock_census_response(2022)
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        df = fetch_acs_for_year(2022)
        returned_towns = set(df["town"].tolist())
        missing = ALL_15_TOWNS - returned_towns
        assert not missing, f"Missing towns: {missing}"

    @patch("ingestion.census_api.requests.get")
    def test_numeric_columns_are_numeric(self, mock_get):
        """All value columns should be numeric (float or int), not strings."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = make_mock_census_response(2022)
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        df = fetch_acs_for_year(2022)
        numeric_cols = list(CENSUS_VARIABLES.values())
        for col in numeric_cols:
            assert pd.api.types.is_numeric_dtype(df[col]), \
                f"Column {col} should be numeric, got {df[col].dtype}"

    @patch("ingestion.census_api.requests.get")
    def test_no_nulls_on_key_columns(self, mock_get):
        """town, survey_year, total_housing_units must never be null."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = make_mock_census_response(2022)
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        df = fetch_acs_for_year(2022)
        for col in ["town", "survey_year", "total_housing_units"]:
            null_count = df[col].isna().sum()
            assert null_count == 0, f"Column {col} has {null_count} nulls"

    @patch("ingestion.census_api.requests.get")
    def test_survey_year_matches_request(self, mock_get):
        """survey_year column should match the year requested."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = make_mock_census_response(2021)
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        df = fetch_acs_for_year(2021)
        assert (df["survey_year"] == 2021).all(), "survey_year should all be 2021"

    @patch("ingestion.census_api.requests.get")
    def test_census_sentinel_values_become_null(self, mock_get):
        """Census sentinel values (-666666666 etc.) should be replaced with NaN."""
        headers = ["NAME"] + list(CENSUS_VARIABLES.keys()) + ["state", "county", "county subdivision"]
        # One row with a sentinel value in median_household_income
        row = [
            "Truro town, Barnstable County, Massachusetts",
            "1000",
            "600",
            "400",
            "300",
            "400",
            "200",
            "-666666666",  # suppressed income data — common in small towns
            "500000",
            "2000",
            "25",
            "001",
            BARNSTABLE_TOWNS["Truro"],
        ]
        mock_resp = MagicMock()
        mock_resp.json.return_value = [headers, row]
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        df = fetch_acs_for_year(2022)
        truro_income = df[df["town"] == "Truro"]["median_household_income"].iloc[0]
        assert pd.isna(truro_income), \
            f"Sentinel value should be NaN, got {truro_income}"

    @patch("ingestion.census_api.requests.get")
    def test_schema_columns_match_expected(self, mock_get):
        """Output DataFrame should have the exact columns matching our DB schema."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = make_mock_census_response(2022)
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        df = fetch_acs_for_year(2022)
        expected_cols = {
            "ingested_at", "survey_year", "geo_id", "town", "county",
            "total_housing_units", "occupied_units", "vacant_units",
            "seasonal_units", "owner_occupied", "renter_occupied",
            "median_household_income", "median_home_value", "population",
        }
        actual_cols = set(df.columns)
        assert actual_cols == expected_cols, \
            f"Column mismatch.\nMissing: {expected_cols - actual_cols}\nExtra: {actual_cols - expected_cols}"

    @patch("ingestion.census_api.requests.get")
    def test_empty_response_returns_empty_df(self, mock_get):
        """If Census returns no rows, should return empty DataFrame gracefully."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = [["NAME", "B25001_001E", "state", "county", "county subdivision"]]
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        df = fetch_acs_for_year(2022)
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    @patch("ingestion.census_api.requests.get")
    def test_seasonal_units_less_than_vacant(self, mock_get):
        """Seasonal units is a subset of vacant units — basic sanity check."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = make_mock_census_response(2022)
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        df = fetch_acs_for_year(2022)
        invalid = df[df["seasonal_units"] > df["vacant_units"]]
        assert invalid.empty, \
            f"seasonal_units exceeds vacant_units for: {invalid['town'].tolist()}"


# ---------------------------------------------------------------------------
# Integration-style sanity checks (data shape / business logic)
# ---------------------------------------------------------------------------

class TestCensusDataSanity:

    @patch("ingestion.census_api.requests.get")
    def test_occupied_plus_vacant_equals_total(self, mock_get):
        """occupied + vacant should equal total_housing_units."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = make_mock_census_response(2022)
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        df = fetch_acs_for_year(2022)
        df["check"] = df["occupied_units"] + df["vacant_units"]
        mismatches = df[df["check"] != df["total_housing_units"]]
        assert mismatches.empty, \
            f"occupied + vacant != total for: {mismatches['town'].tolist()}"

    @patch("ingestion.census_api.requests.get")
    def test_owner_plus_renter_equals_occupied(self, mock_get):
        """owner_occupied + renter_occupied should equal occupied_units."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = make_mock_census_response(2022)
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        df = fetch_acs_for_year(2022)
        df["check"] = df["owner_occupied"] + df["renter_occupied"]
        mismatches = df[df["check"] != df["occupied_units"]]
        assert mismatches.empty, \
            f"owner + renter != occupied for: {mismatches['town'].tolist()}"
