# Cape Cod Short-Term Rental Tracker

An ETL pipeline that tracks short-term rental (STR) growth across all 15 Barnstable County towns

---


## Project Structure

```
cape-cod-str-tracker/
│
├── README.md
├── .env                        # API keys, DB path (gitignored)
├── .env.example
├── requirements.txt
├── pyproject.toml
│
├── ingestion/                  # Extract layer — one file per source
│   ├── __init__.py
│   ├── dor_str_registry.py     # MA DOR short-term rental registry
│   ├── massgis_parcels.py      # MassGIS parcel shapefile ingestion
│   ├── census_api.py           # Census Bureau ACS API
│   ├── inside_airbnb.py        # InsideAirbnb CSV download
│   └── datacapecod.py          # datacapecod.org CSV datasets
│
├── transform/                  # Transform layer
│   ├── __init__.py
│   ├── address_matcher.py      # Fuzzy address matching (rapidfuzz)
│   ├── town_normalizer.py      # Standardize town names across sources
│   └── metrics.py              # Derived metrics (STR %, YoY change, etc.)
│
├── load/                       # Load layer
│   ├── __init__.py
│   └── duckdb_loader.py        # Upserts and snapshot management
│
├── dbt/                        # dbt transformation models
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/            # stg_ models: clean raw tables 1:1
│   │   │   ├── stg_str_registry.sql
│   │   │   ├── stg_parcels.sql
│   │   │   ├── stg_census.sql
│   │   │   └── stg_airbnb.sql
│   │   ├── intermediate/       # int_ models: joins and enrichment
│   │   │   ├── int_str_parcel_matched.sql
│   │   │   └── int_town_housing_base.sql
│   │   └── marts/              # Final output tables
│   │       ├── mart_town_str_summary.sql
│   │       ├── mart_property_str_history.sql
│   │       └── mart_affordability_correlation.sql
│   └── tests/
│       ├── assert_town_names_valid.sql
│       └── assert_str_pct_between_0_and_1.sql
│
├── orchestration/
│   └── flows.py                # Prefect flow definitions
│
├── dashboard/
│   └── app.py                  # Streamlit dashboard
│
├── data/
│   ├── raw/                    # Raw downloads, gitignored
│   ├── staging/                # Intermediate files, gitignored
│   └── reference/
│       └── town_name_lookup.csv  # Canonical town name mapping
│
└── tests/
    ├── test_address_matcher.py
    ├── test_town_normalizer.py
    └── test_metrics.py
```

---

## Data Sources


---

## Database Schema

See `schema.sql` for full DDL. Key tables:

- **`raw.*`** — Unmodified source data, append-only with ingestion timestamp
- **`staging.*`** — Cleaned, typed, deduplicated versions of raw tables
- **`mart.*`** — Final analytical tables consumed by the dashboard

### Core Mart Tables

**`mart_town_str_summary`** — One row per town per snapshot month
| Column | Type | Description |
|---|---|---|
| `town` | VARCHAR | Canonical town name |
| `snapshot_month` | DATE | First of month for this snapshot |
| `total_housing_units` | INTEGER | Total units from Census |
| `year_round_units` | INTEGER | Year-round occupied units |
| `str_registered_count` | INTEGER | Count from DOR registry |
| `airbnb_active_count` | INTEGER | Active Airbnb listings |
| `str_pct_of_total` | FLOAT | STR registered / total units |
| `str_pct_yoy_change` | FLOAT | YoY change in str_pct |
| `median_home_price` | FLOAT | Median sale price |
| `median_income` | FLOAT | ACS median household income |
| `affordability_ratio` | FLOAT | Median price / median income |

**`mart_property_str_history`** — SCD Type 2 history of each property's STR status
| Column | Type | Description |
|---|---|---|
| `property_id` | VARCHAR | MassGIS parcel ID |
| `address` | VARCHAR | Normalized address |
| `town` | VARCHAR | Town |
| `owner_name` | VARCHAR | From parcel data |
| `is_owner_occupied` | BOOLEAN | Mailing addr matches property addr |
| `is_str_registered` | BOOLEAN | Found in DOR registry |
| `is_airbnb_listed` | BOOLEAN | Found in Inside Airbnb |
| `first_seen_as_str` | DATE | When first appeared in registry |
| `last_seen_as_str` | DATE | Last date found in registry |
| `assessed_value` | FLOAT | From parcel data |
| `zoning` | VARCHAR | Zoning classification |

---

## Key Metrics Produced

- **STR % of housing stock** per town, updated monthly
- **Year-over-year conversion rate** — how fast each town is changing
- **Owner-occupied vs investor** STR split — absentee landlords vs local owners
- **STR density by street** — neighborhood-level concentration
- **Affordability correlation** — STR % vs median home price over time

---


## Tech Stack
