-- ============================================================
-- Cape Cod STR Tracker — DuckDB Schema
-- ============================================================
-- Layers: raw → staging → mart
-- raw:     append-only, unmodified source data
-- staging: cleaned, typed, deduplicated
-- mart:    final analytical tables for dashboard/queries
-- ============================================================

-- ============================================================
-- RAW LAYER
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;

-- MA DOR Short-Term Rental Registry
-- One row per registered STR per ingestion run
CREATE TABLE IF NOT EXISTS raw.str_registry (
    ingested_at         TIMESTAMP   NOT NULL DEFAULT now(),
    snapshot_date       DATE        NOT NULL,
    street_name         VARCHAR,    -- Street name only (no house number per DOR policy)
    town                VARCHAR,
    county              VARCHAR,
    registration_type   VARCHAR,    -- 'owner-occupied', 'professional', etc.
    source_row_hash     VARCHAR,    -- MD5 of raw row for deduplication
    zip_code            VARCHAR,     
    certificate_id      VARCHAR,
    property_type       VARCHAR,
    city_raw            VARCHAR
);

-- MassGIS Parcel Data
-- One row per parcel in Barnstable County
CREATE TABLE IF NOT EXISTS raw.parcels (
    ingested_at         TIMESTAMP   NOT NULL DEFAULT now(),
    parcel_id           VARCHAR,    -- MassGIS LOC_ID
    address_full        VARCHAR,
    street_number       VARCHAR,
    street_name         VARCHAR,
    town                VARCHAR,
    owner_name          VARCHAR,
    owner_address       VARCHAR,    -- Mailing address (used for owner-occupied flag)
    owner_city          VARCHAR,
    owner_state         VARCHAR,
    land_use_code       VARCHAR,    -- State classification code
    zoning              VARCHAR,
    assessed_land       FLOAT,
    assessed_building   FLOAT,
    assessed_total      FLOAT,
    lot_size_sqft       FLOAT,
    year_built          INTEGER,
    geom_wkt            VARCHAR,    -- WKT geometry (centroid)
    data_year           INTEGER
);

-- Census ACS Data (town level)
CREATE TABLE IF NOT EXISTS raw.census_acs (
    ingested_at             TIMESTAMP   NOT NULL DEFAULT now(),
    survey_year             INTEGER     NOT NULL,
    geo_id                  VARCHAR,
    town                    VARCHAR,
    county                  VARCHAR,
    total_housing_units     INTEGER,
    occupied_units          INTEGER,
    vacant_units            INTEGER,
    seasonal_units          INTEGER,    -- B25004_006E: vacant for seasonal use
    owner_occupied          INTEGER,
    renter_occupied         INTEGER,
    median_household_income FLOAT,
    median_home_value       FLOAT,
    population              INTEGER
);

-- Inside Airbnb Listings
CREATE TABLE IF NOT EXISTS raw.airbnb_listings (
    ingested_at         TIMESTAMP   NOT NULL DEFAULT now(),
    snapshot_date       DATE        NOT NULL,
    listing_id          BIGINT,
    listing_url         VARCHAR,
    name                VARCHAR,
    host_id             BIGINT,
    host_name           VARCHAR,
    neighbourhood       VARCHAR,    -- Airbnb's town/neighborhood label
    latitude            FLOAT,
    longitude           FLOAT,
    room_type           VARCHAR,
    price_usd           FLOAT,
    minimum_nights      INTEGER,
    number_of_reviews   INTEGER,
    last_review         DATE,
    reviews_per_month   FLOAT,
    availability_365    INTEGER,
    is_active           BOOLEAN     -- reviews_per_month > 0 AND availability_365 > 0
);

-- datacapecod.org Housing Data
CREATE TABLE IF NOT EXISTS raw.datacapecod_housing (
    ingested_at         TIMESTAMP   NOT NULL DEFAULT now(),
    data_year           INTEGER,
    town                VARCHAR,
    median_sale_price   FLOAT,
    num_sales           INTEGER,
    pct_change_yoy      FLOAT,
    source_file         VARCHAR
);


-- ============================================================
-- STAGING LAYER
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- Cleaned STR Registry: standardized town names, deduped
CREATE TABLE IF NOT EXISTS staging.stg_str_registry (
    snapshot_date       DATE        NOT NULL,
    street_name         VARCHAR,
    town                VARCHAR,    -- normalized via town_name_lookup
    registration_type   VARCHAR,
    source_row_hash     VARCHAR,
    certificate_id      VARCHAR,
    property_type       VARCHAR,
    zip_code            VARCHAR,
    CONSTRAINT stg_str_registry_pkey PRIMARY KEY (certificate_id, snapshot_date)
);

-- Cleaned Parcel data: one row per parcel, most recent data year
CREATE TABLE IF NOT EXISTS staging.stg_parcels (
    parcel_id           VARCHAR     PRIMARY KEY,
    address_full        VARCHAR,
    street_name         VARCHAR,
    town                VARCHAR,
    owner_name          VARCHAR,
    is_owner_occupied   BOOLEAN,    -- owner_state = 'MA' AND owner_city = town
    land_use_code       VARCHAR,
    is_residential      BOOLEAN,
    zoning              VARCHAR,
    assessed_total      FLOAT,
    year_built          INTEGER,
    latitude            FLOAT,
    longitude           FLOAT,
    data_year           INTEGER
);

-- Cleaned Census data
CREATE TABLE IF NOT EXISTS staging.stg_census_acs (
    survey_year                 INTEGER,
    town                        VARCHAR,
    total_housing_units         INTEGER,
    occupied_units              INTEGER,
    vacant_units                INTEGER,
    seasonal_units              INTEGER,
    owner_occupied              INTEGER,
    renter_occupied             INTEGER,
    median_household_income     FLOAT,
    median_home_value           FLOAT,
    population                  INTEGER,
    PRIMARY KEY (survey_year, town)
);

-- Cleaned Airbnb listings
CREATE TABLE IF NOT EXISTS staging.stg_airbnb_listings (
    snapshot_date       DATE,
    listing_id          BIGINT,
    host_id             BIGINT,
    town                VARCHAR,    -- normalized from neighbourhood
    latitude            FLOAT,
    longitude           FLOAT,
    room_type           VARCHAR,
    price_usd           FLOAT,
    minimum_nights      INTEGER,
    availability_365    INTEGER,
    is_active           BOOLEAN,
    PRIMARY KEY (listing_id, snapshot_date)
);


-- ============================================================
-- MART LAYER
-- ============================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- Primary output: monthly snapshot per town
-- One row per town per month
CREATE TABLE IF NOT EXISTS mart.town_str_summary (
    town                    VARCHAR     NOT NULL,
    snapshot_month          DATE        NOT NULL,   -- First of month
    total_housing_units     INTEGER,
    year_round_units        INTEGER,
    seasonal_units          INTEGER,
    str_registered_count    INTEGER,    -- From DOR registry
    airbnb_active_count     INTEGER,    -- From Inside Airbnb
    str_pct_of_total        FLOAT,      -- str_registered / total_housing_units
    str_pct_yoy_change      FLOAT,      -- Current vs same month prior year
    investor_owned_str_pct  FLOAT,      -- % of STRs NOT owner-occupied
    median_home_price       FLOAT,
    median_household_income FLOAT,
    affordability_ratio     FLOAT,      -- median_home_price / median_income
    PRIMARY KEY (town, snapshot_month)
);

-- SCD Type 2: full history of each property's STR status
-- Tracks when properties entered and exited the registry
CREATE TABLE IF NOT EXISTS mart.property_str_history (
    property_id             VARCHAR     NOT NULL,   -- MassGIS parcel_id
    address_full            VARCHAR,
    street_name             VARCHAR,
    town                    VARCHAR,
    owner_name              VARCHAR,
    is_owner_occupied       BOOLEAN,
    assessed_total          FLOAT,
    zoning                  VARCHAR,
    is_str_registered       BOOLEAN,
    is_airbnb_listed        BOOLEAN,
    first_seen_as_str       DATE,
    last_seen_as_str        DATE,
    str_registration_months INTEGER,    -- Total months ever in registry
    is_currently_str        BOOLEAN,
    last_updated            TIMESTAMP
);

-- Affordability correlation over time
-- For trend analysis: are higher STR towns getting more expensive faster?
CREATE TABLE IF NOT EXISTS mart.affordability_correlation (
    snapshot_month          DATE        NOT NULL,
    town                    VARCHAR     NOT NULL,
    str_pct_of_total        FLOAT,
    median_home_price       FLOAT,
    pct_change_price_yoy    FLOAT,
    median_income           FLOAT,
    affordability_ratio     FLOAT,
    PRIMARY KEY (snapshot_month, town)
);


-- ============================================================
-- REFERENCE / LOOKUP TABLES
-- ============================================================

CREATE SCHEMA IF NOT EXISTS ref;

-- Canonical town names — used by town_normalizer.py
-- All 15 Barnstable County towns + common aliases
CREATE TABLE IF NOT EXISTS ref.town_name_lookup (
    raw_name        VARCHAR     NOT NULL,   -- As it appears in source data
    canonical_name  VARCHAR     NOT NULL,   -- Standardized form
    source          VARCHAR,                -- Which source this alias came from
    PRIMARY KEY (raw_name, source)
);

-- Seed data
INSERT INTO ref.town_name_lookup (raw_name, canonical_name, source)
VALUES
    ('Barnstable',          'Barnstable',       'ALL'),
    ('Bourne',              'Bourne',           'ALL'),
    ('Brewster',            'Brewster',         'ALL'),
    ('Chatham',             'Chatham',          'ALL'),
    ('Dennis',              'Dennis',           'ALL'),
    ('Eastham',             'Eastham',          'ALL'),
    ('Falmouth',            'Falmouth',         'ALL'),
    ('Harwich',             'Harwich',          'ALL'),
    ('Mashpee',             'Mashpee',          'ALL'),
    ('Orleans',             'Orleans',          'ALL'),
    ('Provincetown',        'Provincetown',     'ALL'),
    ('Sandwich',            'Sandwich',         'ALL'),
    ('Truro',               'Truro',            'ALL'),
    ('Wellfleet',           'Wellfleet',        'ALL'),
    ('Yarmouth',            'Yarmouth',         'ALL'),
    -- Common aliases
    ('Hyannis',             'Barnstable',       'DOR'),
    ('Centerville',         'Barnstable',       'DOR'),
    ('Osterville',          'Barnstable',       'DOR'),
    ('Cotuit',              'Barnstable',       'DOR'),
    ('Marstons Mills',      'Barnstable',       'DOR'),
    ('West Barnstable',     'Barnstable',       'DOR'),
    ('Barnstable County',   'Barnstable',       'CENSUS'),
    ('P-Town',              'Provincetown',     'AIRBNB'),
    ('East Falmouth',       'Falmouth',         'PARCELS'),
    ('North Falmouth',      'Falmouth',         'PARCELS'),
    ('West Falmouth',       'Falmouth',         'PARCELS'),
    ('South Yarmouth',      'Yarmouth',         'PARCELS'),
    ('West Yarmouth',       'Yarmouth',         'PARCELS'),
    ('Yarmouthport',        'Yarmouth',         'PARCELS'),
    ('South Dennis',        'Dennis',           'PARCELS'),
    ('East Dennis',         'Dennis',           'PARCELS'),
    ('West Dennis',         'Dennis',           'PARCELS'),
    ('Dennis Port',         'Dennis',           'PARCELS'),
    ('North Harwich',       'Harwich',          'PARCELS'),
    ('Harwich Port',        'Harwich',          'PARCELS'),
    ('West Harwich',        'Harwich',          'PARCELS')
ON CONFLICT (raw_name, source) DO NOTHING;


CREATE TABLE IF NOT EXISTS raw.massgis_parcels (
    loc_id          VARCHAR,
    prop_id         VARCHAR,
    site_addr       VARCHAR,
    addr_num        VARCHAR,
    full_str        VARCHAR,
    city            VARCHAR,
    zip             VARCHAR(10),
    use_code        VARCHAR,
    total_val       INTEGER,
    other_val       INTEGER,
    bldg_val        INTEGER,
    land_val        INTEGER,
    lot_size        FLOAT,
    lot_units       VARCHAR,
    year_built      INTEGER,
    bld_area        INTEGER,
    res_area        INTEGER,
    units           INTEGER,
    style           VARCHAR,
    stories         VARCHAR,
    num_rooms       INTEGER,
    owner1          VARCHAR,
    own_addr        VARCHAR,
    own_city        VARCHAR,
    own_state       VARCHAR,
    own_zip         VARCHAR,
    zoning          VARCHAR,
    fy              INTEGER,
    town_id         INTEGER,
    town            VARCHAR,
    ingested_at     TIMESTAMP,
    PRIMARY KEY (loc_id)
);

CREATE TABLE IF NOT EXISTS staging.stg_massgis_parcels (
    loc_id          VARCHAR,
    prop_id         VARCHAR,
    site_addr       VARCHAR,
    addr_num        VARCHAR,
    full_str        VARCHAR,
    city            VARCHAR,
    zip             VARCHAR(10),
    use_code        VARCHAR,
    use_desc        VARCHAR,
    total_val       INTEGER,
    other_val       INTEGER,
    bldg_val        INTEGER,
    land_val        INTEGER,
    lot_size        FLOAT,
    lot_units       VARCHAR,
    year_built      INTEGER,
    bld_area        INTEGER,
    res_area        INTEGER,
    units           INTEGER,
    town_id         INTEGER,
    style           VARCHAR,
    stories         VARCHAR,
    num_rooms       INTEGER,
    owner1          VARCHAR,
    own_addr        VARCHAR,
    own_city        VARCHAR,
    own_state       VARCHAR,
    own_zip         VARCHAR,
    zoning          VARCHAR,
    fy              INTEGER,
    town            VARCHAR,
    is_residential  BOOLEAN,
    ingested_at     TIMESTAMP,
    PRIMARY KEY (loc_id)
);