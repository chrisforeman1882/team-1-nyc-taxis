"""Project-wide constants for the NYC Yellow Taxi pipeline."""

# ── Source ────────────────────────────────────────────────────────────────────
SOURCE_TABLE = "data_academy_resources.nyc_taxi.yellow_tripdata"

# ── Unity Catalog table paths ─────────────────────────────────────────────────
BRONZE_TABLE = "students_data.`chris-foreman`.bronze_yellow_tripdata"
SILVER_TABLE = "students_data.`chris-foreman`.silver_yellow_tripdata"

# Gold layer: fact table persisted as Delta, dimension views computed on read.
# Decision rationale: fact table aggregates 140M Silver rows to ~500K rows —
# persisting avoids expensive re-aggregation on every dashboard load.
# Dims are tiny (168 rows, ~3–5K rows) and compute instantly, so views
# guarantee freshness with no staleness risk.
GOLD_FACT_TABLE = "students_data.`chris-foreman`.gold_fact_trips"

# Column name mapping: Bronze (raw) -> Silver (standardised snake_case)
# NOTE: The 2016 CSVs use "RatecodeID" (lowercase c) while 2015 uses "RateCodeID".
# Schema mismatch causes 73% of rows to have NULL RateCodeID with the real value
# rescued into _rescued_data JSON — see recover_rate_code_id() in transforms.py.
COLUMN_RENAME_MAP = {
    "VendorID": "vendor_id",
    "RateCodeID": "rate_code_id",
}

# Valid TLC rate codes. Code 99 appears in rescued data (1,670 rows) and is
# treated as a data entry error -> mapped to NULL in Silver.
VALID_RATE_CODES = {1, 2, 3, 4, 5, 6}

# Lat/lon grid-binning resolution for zone approximation.
# The dataset uses raw lat/lon (pre-2016 format) rather than TLC taxi zone IDs.
# We bin to ~0.01 degree grid cells (~1.1 km) so Gold can aggregate "per zone"
# without a spatial join to the TLC shapefile.
LAT_LON_BIN_SIZE = 0.01

# Conservative NYC bounding box (covers all 5 boroughs + nearby airports).
# Coordinates outside this box are treated as missing/corrupt.
NYC_LAT_MIN = 40.4
NYC_LAT_MAX = 40.95
NYC_LON_MIN = -74.3
NYC_LON_MAX = -73.7

# Columns that must be cast to timestamp in Silver
DATETIME_COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

# Columns that must be cast to numeric types in Silver
NUMERIC_CAST_MAP = {
    "vendor_id": "int",
    "passenger_count": "int",
    "rate_code_id": "int",
    "payment_type": "int",
    "trip_distance": "double",
    "pickup_longitude": "double",
    "pickup_latitude": "double",
    "dropoff_longitude": "double",
    "dropoff_latitude": "double",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
}

# Columns that must not be null in a valid Silver row
REQUIRED_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "payment_type",
]

# Silver column coverage by business question (Gold/ML must only read from Silver).
# BQ-1 (demand by location/time): pickup_zone, hour_of_day, day_of_week
# BQ-2 (fare drivers):            total_amount, fare_amount, trip_distance, hour_of_day, pickup_zone
# BQ-3 (fare prediction):         pickup_zone, dropoff_zone, hour_of_day, day_of_week, is_weekend,
#                                  trip_distance, passenger_count, rate_code_id, total_amount
# BQ-4 (tip prediction, stretch): tip_amount, payment_type, fare_amount, trip_distance, pickup_zone
# A-02 (Gold fact table):         pickup_zone, hour_of_day, trip_duration_min, fare_amount, total_amount
# A-03 (revenue/zone/hour):       pickup_zone, hour_of_day, total_amount
# A-04 (duration by day):         trip_duration_min, day_of_week

# ── I-04 Outlier thresholds ──────────────────────────────────────────────────

# Finding #8: 914 trips exceed 100 mi (likely GPS errors or inter-city rides)
MAX_TRIP_DISTANCE = 100

# Finding #4/#8: fare_amount ≤ 0 dropped; 458 fares > $500 dropped
MAX_FARE_AMOUNT = 500

# Finding #5/#12: total_amount < 0 dropped (voided); 210 totals > $1,000 dropped
MAX_TOTAL_AMOUNT = 1000

# Finding #12: tips capped at $200 (840 negative → 0, 1,368 > $100 outliers)
MAX_TIP_AMOUNT = 200

# Finding #11: trips > 24 h dropped (330 rows, max ~381 days).
# Trips ≤ 24 h retained — includes legitimate airport flat-rate rides.
MAX_TRIP_DURATION_MIN = 1440  # 24 hours in minutes

# Finding #12: extra surcharge should be $0.50 (rush hour) or $1.00 (overnight)
VALID_EXTRA_VALUES = {0.0, 0.5, 1.0}

# ── ML-02 Feature table ─────────────────────────────────────────────────────

# Features selected for fare prediction (BQ-3).
# Known before/at trip start — EXCLUDES fare_amount, tip_amount, tolls_amount,
# mta_tax, improvement_surcharge (all components of total_amount → leakage).
ML_FEATURE_COLUMNS = [
    "pickup_zone",
    "dropoff_zone",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "trip_distance",
    "passenger_count",
    "rate_code_id",
]

ML_TARGET_COLUMN = "total_amount"

# Train/test split configuration
ML_TEST_SIZE = 0.2
ML_RANDOM_STATE = 42

# Sampling fraction — full Silver (~94M rows) is too large for sklearn
# in-memory training. 1% ≈ 900K rows gives robust temporal coverage.
# Risk note: "Use a sampled subset for training; scale up only if time permits."
ML_SAMPLE_FRACTION = 0.01

# ── A-01 Gold schema constants ───────────────────────────────────────────────

# Spark dayofweek() convention: 1=Sunday, 2=Monday, … 7=Saturday
DAY_NAME_MAP = {
    1: "Sunday",
    2: "Monday",
    3: "Tuesday",
    4: "Wednesday",
    5: "Thursday",
    6: "Friday",
    7: "Saturday",
}

# Time-of-day period bins for dim_time (human-readable dashboard labels)
TIME_PERIOD_BINS = {
    "Night": (0, 5),  # 00:00–05:59
    "Morning": (6, 11),  # 06:00–11:59
    "Afternoon": (12, 17),  # 12:00–17:59
    "Evening": (18, 23),  # 18:00–23:59
}

# ── TLC data dictionary lookups ──────────────────────────────────────────────
# Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# Codes 0 (Flex Fare) and 6 (Voided trip) are not present in the
# Jan 2015 / Jan–Mar 2016 dataset but are included for completeness.

PAYMENT_TYPE_MAP = {
    0: "Flex Fare trip",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}

RATE_CODE_MAP = {
    1: "Standard",
    2: "JFK",
    3: "Newark",
    4: "Nassau/Westchester",
    5: "Negotiated",
    6: "Group ride",
}
