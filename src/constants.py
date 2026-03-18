"""Project-wide constants for the NYC Yellow Taxi pipeline."""

# Unity Catalog table paths
CATALOG_SCHEMA = "students_data.`matthew-dobson-schema`"
BRONZE_TABLE = f"{CATALOG_SCHEMA}.nyc_yellow_taxi"
SILVER_TABLE = f"{CATALOG_SCHEMA}.nyc_yellow_taxi_silver"
GOLD_TABLE = f"{CATALOG_SCHEMA}.nyc_yellow_taxi_gold"

# Column name mapping: Bronze (raw) -> Silver (standardised snake_case)
COLUMN_RENAME_MAP = {
    "VendorID": "vendor_id",
    "RateCodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",  # already snake_case, kept for explicitness
}

# Lat/lon grid-binning resolution for zone approximation.
# The dataset uses raw lat/lon (pre-2016 format) rather than TLC taxi zone IDs.
# We bin to ~0.01 degree grid cells (~1.1 km) so Gold can aggregate "per zone"
# without a spatial join to the TLC shapefile.
LAT_LON_BIN_SIZE = 0.01

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

# Complete list of columns the Silver table must contain.
# Gold and ML layers should ONLY read from Silver, never Bronze.
# BQ-1 (demand by location/time): pickup_zone, hour_of_day, day_of_week
# BQ-2 (fare drivers):            total_amount, fare_amount, trip_distance, hour_of_day, pickup_zone
# BQ-3 (fare prediction):         pickup_zone, dropoff_zone, hour_of_day, day_of_week, is_weekend,
#                                  trip_distance, passenger_count, rate_code_id, total_amount
# BQ-4 (tip prediction, stretch): tip_amount, payment_type, fare_amount, trip_distance, pickup_zone
# A-02 (Gold fact table):          pickup_zone, hour_of_day, trip_duration_min, fare_amount, total_amount
# A-03 (revenue/zone/hour):       pickup_zone, hour_of_day, total_amount
# A-04 (duration by day):         trip_duration_min, day_of_week
