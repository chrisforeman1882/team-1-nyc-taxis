"""Project-wide constants for the NYC Yellow Taxi pipeline."""

# ---------------------------------------------------------------------------
# Layer paths (relative to the catalog / mount root)
# ---------------------------------------------------------------------------
BRONZE_PATH = "bronze.nyc_taxis"
SILVER_PATH = "silver.nyc_taxis"
GOLD_PATH = "gold.nyc_taxis"

# ---------------------------------------------------------------------------
# Raw schema expectations
# ---------------------------------------------------------------------------
EXPECTED_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pickup_longitude",
    "pickup_latitude",
    "RatecodeID",
    "store_and_fwd_flag",
    "dropoff_longitude",
    "dropoff_latitude",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
]

# ---------------------------------------------------------------------------
# Accepted / reference values
# ---------------------------------------------------------------------------
PAYMENT_TYPE_MAP = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}

RATE_CODE_MAP = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride",
}
