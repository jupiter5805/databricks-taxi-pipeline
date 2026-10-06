from __future__ import annotations

from pathlib import Path
import pandas as pd
import requests

RAW_URL = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "trip-data/yellow_tripdata_2024-01.parquet"
)
ZONE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "misc/taxi_zone_lookup.csv"
)

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

RAW_TRIPS_FILE = RAW_DIR / "yellow_tripdata_2024-01.parquet"
RAW_ZONES_FILE = RAW_DIR / "taxi_zone_lookup.csv"

TRIPS_OUT = PROCESSED_DIR / "trips.csv"
LOCATIONS_OUT = PROCESSED_DIR / "locations.csv"
PAYMENTS_OUT = PROCESSED_DIR / "payment_types.csv"

PAYMENT_TYPES = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}


def download_file(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists():
        print(f"Already exists: {destination}")
        return

    print(f"Downloading {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    destination.write_bytes(response.content)
    print(f"Saved: {destination}")


def build_related_tables(
    trips_df: pd.DataFrame,
    zones_df: pd.DataFrame,
    sample_rows: int | None = 250_000,
):
    trips = trips_df.copy()
    zones = zones_df.copy()

    if sample_rows is not None and len(trips) > sample_rows:
        trips = trips.sample(
            n=sample_rows,
            random_state=42,
        ).sort_index()

    trips = trips.reset_index(drop=True)
    trips.insert(0, "trip_id", range(1, len(trips) + 1))

    trips = trips.rename(
        columns={
            "VendorID": "vendor_id",
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
            "passenger_count": "passenger_count",
            "trip_distance": "trip_distance",
            "RatecodeID": "rate_code_id",
            "store_and_fwd_flag": "store_and_forward_flag",
            "PULocationID": "pickup_location_id",
            "DOLocationID": "dropoff_location_id",
            "payment_type": "payment_type_id",
            "fare_amount": "fare_amount",
            "extra": "extra",
            "mta_tax": "mta_tax",
            "tip_amount": "tip_amount",
            "tolls_amount": "tolls_amount",
            "improvement_surcharge": "improvement_surcharge",
            "total_amount": "total_amount",
            "congestion_surcharge": "congestion_surcharge",
            "Airport_fee": "airport_fee",
        }
    )

    wanted_columns = [
        "trip_id",
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "store_and_forward_flag",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type_id",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
    ]

    trips = trips[
        [c for c in wanted_columns if c in trips.columns]
    ]

    locations = zones.rename(
        columns={
            "LocationID": "location_id",
            "Borough": "borough",
            "Zone": "zone",
            "service_zone": "service_zone",
        }
    )[
        ["location_id", "borough", "zone", "service_zone"]
    ].drop_duplicates(subset=["location_id"])

    payment_types = pd.DataFrame(
        [
            {
                "payment_type_id": key,
                "payment_type": value,
            }
            for key, value in PAYMENT_TYPES.items()
        ]
    )

    return trips, locations, payment_types


def validate_relationships(
    trips: pd.DataFrame,
    locations: pd.DataFrame,
    payment_types: pd.DataFrame,
):
    valid_locations = set(
        pd.to_numeric(
            locations["location_id"],
            errors="coerce",
        ).dropna().astype(int)
    )

    valid_payments = set(
        pd.to_numeric(
            payment_types["payment_type_id"],
            errors="coerce",
        ).dropna().astype(int)
    )

    pickup_ids = pd.to_numeric(
        trips["pickup_location_id"],
        errors="coerce",
    ).dropna().astype(int)

    dropoff_ids = pd.to_numeric(
        trips["dropoff_location_id"],
        errors="coerce",
    ).dropna().astype(int)

    payment_ids = pd.to_numeric(
        trips["payment_type_id"],
        errors="coerce",
    ).dropna().astype(int)

    return {
        "invalid_pickup_locations": int(
            (~pickup_ids.isin(valid_locations)).sum()
        ),
        "invalid_dropoff_locations": int(
            (~dropoff_ids.isin(valid_locations)).sum()
        ),
        "invalid_payment_types": int(
            (~payment_ids.isin(valid_payments)).sum()
        ),
    }


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    download_file(RAW_URL, RAW_TRIPS_FILE)
    download_file(ZONE_URL, RAW_ZONES_FILE)

    print("Reading source files...")
    source_trips = pd.read_parquet(RAW_TRIPS_FILE)
    source_zones = pd.read_csv(RAW_ZONES_FILE)

    trips, locations, payment_types = build_related_tables(
        source_trips,
        source_zones,
        sample_rows=250_000,
    )

    report = validate_relationships(
        trips,
        locations,
        payment_types,
    )

    trips.to_csv(TRIPS_OUT, index=False)
    locations.to_csv(LOCATIONS_OUT, index=False)
    payment_types.to_csv(PAYMENTS_OUT, index=False)

    print("\nPrepared Databricks ingestion files")
    print("----------------------------------")
    print(f"Trips:         {len(trips):,} -> {TRIPS_OUT}")
    print(f"Locations:     {len(locations):,} -> {LOCATIONS_OUT}")
    print(f"Payment types: {len(payment_types):,} -> {PAYMENTS_OUT}")

    print("\nForeign-key validation")
    for key, value in report.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
