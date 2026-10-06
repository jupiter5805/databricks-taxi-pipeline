import pandas as pd

from src.prepare_data import (
    build_related_tables,
    validate_relationships,
)


def sample_trips():
    return pd.DataFrame(
        {
            "VendorID": [1, 2],
            "tpep_pickup_datetime": [
                "2024-01-01 08:00:00",
                "2024-01-01 09:00:00",
            ],
            "tpep_dropoff_datetime": [
                "2024-01-01 08:15:00",
                "2024-01-01 09:20:00",
            ],
            "passenger_count": [1, 2],
            "trip_distance": [2.1, 4.5],
            "RatecodeID": [1, 1],
            "store_and_fwd_flag": ["N", "N"],
            "PULocationID": [10, 20],
            "DOLocationID": [20, 10],
            "payment_type": [1, 2],
            "fare_amount": [12.0, 20.0],
            "extra": [1.0, 1.0],
            "mta_tax": [0.5, 0.5],
            "tip_amount": [2.0, 0.0],
            "tolls_amount": [0.0, 0.0],
            "improvement_surcharge": [1.0, 1.0],
            "total_amount": [16.5, 22.5],
            "congestion_surcharge": [0.0, 0.0],
            "Airport_fee": [0.0, 0.0],
        }
    )


def sample_zones():
    return pd.DataFrame(
        {
            "LocationID": [10, 20],
            "Borough": ["Manhattan", "Queens"],
            "Zone": ["Zone A", "Zone B"],
            "service_zone": ["Yellow Zone", "Boro Zone"],
        }
    )


def test_builds_three_related_tables():
    trips, locations, payments = build_related_tables(
        sample_trips(),
        sample_zones(),
        sample_rows=None,
    )

    assert len(trips) == 2
    assert len(locations) == 2
    assert len(payments) == 6


def test_trip_ids_are_unique():
    trips, _, _ = build_related_tables(
        sample_trips(),
        sample_zones(),
        sample_rows=None,
    )

    assert trips["trip_id"].is_unique


def test_foreign_keys_are_valid():
    trips, locations, payments = build_related_tables(
        sample_trips(),
        sample_zones(),
        sample_rows=None,
    )

    assert validate_relationships(
        trips,
        locations,
        payments,
    ) == {
        "invalid_pickup_locations": 0,
        "invalid_dropoff_locations": 0,
        "invalid_payment_types": 0,
    }
