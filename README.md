# NYC Taxi Databricks Pipeline

A complete Databricks data-engineering sprint built with public NYC Yellow Taxi trip data.

## What this project demonstrates

- Databricks notebooks
- Apache Spark / PySpark
- Python and Pandas
- SQL
- Delta tables
- relational data modelling
- data cleaning
- feature engineering
- native Databricks visualisations
- automated file ingestion

## Architecture

```text
NYC Yellow Taxi Parquet + Taxi Zone Lookup
                    |
                    v
          Local Python preparation
                    |
      +-------------+-------------+
      |             |             |
      v             v             v
 trips.csv    locations.csv   payment_types.csv
      |             |             |
      +-------------+-------------+
                    |
                    v
          Databricks raw tables
                    |
                    v
           PySpark cleaning
                    |
                    v
      Feature engineering + joins
                    |
                    v
             trips_clean
             Delta table
                    |
          +---------+---------+
          |         |         |
          v         v         v
         SQL      Views   Visualisations
```

## Related datasets

### `trips.csv`
Fact-like taxi trip records.

Foreign keys:

- `pickup_location_id` -> `locations.location_id`
- `dropoff_location_id` -> `locations.location_id`
- `payment_type_id` -> `payment_types.payment_type_id`

### `locations.csv`

- location_id
- borough
- zone
- service_zone

### `payment_types.csv`

- payment_type_id
- payment_type

## Local setup

```bash
git clone YOUR_REPOSITORY_URL
cd databricks-taxi-pipeline

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
pytest -v
python -m src.prepare_data
```

The preparation script downloads the source data and generates up to
250,000 reproducibly sampled trips for the Databricks exercise.

## Databricks

Import:

```text
notebooks/01_ingest_transform.py
```

as the notebook:

```text
01_ingest_transform
```

Upload the three generated CSV files and create tables named:

```text
trips_raw
locations_raw
payment_types_raw
```

The notebook then:

1. queries raw data with SQL
2. reads tables with Spark
3. casts and cleans raw data
4. calculates trip duration
5. calculates average speed
6. calculates fare per mile
7. calculates tip percentage
8. classifies trips by distance
9. classifies trips by time of day
10. joins pickup/drop-off locations
11. joins payment descriptions
12. writes `trips_clean` as a Delta table
13. creates analytical SQL queries and views
14. provides four visualisations

## Visualisations

- Hourly taxi demand
- Top pickup zones
- Payment method distribution
- Revenue by pickup borough

## Automated ingestion

`notebooks/02_volume_ingestion.py` demonstrates a second ingestion path
from a Databricks Volume and writes the input files to Delta raw tables.

## Tests

```bash
pytest -v
```

The tests verify:

- 3 related datasets are created
- `trip_id` is unique
- location foreign keys are valid
- payment type foreign keys are valid

## Project structure

```text
databricks-taxi-pipeline/
├── data/
│   ├── raw/
│   └── processed/
├── docs/
│   └── databricks_setup.md
├── notebooks/
│   ├── 01_ingest_transform.py
│   └── 02_volume_ingestion.py
├── src/
│   ├── __init__.py
│   └── prepare_data.py
├── tests/
│   └── test_prepare_data.py
├── .gitignore
├── README.md
└── requirements.txt
```

## Data source

NYC Taxi & Limousine Commission Trip Record Data.

The project uses the January 2024 Yellow Taxi Parquet dataset and
the official Taxi Zone Lookup Table.
