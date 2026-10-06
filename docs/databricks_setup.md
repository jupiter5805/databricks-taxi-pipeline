# Databricks Setup

## Generate the three related files locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -v
python -m src.prepare_data
```

This creates:

- `data/processed/trips.csv`
- `data/processed/locations.csv`
- `data/processed/payment_types.csv`

## Upload to Databricks Free Edition

Create/import a notebook named `01_ingest_transform`.

Use **New > Add or upload data > Create or modify a table** and create:

- `trips_raw`
- `locations_raw`
- `payment_types_raw`

from the three generated CSV files.

Then run `notebooks/01_ingest_transform.py`.

## Visualisations

For each `display(...)` result in the notebook, use Databricks'
native visualization menu.

Recommended charts:

- Line: pickup hour vs trip count
- Bar: pickup zone vs trip count
- Bar/Pie: payment type vs trip count
- Bar: pickup borough vs total revenue

## Optional automation

Upload the three CSV files to a Databricks Volume and change
`BASE_PATH` in `notebooks/02_volume_ingestion.py`.

That notebook automatically creates the three raw Delta tables.
