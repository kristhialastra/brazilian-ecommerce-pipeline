"""
load_olist_to_postgres.py
--------------------------
Standalone script to load Olist CSV files into the olist_raw schema in PostgreSQL.

Run this directly from your terminal (outside Docker) for manual testing:
  python scripts/load_olist_to_postgres.py

Prerequisites:
  pip install pandas sqlalchemy psycopg2-binary

Connection: localhost:5433  (the external port mapped from postgres-lab container)

This script mirrors the logic inside olist_ingestion_dag.py but runs
independently — no Airflow required.
"""

import glob
import logging
import os
import sys

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Logging setup — print timestamped messages to the terminal
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)s  %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — use localhost:5433 (external port, for running outside Docker)
# ---------------------------------------------------------------------------

DB_PARAMS = {
    'host': 'localhost',
    'port': 5433,           # external port exposed by postgres-lab container
    'dbname': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
}

DB_CONN_STRING = "postgresql+psycopg2://airflow:airflow@localhost:5433/airflow"

RAW_SCHEMA = 'olist_raw'

# ---------------------------------------------------------------------------
# Helper function
# ---------------------------------------------------------------------------

def csv_to_table_name(filepath: str) -> str:
    """
    Derive a clean table name from a CSV filename.

    Examples:
      olist_orders_dataset.csv               ->  orders
      olist_order_items_dataset.csv          ->  order_items
      product_category_name_translation.csv  ->  product_category_name_translation
    """
    name = os.path.basename(filepath).replace('.csv', '')
    if name.startswith('olist_'):
        name = name[len('olist_'):]
    if name.endswith('_dataset'):
        name = name[: -len('_dataset')]
    return name

# ---------------------------------------------------------------------------
# Step 1: Create schema
# ---------------------------------------------------------------------------

def create_raw_schema():
    log.info("Step 1 — Creating schema '%s' if it does not exist...", RAW_SCHEMA)
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};")
    conn.commit()
    cur.close()
    conn.close()
    log.info("Schema '%s' is ready.", RAW_SCHEMA)

# ---------------------------------------------------------------------------
# Step 2: Load CSVs
# ---------------------------------------------------------------------------

def load_csv_files(csv_dir: str):
    log.info("Step 2 — Loading CSV files from: %s", csv_dir)
    engine = create_engine(DB_CONN_STRING)

    csv_files = sorted(glob.glob(os.path.join(csv_dir, '*.csv')))

    if not csv_files:
        log.error("No CSV files found in '%s'. Check your path.", csv_dir)
        sys.exit(1)

    log.info("Found %d CSV file(s).", len(csv_files))

    for csv_path in csv_files:
        table_name = csv_to_table_name(csv_path)
        log.info("Loading '%s'  ->  %s.%s", os.path.basename(csv_path), RAW_SCHEMA, table_name)

        df = pd.read_csv(csv_path, low_memory=False)

        df.to_sql(
            name=table_name,
            con=engine,
            schema=RAW_SCHEMA,
            if_exists='replace',
            index=False,
        )

        log.info("  Loaded %s rows.", f"{len(df):,}")

    engine.dispose()
    log.info("All %d CSV file(s) loaded successfully.", len(csv_files))

# ---------------------------------------------------------------------------
# Step 3: Validate row counts
# ---------------------------------------------------------------------------

def validate_row_counts():
    log.info("Step 3 — Validating row counts in schema '%s':", RAW_SCHEMA)
    engine = create_engine(DB_CONN_STRING)

    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :schema ORDER BY table_name"
            ),
            {'schema': RAW_SCHEMA},
        )
        tables = [row[0] for row in result]

    with engine.connect() as conn:
        for table in tables:
            count = conn.execute(
                text(f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{table}")
            ).scalar()
            log.info("  %-50s  %s rows", f"{RAW_SCHEMA}.{table}", f"{count:,}")

    engine.dispose()

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    # Default CSV directory: the olist folder inside the Airflow datasets volume
    # Adjust this path if your CSVs are in a different location
    DEFAULT_CSV_DIR = (
        r'C:\Users\i_kristhiacayle.last\Documents'
        r'\data-engineering-bootcamp\airflow\datasets\olist'
    )

    csv_dir = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CSV_DIR

    create_raw_schema()
    load_csv_files(csv_dir)
    validate_row_counts()
