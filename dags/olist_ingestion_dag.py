"""
olist_ingestion_dag.py
----------------------
Airflow DAG that loads the Olist Brazilian E-commerce CSV files from the
shared datasets directory into a dedicated `olist_raw` schema in PostgreSQL.

DAG tasks (run in order):
  1. create_raw_schema   - ensures the olist_raw schema exists in Postgres
  2. load_csv_files      - reads each CSV with pandas and bulk-loads it into Postgres
  3. validate_row_counts - queries each loaded table and logs its row count

CSV files are read from the Airflow datasets volume:
  /opt/airflow/datasets/olist/
  (on host: data-engineering-bootcamp/airflow/datasets/olist/)
"""

import glob
import logging
import os

import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Configuration — matches the running postgres-lab container
# ---------------------------------------------------------------------------

# psycopg2 uses the Docker-internal hostname 'postgres' and port 5432
DB_PARAMS = {
    'host': 'postgres',
    'port': 5432,
    'dbname': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
}

# SQLAlchemy connection string — used by pandas.to_sql() for bulk loading
DB_CONN_STRING = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

# Path inside the Airflow container where CSV files are mounted
CSV_DIR = '/opt/airflow/datasets/olist'

# The PostgreSQL schema where all raw Olist tables will land
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
# Task 1 callable
# ---------------------------------------------------------------------------

def create_raw_schema():
    """
    Creates the olist_raw schema in PostgreSQL if it does not already exist.
    Uses psycopg2 directly — no pandas needed for a single SQL statement.
    IF NOT EXISTS makes this safe to re-run.
    """
    log = logging.getLogger(__name__)
    log.info("Creating schema '%s' if it does not exist...", RAW_SCHEMA)

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};")
    conn.commit()
    cur.close()
    conn.close()

    log.info("Schema '%s' is ready.", RAW_SCHEMA)

# ---------------------------------------------------------------------------
# Task 2 callable
# ---------------------------------------------------------------------------

def load_csv_files():
    """
    Reads every CSV in CSV_DIR with pandas and bulk-loads it into olist_raw.

    if_exists='replace' drops and recreates each table on every run,
    which makes the DAG idempotent (re-running it won't duplicate rows).
    """
    log = logging.getLogger(__name__)
    engine = create_engine(DB_CONN_STRING)

    csv_files = sorted(glob.glob(os.path.join(CSV_DIR, '*.csv')))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in '{CSV_DIR}'. "
            "Make sure the Olist CSVs are in "
            "data-engineering-bootcamp/airflow/datasets/olist/"
        )

    log.info("Found %d CSV file(s) to load.", len(csv_files))

    for csv_path in csv_files:
        table_name = csv_to_table_name(csv_path)
        log.info(
            "Loading '%s'  ->  %s.%s",
            os.path.basename(csv_path), RAW_SCHEMA, table_name
        )

        # low_memory=False suppresses dtype-guessing warnings on large files
        df = pd.read_csv(csv_path, low_memory=False)

        df.to_sql(
            name=table_name,
            con=engine,
            schema=RAW_SCHEMA,
            if_exists='replace',  # drop + recreate on each run (idempotent)
            index=False,          # don't write the DataFrame index as a DB column
        )

        log.info("  Loaded %s rows into %s.%s", f"{len(df):,}", RAW_SCHEMA, table_name)

    engine.dispose()
    log.info("All %d CSV file(s) loaded successfully.", len(csv_files))

# ---------------------------------------------------------------------------
# Task 3 callable
# ---------------------------------------------------------------------------

def validate_row_counts():
    """
    Queries each table in olist_raw and logs its row count.
    A count of 0 or a missing table signals that Task 2 failed silently.
    """
    log = logging.getLogger(__name__)
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

    log.info("Validating %d table(s) in schema '%s':", len(tables), RAW_SCHEMA)

    with engine.connect() as conn:
        for table in tables:
            count = conn.execute(
                text(f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{table}")
            ).scalar()
            log.info("  %-50s  %s rows", f"{RAW_SCHEMA}.{table}", f"{count:,}")

    engine.dispose()

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

with DAG(
    dag_id='olist_ingestion_dag',
    default_args=default_args,
    schedule=None,          # triggered manually from the Airflow UI
    description='Load Olist CSV files into PostgreSQL olist_raw schema',
    tags=['olist', 'ingestion'],
) as dag:

    create_schema_task = PythonOperator(
        task_id='create_raw_schema',
        python_callable=create_raw_schema,
    )

    load_task = PythonOperator(
        task_id='load_csv_files',
        python_callable=load_csv_files,
    )

    validate_task = PythonOperator(
        task_id='validate_row_counts',
        python_callable=validate_row_counts,
    )

    # Arrow syntax defines the execution order: schema -> load -> validate
    create_schema_task >> load_task >> validate_task
