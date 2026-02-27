"""
olist_pipeline_dag.py
---------------------
Single Airflow DAG that orchestrates the entire Olist pipeline end-to-end:
  1. create_raw_schema   - ensures olist_raw schema exists in PostgreSQL
  2. load_csv_files      - reads all 9 Olist CSVs and bulk-loads into olist_raw
  3. validate_row_counts - queries each table and logs its row count
  4. dbt_seed            - loads seed CSV files into olist_staging
  5. dbt_run             - builds all staging, dim, and fact models
  6. dbt_test            - runs all dbt tests

Ingestion tasks use PythonOperator with pandas + SQLAlchemy.
dbt tasks use BashOperator to run dbt directly inside the Airflow container.
"""

import glob
import logging
import os

import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# PostgreSQL config — matches the postgres-br container
# ---------------------------------------------------------------------------

DB_PARAMS = {
    'host': 'postgres',
    'port': 5432,
    'dbname': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
}

DB_CONN_STRING = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

CSV_DIR = '/opt/airflow/datasets'
RAW_SCHEMA = 'olist_raw'

# ---------------------------------------------------------------------------
# dbt config — runs directly inside the Airflow container
# ---------------------------------------------------------------------------

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"


def dbt_cmd(subcommand: str) -> str:
    """Build a dbt command to run inside the Airflow container."""
    return (
        f"dbt {subcommand} "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR}"
    )


# ---------------------------------------------------------------------------
# Ingestion helper
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
# Task callables
# ---------------------------------------------------------------------------

def create_raw_schema():
    """Create olist_raw schema if it does not exist.
    Also drops olist_staging (CASCADE) so raw tables can be replaced
    without 'dependent objects still exist' errors. dbt_run rebuilds it.
    """
    log = logging.getLogger(__name__)

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS olist_staging CASCADE;")
    log.info("Dropped olist_staging schema (will be rebuilt by dbt_run).")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};")
    conn.commit()
    cur.close()
    conn.close()

    log.info("Schema '%s' is ready.", RAW_SCHEMA)


def load_csv_files():
    """Read every CSV in CSV_DIR and bulk-load into olist_raw (replace mode)."""
    log = logging.getLogger(__name__)
    engine = create_engine(DB_CONN_STRING)

    csv_files = sorted(glob.glob(os.path.join(CSV_DIR, '*.csv')))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in '{CSV_DIR}'. "
            "Make sure the Olist CSVs are in the datasets/olist/ volume."
        )

    log.info("Found %d CSV file(s) to load.", len(csv_files))

    for csv_path in csv_files:
        table_name = csv_to_table_name(csv_path)
        log.info(
            "Loading '%s'  ->  %s.%s",
            os.path.basename(csv_path), RAW_SCHEMA, table_name
        )

        df = pd.read_csv(csv_path, low_memory=False)

        df.to_sql(
            name=table_name,
            con=engine,
            schema=RAW_SCHEMA,
            if_exists='replace',
            index=False,
        )

        log.info("  Loaded %s rows into %s.%s", f"{len(df):,}", RAW_SCHEMA, table_name)

    engine.dispose()
    log.info("All %d CSV file(s) loaded successfully.", len(csv_files))


def validate_row_counts():
    """Query each table in olist_raw and log its row count."""
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

with DAG(
    dag_id='olist_pipeline_dag',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    description='End-to-end Olist pipeline: CSV ingestion → dbt transformations',
    tags=['olist', 'ingestion', 'dbt'],
) as dag:

    # --- Ingestion tasks ---

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

    # --- dbt tasks ---

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=dbt_cmd('seed'),
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=dbt_cmd('run'),
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=dbt_cmd('test'),
    )

    # --- Pipeline chain ---

    create_schema_task >> load_task >> validate_task >> dbt_seed >> dbt_run >> dbt_test
