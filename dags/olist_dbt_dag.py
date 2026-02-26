"""
olist_dbt_dag.py
----------------
Airflow DAG that orchestrates dbt transformation tasks for the Olist pipeline.
Should be triggered manually after olist_ingestion_dag has loaded raw data.

DAG tasks (run in order):
  1. dbt_seed  - loads any seed CSV files from dbt/seeds/ into olist_staging
  2. dbt_run   - builds all staging, dim, and fact models

dbt commands run inside the persistent dbt-br container via docker exec.
The dbt project lives at /usr/app/olist inside that container.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# ---------------------------------------------------------------------------
# dbt container config
# ---------------------------------------------------------------------------

DBT_CONTAINER    = "dbt-br"
DBT_PROJECT_DIR  = "/usr/app/olist"
DBT_PROFILES_DIR = "/usr/app/olist"


def dbt_cmd(subcommand: str) -> str:
    """Build a docker exec command for a dbt subcommand."""
    return (
        f"docker exec {DBT_CONTAINER} "
        f"dbt {subcommand} "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR}"
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
}

with DAG(
    dag_id="olist_dbt_dag",
    default_args=default_args,
    schedule=None,
    description="Orchestrate dbt transformations for the Olist pipeline",
    tags=["olist", "dbt"],
) as dag:

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=dbt_cmd("seed"),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=dbt_cmd("run"),
    )

    dbt_seed >> dbt_run
