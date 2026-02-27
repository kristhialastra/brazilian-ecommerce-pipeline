# Brazilian E-Commerce Pipeline (Olist)

End-to-end data pipeline that ingests raw CSV data from the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), loads it into PostgreSQL, transforms it into a star schema using dbt, and orchestrates the entire flow with Apache Airflow.

Created by: [kristhiacayle](https://github.com/kristhiacayle)

Submitted to: [jgvillanuevastratpoint](https://github.com/jgvillanuevastratpoint)

Submission Date: February 27, 2026

## Project Structure

```
brazilian-ecommerce-pipeline/
├── docker-compose.yml        
├── .gitignore
│
├── dags/
│   └── olist_pipeline_dag.py   
│
├── scripts/
│   ├── load_olist_to_postgres.py   
│   ├── dockerfile
│   └── requirements.txt
│
├── airflow/
│   ├── dockerfile              
│   ├── requirements.txt
│   └── logs/                   
│
├── datasets/                  
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── sources.yml         
│   │   ├── staging/            
│   │   └── marts/              
│   └── dbt_packages/           
│
└── docs/
    └── diagrams/               
    └── screenshots/            
```

## Architecture and Pipeline Flow
![My First Board (6)](https://github.com/user-attachments/assets/19e4f484-ad97-44e9-b369-40d1c4150309)


### Airflow DAG

`olist_pipeline_dag` runs 6 tasks in sequence:

```
create_raw_schema → load_csv_files → validate_row_counts → dbt_seed → dbt_run → dbt_test
```

| Task | Type | What It Does |
|------|------|-------------|
| `create_raw_schema` | PythonOperator | Drops `olist_staging` (CASCADE) and ensures `olist_raw` schema exists. Dropping staging first makes the pipeline idempotent — raw tables can be replaced without "dependent objects" errors. |
| `load_csv_files` | PythonOperator | Reads all 9 CSVs with pandas, bulk-loads each into `olist_raw` using `to_sql(if_exists='replace')` |
| `validate_row_counts` | PythonOperator | Queries every table in `olist_raw` and logs its row count |
| `dbt_seed` | BashOperator | Runs `dbt seed` to load any seed CSVs |
| `dbt_run` | BashOperator | Runs `dbt run` — builds all 9 staging views + 5 mart tables |
| `dbt_test` | BashOperator | Runs `dbt test` — validates primary keys, foreign keys, and data quality |

Schedule: `@daily` with `catchup=False`.

## Star Schema
![My First Board (3)](https://github.com/user-attachments/assets/8ba0b74b-c22e-4cf8-a19d-bdc41d9861f3)


## dbt Models

### Staging (9 views in `olist_staging`)

Each staging model maps 1:1 to a raw source table. They clean column names, cast types (e.g., text → timestamp), and fix known data issues (e.g., `product_name_lenght` typo → `product_name_length`).

| Model | Source Table |
|-------|-------------|
| `stg_orders` | `olist_raw.orders` |
| `stg_order_items` | `olist_raw.order_items` |
| `stg_order_payments` | `olist_raw.order_payments` |
| `stg_order_reviews` | `olist_raw.order_reviews` |
| `stg_customers` | `olist_raw.customers` |
| `stg_sellers` | `olist_raw.sellers` |
| `stg_products` | `olist_raw.products` |
| `stg_geolocation` | `olist_raw.geolocation` |
| `stg_product_category_translation` | `olist_raw.product_category_name_translation` |

### Marts (5 tables in `olist_staging`)

All mart models use `ref()` to depend on staging models, forming a clean DAG.

**Design decisions:**
- **Geolocation excluded** from the star schema — 1M+ rows with multiple coordinates per zip code, not a clean dimension.
- **Payments aggregated** to order level in the fact table (total payment value + primary payment type) rather than a separate dimension.
- **Reviews deduplicated** to one per order (most recent), stored as `review_score` in the fact.
- **~3% of orders** have NULL delivery dates (canceled/processing). Their date keys are NULL in the fact.


### Verification Query

```sql
SELECT 'fact_order_items' AS table_name, COUNT(*) AS row_count FROM olist_staging.fact_order_items
UNION ALL SELECT 'dim_customers',  COUNT(*) FROM olist_staging.dim_customers
UNION ALL SELECT 'dim_products',   COUNT(*) FROM olist_staging.dim_products
UNION ALL SELECT 'dim_sellers',    COUNT(*) FROM olist_staging.dim_sellers
UNION ALL SELECT 'dim_date',       COUNT(*) FROM olist_staging.dim_date;
```

Expected output:

| table_name | row_count |
|-----------|-----------|
| fact_order_items | 112,650 |
| dim_customers | 96,096 |
| dim_products | 32,951 |
| dim_sellers | 3,095 |
| dim_date | 1,096 |

## Challenges & Solutions

### 1. Docker-in-Docker: Airflow calling dbt in a separate container

**Problem:** The initial design had Airflow's BashOperator running `docker exec dbt-br dbt run` to trigger dbt in a separate container. This required mounting the Docker socket (`/var/run/docker.sock`) into Airflow containers and installing the Docker CLI.

**What went wrong:**
- Docker socket permission denied — the `airflow` user (non-root) couldn't access the socket. Fixed with `group_add` in docker-compose.yml, but then:
- Docker CLI version mismatch — the `docker.io` apt package installed API version 1.41, but Docker Desktop required minimum 1.44. Installing the official Docker CE CLI from Docker's apt repository would fix it, but added too much complexity.

**Solution:** Installed `dbt-postgres==1.7.6` directly in the Airflow Docker image via `requirements.txt` and mounted the dbt project as a volume (`./dbt:/opt/airflow/dbt`). BashOperator now runs `dbt run --project-dir /opt/airflow/dbt` directly — no Docker socket, no Docker CLI, no version issues.

### 2. dbt container crash-looping on startup

**Problem:** The `dbt-postgres` Docker image has `ENTRYPOINT ["dbt"]`, so adding `command: tail -f /dev/null` in docker-compose actually ran `dbt tail -f /dev/null`, which failed immediately and kept restarting.

**Solution:** Overrode the entrypoint entirely with `entrypoint: ["tail", "-f", "/dev/null"]` and `restart: unless-stopped`. This makes dbt-br a persistent dev container for manual `docker exec` commands.

### 3. `catchup=False` silently ignored — 735 failed DAG runs

**Problem:** Initially had two separate DAGs (`olist_ingestion_dag` and `olist_dbt_dag`). Both had `catchup: False` inside `default_args`, but Airflow ignores `catchup` in `default_args` — it must be a DAG-level parameter. With `start_date=datetime(2024, 1, 1)`, Airflow backfilled ~760 daily runs in minutes, producing 735 failures and 16 simultaneous running tasks.

**Solution:** Merged into a single `olist_pipeline_dag` with `catchup=False` as a direct DAG parameter. Deleted the two old DAGs from Airflow's metadata with `airflow dags delete`.

### 4. Raw table drops blocked by dependent dbt views

**Problem:** `pandas.to_sql(if_exists='replace')` tries to `DROP TABLE olist_raw.customers`, but the dbt staging views (like `stg_customers`) reference those raw tables. PostgreSQL returns: `cannot drop table because other objects depend on it`. First run worked (no views yet), but every subsequent run failed.

**Solution:** Added `DROP SCHEMA IF EXISTS olist_staging CASCADE` to the `create_raw_schema` task. This drops all staging views before reloading raw tables. Since `dbt_run` rebuilds everything later in the pipeline, the data is always restored. Fully idempotent.

### 5. ExternalTaskSensor stuck in "up for reschedule"

**Problem:** The two-DAG design used `ExternalTaskSensor` to wait for the ingestion DAG to complete before running dbt. But execution date matching between DAGs is notoriously flaky — the sensor never found a matching successful run and sat in "reschedule" mode indefinitely.

**Solution:** Eliminated the problem entirely by merging into a single DAG. Tasks within one DAG use a simple dependency chain (`>>`) — no sensor needed.

### 6. UTF-8 BOM in product category translation CSV

**Problem:** `product_category_name_translation.csv` has a UTF-8 BOM (byte order mark) on the first column, causing the column name to appear as `\ufeffproduct_category_name` instead of `product_category_name`.

**Solution:** pandas `read_csv` strips the BOM automatically. The staging model references the clean column name, and `dim_products` uses `COALESCE` to fall back to the Portuguese name if no English translation is found.

### 7. Raw data typos in column names

**Problem:** The Olist `products` table has typos: `product_name_lenght` and `product_description_lenght` (missing the 'h' in 'length').

**Solution:** `stg_products` renames them to `product_name_length` and `product_description_length`. All downstream models reference the corrected names.

### 8. DBeaver SQL editor connected to wrong database

**Problem:** Queries against `olist_staging` tables returned "relation does not exist" even though the Database Navigator showed the tables. The SQL editor tab was connected to a different PostgreSQL instance (port 5433) rather than the pipeline's database (port 5434).

**Solution:** Opened a new SQL editor from the correct connection: right-click the connection in Database Navigator → SQL Editor → Open SQL Script.

