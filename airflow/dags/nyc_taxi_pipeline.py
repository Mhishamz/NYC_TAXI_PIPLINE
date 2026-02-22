"""
NYC Taxi Data Pipeline DAG
===========================
Orchestrates the full ELT pipeline:
  1. Ingest raw NYC taxi data from public API → bronze (PostgreSQL)
  2. Trigger dbt transformations: bronze → silver → gold (Medallion Architecture)
  3. Validate data quality via dbt tests
  4. Log pipeline metadata for observability

Schedule: Daily at 02:00 UTC
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# ─────────────────────────────────────────────
# Logger
# ─────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# DAG Default Arguments
# ─────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@yourcompany.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
NYC_TAXI_URL = os.getenv(
    "NYC_TAXI_URL",
    "https://data.cityofnewyork.us/resource/qp3b-zxtp.json"
)
DW_CONN_ID = "postgres_dw"
PAGE_SIZE   = 1000
MAX_PAGES   = 10  # cap at 10k records per run (adjustable)

# ─────────────────────────────────────────────
# Task Functions
# ─────────────────────────────────────────────

def log_pipeline_start(dag_id: str, task_id: str, execution_date: str, **kwargs) -> int:
    """Register a pipeline run in metadata.pipeline_runs and return run_id."""
    logger.info(f"[METADATA] Starting pipeline run: dag={dag_id}, task={task_id}")
    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO metadata.pipeline_runs
                    (dag_id, task_id, execution_date, status, started_at)
                VALUES (%s, %s, %s, 'running', NOW())
                RETURNING run_id
                """,
                (dag_id, task_id, execution_date),
            )
            run_id = cur.fetchone()[0]
            conn.commit()
    logger.info(f"[METADATA] Pipeline run registered with run_id={run_id}")
    kwargs["ti"].xcom_push(key="pipeline_run_id", value=run_id)
    return run_id


def ingest_nyc_taxi_data(**kwargs) -> dict:
    """
    Fetch NYC Yellow Taxi trip data from the Socrata API
    and load raw records into bronze.raw_taxi_trips.

    Returns summary dict with rows_ingested count.
    """
    import requests
    import pandas as pd
    import uuid
    from datetime import timezone

    ti = kwargs["ti"]
    execution_date = kwargs.get("ds", datetime.utcnow().strftime("%Y-%m-%d"))
    run_id = ti.xcom_pull(key="pipeline_run_id", task_ids="log_pipeline_start")
    batch_id = f"batch_{execution_date}_{str(uuid.uuid4())[:8]}"

    logger.info(f"[INGEST] Starting ingestion | batch_id={batch_id} | run_id={run_id}")

    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    total_rows = 0

    for page in range(MAX_PAGES):
        offset = page * PAGE_SIZE
        params = {
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$order": "tpep_pickup_datetime DESC",
        }

        logger.info(f"[INGEST] Fetching page {page + 1}/{MAX_PAGES} | offset={offset}")
        try:
            response = requests.get(NYC_TAXI_URL, params=params, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"[INGEST] HTTP error on page {page}: {e}")
            raise

        records = response.json()
        if not records:
            logger.info(f"[INGEST] No more records at offset={offset}. Stopping.")
            break

        df = pd.DataFrame(records)
        logger.info(f"[INGEST] Page {page + 1}: {len(df)} records retrieved")

        # Normalize column names
        column_map = {
            "VendorID":            "vendor_id",
            "tpep_pickup_datetime": "tpep_pickup_datetime",
            "tpep_dropoff_datetime":"tpep_dropoff_datetime",
            "passenger_count":     "passenger_count",
            "trip_distance":       "trip_distance",
            "RatecodeID":          "rate_code_id",
            "store_and_fwd_flag":  "store_and_fwd_flag",
            "PULocationID":        "pu_location_id",
            "DOLocationID":        "do_location_id",
            "payment_type":        "payment_type",
            "fare_amount":         "fare_amount",
            "extra":               "extra",
            "mta_tax":             "mta_tax",
            "tip_amount":          "tip_amount",
            "tolls_amount":        "tolls_amount",
            "improvement_surcharge":"improvement_surcharge",
            "total_amount":        "total_amount",
            "congestion_surcharge":"congestion_surcharge",
            "airport_fee":         "airport_fee",
        }
        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

        # Add audit columns
        df["source_url"]       = NYC_TAXI_URL
        df["batch_id"]         = batch_id
        df["pipeline_run_id"]  = run_id
        df["ingested_at"]      = datetime.now(timezone.utc).isoformat()

        # Select only known columns, fill missing with None
        expected_cols = list(column_map.values()) + ["source_url", "batch_id", "pipeline_run_id", "ingested_at"]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None
        df = df[expected_cols]

        # Bulk insert
        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
        insert_sql = f"""
            INSERT INTO bronze.raw_taxi_trips (
                {', '.join(expected_cols)}
            ) VALUES %s
        """
        from psycopg2.extras import execute_values
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, rows, page_size=500)
            conn.commit()

        total_rows += len(df)
        logger.info(f"[INGEST] Inserted {len(df)} rows | cumulative={total_rows}")

        if len(records) < PAGE_SIZE:
            logger.info("[INGEST] Last page reached (partial page).")
            break

    logger.info(f"[INGEST] Ingestion complete | total_rows={total_rows} | batch_id={batch_id}")

    # Update metadata
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE metadata.pipeline_runs
                SET rows_ingested = %s, status = 'ingested'
                WHERE run_id = %s
                """,
                (total_rows, run_id),
            )
        conn.commit()

    ti.xcom_push(key="rows_ingested", value=total_rows)
    ti.xcom_push(key="batch_id", value=batch_id)
    return {"rows_ingested": total_rows, "batch_id": batch_id}


def log_pipeline_end(status: str, **kwargs) -> None:
    """Update pipeline run metadata on success or failure."""
    ti = kwargs["ti"]
    run_id = ti.xcom_pull(key="pipeline_run_id", task_ids="log_pipeline_start")
    rows_ingested = ti.xcom_pull(key="rows_ingested", task_ids="ingest_data") or 0

    logger.info(f"[METADATA] Closing run_id={run_id} with status={status}")
    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE metadata.pipeline_runs
                SET status       = %s,
                    rows_ingested = %s,
                    completed_at = NOW(),
                    duration_secs = EXTRACT(EPOCH FROM (NOW() - started_at))
                WHERE run_id = %s
                """,
                (status, rows_ingested, run_id),
            )
        conn.commit()
    logger.info(f"[METADATA] Run {run_id} marked as {status}")


def validate_bronze_data(**kwargs) -> None:
    """Basic data quality checks on the latest bronze batch."""
    ti = kwargs["ti"]
    batch_id = ti.xcom_pull(key="batch_id", task_ids="ingest_data")

    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:

            # Check 1: Row count
            cur.execute(
                "SELECT COUNT(*) FROM bronze.raw_taxi_trips WHERE batch_id = %s",
                (batch_id,),
            )
            count = cur.fetchone()[0]
            logger.info(f"[VALIDATE] Batch {batch_id}: {count} rows ingested")
            if count == 0:
                raise ValueError(f"[VALIDATE] FAIL: No rows in batch {batch_id}")

            # Check 2: Null critical fields
            cur.execute(
                """
                SELECT COUNT(*) FROM bronze.raw_taxi_trips
                WHERE batch_id = %s
                  AND (tpep_pickup_datetime IS NULL OR total_amount IS NULL)
                """,
                (batch_id,),
            )
            null_count = cur.fetchone()[0]
            logger.info(f"[VALIDATE] Null critical fields: {null_count}")
            if null_count > count * 0.10:
                raise ValueError(
                    f"[VALIDATE] FAIL: >10% null critical fields ({null_count}/{count})"
                )

    logger.info("[VALIDATE] Bronze validation passed ✓")


# ─────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────
with DAG(
    dag_id="nyc_taxi_pipeline",
    description="NYC Taxi ELT Pipeline: Ingest → Bronze → Silver → Gold via dbt",
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["nyc-taxi", "elt", "medallion", "dbt", "production"],
    doc_md=__doc__,
) as dag:

    # ── 1. Log start ──
    log_start = PythonOperator(
        task_id="log_pipeline_start",
        python_callable=log_pipeline_start,
        op_kwargs={
            "dag_id": "nyc_taxi_pipeline",
            "task_id": "full_pipeline",
            "execution_date": "{{ ds }}",
        },
    )

    # ── 2. Ingest raw data → bronze ──
    ingest_data = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_nyc_taxi_data,
    )

    # ── 3. Validate bronze data ──
    validate_bronze = PythonOperator(
        task_id="validate_bronze",
        python_callable=validate_bronze_data,
    )

    # ── 4. dbt: bronze → silver (clean & type) ──
    dbt_silver = BashOperator(
        task_id="dbt_transform_silver",
        bash_command="""
            docker exec dbt dbt run \
                --profiles-dir /dbt \
                --project-dir /dbt \
                --select silver \
                --log-format json 2>&1 | tee /opt/airflow/logs/dbt_silver_{{ ds }}.log
        """,
    )

    # ── 5. dbt: silver → gold (aggregate) ──
    dbt_gold = BashOperator(
        task_id="dbt_transform_gold",
        bash_command="""
            docker exec dbt dbt run \
                --profiles-dir /dbt \
                --project-dir /dbt \
                --select gold \
                --log-format json 2>&1 | tee /opt/airflow/logs/dbt_gold_{{ ds }}.log
        """,
    )

    # ── 6. dbt tests ──
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
            docker exec dbt dbt test \
                --profiles-dir /dbt \
                --project-dir /dbt \
                --log-format json 2>&1 | tee /opt/airflow/logs/dbt_test_{{ ds }}.log
        """,
    )

    # ── 7. Generate dbt docs ──
    dbt_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command="""
            docker exec dbt dbt docs generate \
                --profiles-dir /dbt \
                --project-dir /dbt \
                --log-format json 2>&1 | tee /opt/airflow/logs/dbt_docs_{{ ds }}.log
        """,
        trigger_rule="all_done",
    )

    # ── 8. Log success ──
    log_success = PythonOperator(
        task_id="log_success",
        python_callable=log_pipeline_end,
        op_kwargs={"status": "success"},
        trigger_rule="all_success",
    )

    # ── 9. Log failure (runs even if upstream failed) ──
    log_failure = PythonOperator(
        task_id="log_failure",
        python_callable=log_pipeline_end,
        op_kwargs={"status": "failed"},
        trigger_rule="one_failed",
    )

    # ─────────────────────────────────────────────
    # DAG Dependency Graph
    # ─────────────────────────────────────────────
    log_start >> ingest_data >> validate_bronze >> dbt_silver >> dbt_gold >> dbt_test >> dbt_docs
    dbt_docs >> [log_success, log_failure]