r"""
Retail Sales ETL Pipeline
=========================

Every day a POS export `sales_YYYY-MM-DD.csv` lands in data/raw/.
This DAG waits for it, cleans it with pandas, loads it into a Postgres star
schema, builds a daily aggregate, and records run metrics.

    wait_for_sales_file
          |
    create_tables
          |
    extract  ->  transform  ->  check_quality (branch)
                                   |                \
                          load_dimensions      quarantine_batch
                                   |                 |
                          load_fact_sales            |
                                   |                 |
                          build_daily_summary        |
                          (produces an Asset)        |
                                    \               /
                                     log_run (runs on either path)

Airflow concepts practised here:
  TaskFlow API, sensors (reschedule mode), SQLExecuteQueryOperator,
  Jinja templating ({{ ds }}), XComs (small dicts only), Connections, Variables,
  branching, trigger rules, retries + callbacks, catchup/backfill, Assets.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Asset, Variable, dag, task

from retail_etl import config
from retail_etl.transform import clean_dimension, clean_sales

log = logging.getLogger(__name__)
DAILY_SUMMARY = Asset(config.DAILY_SUMMARY_ASSET_URI)


def notify_failure(context):
    """on_failure_callback: swap the log line for Slack/email in a real job."""
    ti = context["ti"]
    log.error("ALERT: task %s failed for run %s (try %s)",
              ti.task_id, context["dag_run"].run_id, ti.try_number)


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="retail_sales_etl",
    description="Daily retail sales: CSV -> pandas -> Postgres star schema",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=True,                 # one run per missed day = automatic backfill
    max_active_runs=1,            # process days in order, one at a time
    default_args=default_args,
    tags=["retail", "etl", "postgres"],
    doc_md=__doc__,
)
def retail_sales_etl():

    # 1. SENSOR: wait for today's file. reschedule mode frees the worker slot
    #    between checks instead of sleeping on it (poke mode).
    @task.sensor(poke_interval=60, timeout=60 * 60 * 6, mode="reschedule")
    def wait_for_sales_file(**context) -> bool:
        path = config.RAW_DIR / f"sales_{context['ds']}.csv"
        log.info("Looking for %s", path)
        return path.exists()

    # 2. SQL OPERATOR: idempotent DDL, read from dags/sql/schema.sql
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=config.WAREHOUSE_CONN_ID,
        sql="sql/schema.sql",
    )

    # 3. EXTRACT: copy the raw file into a per-run staging folder.
    #    Return a *path*, not the DataFrame: XComs live in the metadata DB and
    #    should stay small.
    @task
    def extract(**context) -> dict:
        ds = context["ds"]
        df = pd.read_csv(config.RAW_DIR / f"sales_{ds}.csv", dtype=str, keep_default_na=False)
        stage = config.STAGING_DIR / ds
        stage.mkdir(parents=True, exist_ok=True)
        raw_path = stage / "sales_raw.csv"
        df.to_csv(raw_path, index=False)
        log.info("Extracted %s rows to %s", len(df), raw_path)
        return {"raw_path": str(raw_path), "rows": len(df)}

    # 4. TRANSFORM: all business rules live in include/retail_etl/transform.py
    @task
    def transform(extracted: dict, **context) -> dict:
        ds = context["ds"]
        raw = pd.read_csv(extracted["raw_path"], dtype=str, keep_default_na=False)
        products = pd.read_csv(config.RAW_DIR / "products.csv")
        stores = pd.read_csv(config.RAW_DIR / "stores.csv")

        clean, rejected, metrics = clean_sales(raw, products, stores, run_date=ds)

        stage = config.STAGING_DIR / ds
        clean_path = stage / "sales_clean.csv"
        clean.to_csv(clean_path, index=False)

        config.REJECTED_DIR.mkdir(parents=True, exist_ok=True)
        rejected_path = config.REJECTED_DIR / f"rejected_{ds}.csv"
        rejected.to_csv(rejected_path, index=False)

        log.info("Transform metrics: %s", json.dumps(metrics))
        return {**metrics, "clean_path": str(clean_path), "rejected_path": str(rejected_path)}

    # 5. BRANCH: decide the path based on data quality.
    #    The threshold is an Airflow Variable, so you can change it in the UI
    #    (Admin -> Variables) without touching code.
    @task.branch
    def check_quality(metrics: dict) -> str:
        max_rate = float(Variable.get("max_reject_rate", default=config.DEFAULT_MAX_REJECT_RATE))
        if metrics["rows_clean"] == 0 or metrics["reject_rate"] > max_rate:
            log.warning("Reject rate %.2f%% > %.2f%%, quarantining",
                        metrics["reject_rate"] * 100, max_rate * 100)
            return "quarantine_batch"
        return "load_dimensions"

    # 6a. LOAD dimensions (UPSERT) using the Postgres Hook + Connection
    @task
    def load_dimensions() -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from retail_etl.load import upsert_dimensions

        products = clean_dimension(pd.read_csv(config.RAW_DIR / "products.csv"), "product_id")
        stores = clean_dimension(pd.read_csv(config.RAW_DIR / "stores.csv"), "store_id")
        conn = PostgresHook(postgres_conn_id=config.WAREHOUSE_CONN_ID).get_conn()
        try:
            return upsert_dimensions(conn, products, stores)
        finally:
            conn.close()

    # 6b. LOAD facts (DELETE + INSERT for the day, one transaction)
    @task
    def load_fact_sales(metrics: dict, **context) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from retail_etl.load import replace_fact_partition

        clean = pd.read_csv(metrics["clean_path"])
        conn = PostgresHook(postgres_conn_id=config.WAREHOUSE_CONN_ID).get_conn()
        try:
            n = replace_fact_partition(conn, clean, context["ds"])
        finally:
            conn.close()
        log.info("Loaded %s fact rows for %s", n, context["ds"])
        return n

    # 7. AGGREGATE with templated SQL. `outlets` marks the Asset as updated,
    #    which triggers the retail_sales_report DAG.
    build_daily_summary = SQLExecuteQueryOperator(
        task_id="build_daily_summary",
        conn_id=config.WAREHOUSE_CONN_ID,
        sql="sql/daily_summary.sql",
        outlets=[DAILY_SUMMARY],
    )

    # 6c. QUARANTINE path: keep the bad batch for a human to inspect
    @task
    def quarantine_batch(metrics: dict, **context) -> str:
        import shutil

        config.QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        target = config.QUARANTINE_DIR / f"sales_{context['ds']}.csv"
        shutil.copy(config.RAW_DIR / f"sales_{context['ds']}.csv", target)
        log.warning("Batch quarantined at %s. Reasons: %s", target, metrics["reject_reasons"])
        return str(target)

    # 8. LOG RUN: trigger rule lets this run after EITHER branch
    #    (the default all_success would skip it because one branch is skipped).
    @task(trigger_rule="none_failed_min_one_success")
    def log_run(metrics: dict, **context) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from retail_etl.load import log_run as write_log

        # A skipped task pushes no XCom, so this tells us which branch ran.
        rows_loaded = context["ti"].xcom_pull(task_ids="load_fact_sales")
        status = "loaded" if rows_loaded is not None else "quarantined"
        conn = PostgresHook(postgres_conn_id=config.WAREHOUSE_CONN_ID).get_conn()
        try:
            write_log(conn, metrics, context["dag_run"].run_id, status)
        finally:
            conn.close()

    # ---- wiring -------------------------------------------------------------
    extracted = extract()
    metrics = transform(extracted)
    decision = check_quality(metrics)

    dims = load_dimensions()
    facts = load_fact_sales(metrics)
    quarantined = quarantine_batch(metrics)
    done = log_run(metrics)

    wait_for_sales_file() >> create_tables >> extracted
    decision >> [dims, quarantined]
    dims >> facts >> build_daily_summary >> done
    quarantined >> done


retail_sales_etl()
