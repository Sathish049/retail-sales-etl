"""Shared settings used by both DAGs."""
import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("RETAIL_DATA_DIR", "/opt/airflow/data"))
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
REJECTED_DIR = DATA_DIR / "rejected"
QUARANTINE_DIR = DATA_DIR / "quarantine"
REPORTS_DIR = DATA_DIR / "reports"

WAREHOUSE_CONN_ID = "retail_dw"          # set by AIRFLOW_CONN_RETAIL_DW in docker-compose
DEFAULT_MAX_REJECT_RATE = 0.10           # override with Airflow Variable `max_reject_rate`

# Data-aware scheduling: the ETL DAG *produces* this asset, the report DAG *consumes* it.
DAILY_SUMMARY_ASSET_URI = "postgres://retail-db:5432/retail_dw/public/agg_daily_sales"
