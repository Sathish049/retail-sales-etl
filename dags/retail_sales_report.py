"""
Retail Sales Report (data-aware scheduling)
==========================================

No cron schedule: this DAG runs whenever `retail_sales_etl` updates the
agg_daily_sales Asset. It writes a 7-day store/category report to
data/reports/.
"""
from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
from airflow.sdk import Asset, dag, task

from retail_etl import config

log = logging.getLogger(__name__)

REPORT_SQL = """
SELECT s.region, a.store_id, s.city, a.category,
       SUM(a.txn_count)   AS transactions,
       SUM(a.units_sold)  AS units,
       SUM(a.net_revenue) AS revenue
FROM agg_daily_sales a
JOIN dim_store s ON s.store_id = a.store_id
WHERE a.sale_date > (SELECT MAX(sale_date) FROM agg_daily_sales) - INTERVAL '7 days'
GROUP BY s.region, a.store_id, s.city, a.category
ORDER BY revenue DESC;
"""


@dag(
    dag_id="retail_sales_report",
    schedule=[Asset(config.DAILY_SUMMARY_ASSET_URI)],
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["retail", "reporting"],
    doc_md=__doc__,
)
def retail_sales_report():

    @task
    def build_weekly_report(**context) -> str:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=config.WAREHOUSE_CONN_ID)
        rows = hook.get_records(REPORT_SQL)
        df = pd.DataFrame(rows, columns=["region", "store_id", "city", "category",
                                         "transactions", "units", "revenue"])

        config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        path = config.REPORTS_DIR / f"weekly_sales_report_{stamp}.csv"
        df.to_csv(path, index=False)
        log.info("Wrote %s rows to %s", len(df), path)
        if not df.empty:
            log.info("Top store/category:\n%s", df.head(5).to_string(index=False))
        return str(path)

    build_weekly_report()


retail_sales_report()
