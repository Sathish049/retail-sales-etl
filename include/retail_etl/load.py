"""
Warehouse load logic (Postgres).

Every load is IDEMPOTENT: re-running the same day produces the same result.
  - dimensions use UPSERT (INSERT ... ON CONFLICT DO UPDATE)
  - the fact table uses DELETE + INSERT for the run date, inside ONE transaction

That is what makes Airflow retries, "Clear task" and backfills safe.

Only plain DB-API calls (cursor.execute / executemany) are used, so this works
whether PostgresHook hands back a psycopg2 or a psycopg 3 connection.
"""
from __future__ import annotations

import pandas as pd

DIM_PRODUCT_SQL = """
INSERT INTO dim_product (product_id, product_name, category, list_price, updated_at)
VALUES (%s, %s, %s, %s, NOW())
ON CONFLICT (product_id) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    category     = EXCLUDED.category,
    list_price   = EXCLUDED.list_price,
    updated_at   = NOW();
"""

DIM_STORE_SQL = """
INSERT INTO dim_store (store_id, city, state, region, updated_at)
VALUES (%s, %s, %s, %s, NOW())
ON CONFLICT (store_id) DO UPDATE SET
    city       = EXCLUDED.city,
    state      = EXCLUDED.state,
    region     = EXCLUDED.region,
    updated_at = NOW();
"""

FACT_INSERT_SQL = """
INSERT INTO fact_sales (
    transaction_id, transaction_ts, sale_date, store_id, product_id, customer_id,
    quantity, unit_price, discount_pct, gross_amount, discount_amount, net_amount,
    payment_method
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""


def _rows(df: pd.DataFrame, columns: list[str]) -> list[tuple]:
    """Convert a DataFrame to plain Python tuples, turning NaN/NA into SQL NULL."""
    clean = df[columns].astype(object).where(df[columns].notna(), None)
    return [tuple(r) for r in clean.itertuples(index=False, name=None)]


def upsert_dimensions(conn, products: pd.DataFrame, stores: pd.DataFrame) -> dict:
    try:
        with conn.cursor() as cur:
            cur.executemany(DIM_PRODUCT_SQL,
                            _rows(products, ["product_id", "product_name", "category", "list_price"]))
            cur.executemany(DIM_STORE_SQL,
                            _rows(stores, ["store_id", "city", "state", "region"]))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"products": len(products), "stores": len(stores)}


def replace_fact_partition(conn, clean: pd.DataFrame, run_date: str) -> int:
    """Delete then insert one day of facts atomically."""
    from retail_etl.transform import FACT_COLUMNS

    rows = _rows(clean, FACT_COLUMNS)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM fact_sales WHERE sale_date = %s;", (run_date,))
            if rows:
                cur.executemany(FACT_INSERT_SQL, rows)
        conn.commit()
    except Exception:
        conn.rollback()   # leave the previous good data untouched
        raise
    return len(rows)


def log_run(conn, metrics: dict, dag_run_id: str, status: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_run_log
                (run_date, dag_run_id, rows_raw, duplicates, rows_clean, rows_rejected,
                 reject_rate, net_revenue, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                metrics["run_date"], dag_run_id, metrics["rows_raw"], metrics["duplicates"],
                metrics["rows_clean"], metrics["rows_rejected"], metrics["reject_rate"],
                metrics["net_revenue"], status,
            ),
        )
    conn.commit()
