"""
Pure pandas transformation logic.

Nothing in this file imports Airflow, so it can be unit tested with plain pytest.
Keeping business logic out of the DAG file is a core Airflow best practice:
the DAG only *orchestrates*, these functions do the *work*.
"""
from __future__ import annotations

import pandas as pd

EXPECTED_COLUMNS = [
    "transaction_id", "transaction_ts", "store_id", "product_id", "customer_id",
    "quantity", "unit_price", "discount_pct", "payment_method",
]

FACT_COLUMNS = [
    "transaction_id", "transaction_ts", "sale_date", "store_id", "product_id",
    "customer_id", "quantity", "unit_price", "discount_pct", "gross_amount",
    "discount_amount", "net_amount", "payment_method",
]


def validate_schema(df: pd.DataFrame) -> None:
    """Fail fast if the source file is missing columns (schema drift)."""
    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Source file is missing columns: {sorted(missing)}")


def clean_sales(
    raw: pd.DataFrame,
    products: pd.DataFrame,
    stores: pd.DataFrame,
    run_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Clean one day of raw sales.

    Returns (clean_df, rejected_df, metrics).
      clean_df    - rows ready for fact_sales
      rejected_df - bad rows plus a `reject_reason` column (kept for auditing)
      metrics     - small dict of counts (safe to pass through XCom)
    """
    validate_schema(raw)
    df = raw[EXPECTED_COLUMNS].copy()
    rows_raw = len(df)

    # 1. Standardise text fields
    for col in ["transaction_id", "store_id", "product_id", "customer_id", "payment_method"]:
        df[col] = df[col].astype("string").str.strip()
    df["store_id"] = df["store_id"].str.upper()
    df["product_id"] = df["product_id"].str.upper()
    df["payment_method"] = df["payment_method"].str.lower()
    df["customer_id"] = df["customer_id"].replace("", pd.NA)

    # 2. Remove exact duplicate transactions
    before = len(df)
    df = df.drop_duplicates(subset="transaction_id", keep="first")
    duplicates = before - len(df)

    # 3. Type conversion (bad values become NaN/NaT instead of crashing)
    df["transaction_ts"] = pd.to_datetime(df["transaction_ts"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0.0)

    # 4. Validation rules: the first rule a row breaks becomes its reject reason
    known_products = set(products["product_id"].str.strip().str.upper())
    known_stores = set(stores["store_id"].str.strip().str.upper())
    expected_day = pd.Timestamp(run_date).date()

    rules = [
        (df["transaction_id"].isna(), "missing_transaction_id"),
        (df["transaction_ts"].isna(), "invalid_timestamp"),
        (df["quantity"].isna() | (df["quantity"] <= 0), "invalid_quantity"),
        (df["unit_price"].isna() | (df["unit_price"] <= 0), "invalid_price"),
        ((df["discount_pct"] < 0) | (df["discount_pct"] > 0.9), "invalid_discount"),
        (~df["product_id"].isin(known_products).fillna(False), "unknown_product"),
        (~df["store_id"].isin(known_stores).fillna(False), "unknown_store"),
        (df["transaction_ts"].dt.date != expected_day, "wrong_business_date"),
    ]
    reason = pd.Series(pd.NA, index=df.index, dtype="object")
    for mask, label in rules:
        mask = mask.fillna(True).astype(bool)
        reason = reason.mask(reason.isna() & mask, label)

    rejected = raw.loc[reason.dropna().index].copy()
    rejected["reject_reason"] = reason.dropna()
    clean = df.loc[reason.isna()].copy()

    # 5. Derived business columns
    clean["quantity"] = clean["quantity"].astype(int)
    clean["sale_date"] = clean["transaction_ts"].dt.date
    clean["gross_amount"] = (clean["quantity"] * clean["unit_price"]).round(2)
    clean["discount_amount"] = (clean["gross_amount"] * clean["discount_pct"]).round(2)
    clean["net_amount"] = (clean["gross_amount"] - clean["discount_amount"]).round(2)
    clean = clean[FACT_COLUMNS].reset_index(drop=True)

    metrics = {
        "run_date": run_date,
        "rows_raw": int(rows_raw),
        "duplicates": int(duplicates),
        "rows_clean": int(len(clean)),
        "rows_rejected": int(len(rejected)),
        "reject_rate": round(len(rejected) / max(rows_raw - duplicates, 1), 4),
        "net_revenue": float(round(clean["net_amount"].sum(), 2)),
        "reject_reasons": {k: int(v) for k, v in rejected["reject_reason"].value_counts().items()},
    }
    return clean, rejected, metrics


def clean_dimension(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Trim/upper-case the key and drop duplicate keys in a master-data file."""
    out = df.copy()
    out[key] = out[key].astype("string").str.strip().str.upper()
    return out.drop_duplicates(subset=key, keep="last").reset_index(drop=True)
