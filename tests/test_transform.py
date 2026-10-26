"""Unit tests for the pandas business logic (no Airflow, no database)."""
import pandas as pd
import pytest

from retail_etl.transform import FACT_COLUMNS, clean_sales, validate_schema

RUN_DATE = "2026-10-05"
PRODUCTS = pd.DataFrame({"product_id": ["P001", "P002"], "product_name": ["A", "B"],
                         "category": ["Grocery", "Home"], "list_price": [2.5, 40.0]})
STORES = pd.DataFrame({"store_id": ["S01"], "city": ["Flagstaff"], "state": ["AZ"],
                       "region": ["Southwest"]})


def row(tid, **kw):
    base = {"transaction_id": tid, "transaction_ts": f"{RUN_DATE} 10:00:00", "store_id": "S01",
            "product_id": "P001", "customer_id": "C00001", "quantity": "2",
            "unit_price": "2.50", "discount_pct": "0", "payment_method": "card"}
    base.update(kw)
    return base


def run(rows):
    return clean_sales(pd.DataFrame(rows).astype(str), PRODUCTS, STORES, RUN_DATE)


def test_good_row_passes_and_amounts_are_computed():
    clean, rejected, m = run([row("T1", quantity="4", unit_price="10", discount_pct="0.1")])
    assert list(clean.columns) == FACT_COLUMNS
    assert rejected.empty
    r = clean.iloc[0]
    assert (r.gross_amount, r.discount_amount, r.net_amount) == (40.0, 4.0, 36.0)
    assert m["rows_clean"] == 1 and m["net_revenue"] == 36.0


def test_messy_ids_are_standardised():
    clean, _, _ = run([row("T1", store_id=" s01 ", product_id="p002", payment_method=" CARD")])
    assert clean.loc[0, ["store_id", "product_id", "payment_method"]].tolist() == ["S01", "P002", "card"]


def test_duplicates_are_dropped():
    clean, _, m = run([row("T1"), row("T1"), row("T2")])
    assert len(clean) == 2 and m["duplicates"] == 1


@pytest.mark.parametrize("override, reason", [
    ({"quantity": "-1"}, "invalid_quantity"),
    ({"unit_price": ""}, "invalid_price"),
    ({"transaction_ts": "not-a-timestamp"}, "invalid_timestamp"),
    ({"product_id": "P999"}, "unknown_product"),
    ({"store_id": "S99"}, "unknown_store"),
    ({"discount_pct": "1.5"}, "invalid_discount"),
    ({"transaction_ts": "2026-10-04 23:59:00"}, "wrong_business_date"),
])
def test_bad_rows_are_rejected_with_reason(override, reason):
    clean, rejected, m = run([row("T1"), row("T2", **override)])
    assert len(clean) == 1
    assert rejected["reject_reason"].tolist() == [reason]
    assert m["reject_reasons"] == {reason: 1}


def test_guest_checkout_is_kept_as_null_customer():
    clean, rejected, _ = run([row("T1", customer_id="")])
    assert rejected.empty and pd.isna(clean.loc[0, "customer_id"])


def test_schema_drift_fails_fast():
    with pytest.raises(ValueError, match="missing columns"):
        validate_schema(pd.DataFrame({"transaction_id": ["T1"]}))
