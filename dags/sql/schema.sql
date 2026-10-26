-- Star schema for the retail warehouse. Safe to run every time (IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS dim_store (
    store_id    VARCHAR(10) PRIMARY KEY,
    city        VARCHAR(100) NOT NULL,
    state       CHAR(2)      NOT NULL,
    region      VARCHAR(50)  NOT NULL,
    updated_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id    VARCHAR(10)   PRIMARY KEY,
    product_name  VARCHAR(200)  NOT NULL,
    category      VARCHAR(50)   NOT NULL,
    list_price    NUMERIC(10,2) NOT NULL,
    updated_at    TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_sales (
    transaction_id   VARCHAR(30)   PRIMARY KEY,
    transaction_ts   TIMESTAMP     NOT NULL,
    sale_date        DATE          NOT NULL,
    store_id         VARCHAR(10)   NOT NULL REFERENCES dim_store(store_id),
    product_id       VARCHAR(10)   NOT NULL REFERENCES dim_product(product_id),
    customer_id      VARCHAR(20),                       -- NULL = guest checkout
    quantity         INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price       NUMERIC(10,2) NOT NULL CHECK (unit_price > 0),
    discount_pct     NUMERIC(4,3)  NOT NULL DEFAULT 0,
    gross_amount     NUMERIC(12,2) NOT NULL,
    discount_amount  NUMERIC(12,2) NOT NULL,
    net_amount       NUMERIC(12,2) NOT NULL,
    payment_method   VARCHAR(20),
    loaded_at        TIMESTAMP     NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_fact_sales_sale_date ON fact_sales (sale_date);

CREATE TABLE IF NOT EXISTS agg_daily_sales (
    sale_date     DATE          NOT NULL,
    store_id      VARCHAR(10)   NOT NULL,
    category      VARCHAR(50)   NOT NULL,
    txn_count     INTEGER       NOT NULL,
    units_sold    INTEGER       NOT NULL,
    net_revenue   NUMERIC(14,2) NOT NULL,
    avg_basket    NUMERIC(12,2) NOT NULL,
    PRIMARY KEY (sale_date, store_id, category)
);

CREATE TABLE IF NOT EXISTS etl_run_log (
    id             SERIAL PRIMARY KEY,
    run_date       DATE         NOT NULL,
    dag_run_id     VARCHAR(250) NOT NULL,
    rows_raw       INTEGER,
    duplicates     INTEGER,
    rows_clean     INTEGER,
    rows_rejected  INTEGER,
    reject_rate    NUMERIC(6,4),
    net_revenue    NUMERIC(14,2),
    status         VARCHAR(20)  NOT NULL,
    logged_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);
