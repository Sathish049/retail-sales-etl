# Retail Sales ETL Pipeline

**Python · pandas · Apache Airflow 3 · PostgreSQL · SQL · Docker**

A daily batch pipeline that picks up raw point-of-sale (POS) CSV exports, cleans
and validates them with pandas, and loads them into a PostgreSQL star schema.
Apache Airflow orchestrates the whole flow, and everything runs locally in
Docker Compose.

---

## 1. The business problem

A retail chain has 8 stores. Every night each store's POS system exports the
day's transactions to a file named `sales_YYYY-MM-DD.csv`. The raw data is messy:

| Problem in the source            | How the pipeline handles it                     |
|----------------------------------|-------------------------------------------------|
| Duplicate rows (POS resends)     | Dropped by `transaction_id`, then counted       |
| `" s01 "`, `p002`, `CARD`        | Trimmed and case-standardised                   |
| Negative quantity, missing price | Rejected with a reason                          |
| Unparseable timestamp            | Rejected (`invalid_timestamp`)                  |
| Product/store not in master data | Rejected (`unknown_product` / `unknown_store`)  |
| Transaction from another day     | Rejected (`wrong_business_date`)                |
| Guest checkout (no customer)     | Kept, stored as `NULL`                          |

Analysts need clean, trusted tables for daily revenue by store and category.

---

## 2. Architecture

```
 data/raw/sales_2026-10-05.csv          Airflow (Docker, LocalExecutor)                     Postgres "retail_dw"
 data/raw/products.csv        ──▶  sensor ─▶ extract ─▶ transform ─▶ quality branch ─▶  dim_store, dim_product
 data/raw/stores.csv                                       │               │            fact_sales
                                                           ▼               ▼            agg_daily_sales
                                              data/rejected/*.csv   data/quarantine/    etl_run_log
                                                                                              │
                                                                       Asset update ──▶ retail_sales_report DAG
                                                                                         └▶ data/reports/*.csv
```

### DAG 1: `retail_sales_etl` (daily)

```
wait_for_sales_file ─▶ create_tables ─▶ extract ─▶ transform ─▶ check_quality
                                                                  ├─▶ load_dimensions ─▶ load_fact_sales ─▶ build_daily_summary ─┐
                                                                  └─▶ quarantine_batch ─────────────────────────────────────────┴─▶ log_run
```

| Task                  | What it does                                                    | Airflow concept you practise                 |
|-----------------------|-----------------------------------------------------------------|----------------------------------------------|
| `wait_for_sales_file` | Waits for the day's file                                        | `@task.sensor`, `mode="reschedule"`, timeout |
| `create_tables`       | Runs `sql/schema.sql` (`IF NOT EXISTS`)                          | `SQLExecuteQueryOperator`, Connections       |
| `extract`             | Copies raw file into `data/staging/<ds>/`                        | TaskFlow `@task`, `{{ ds }}` from context     |
| `transform`           | pandas cleaning, writes clean and rejected CSVs                  | XCom: passes **paths + metrics**, not data   |
| `check_quality`       | Reject rate above threshold → quarantine, else load              | `@task.branch`, Variables                    |
| `load_dimensions`     | UPSERT stores and products                                       | `PostgresHook`, idempotent load              |
| `load_fact_sales`     | DELETE + INSERT the day in one transaction                       | Idempotency, atomic writes                   |
| `build_daily_summary` | Templated SQL aggregate; marks an Asset as updated               | Jinja templating, `outlets=[Asset]`          |
| `quarantine_batch`    | Saves the bad batch for a human to inspect                       | Alternate branch path                        |
| `log_run`             | Writes run metrics + status to `etl_run_log`                     | `trigger_rule="none_failed_min_one_success"` |

DAG-level settings: `schedule="@daily"`, `catchup=True` (automatic backfill
from Oct 1), `max_active_runs=1`, and `retries=2` with exponential backoff.
Every task also has an `execution_timeout` and an `on_failure_callback`.

### DAG 2: `retail_sales_report` (data-aware)

It has no cron schedule. It runs whenever DAG 1 updates the
`agg_daily_sales` Asset, and writes a 7-day revenue report CSV.

### Warehouse model (star schema)

```
dim_store (store_id PK) ─┐
                         ├──< fact_sales (transaction_id PK, sale_date, quantity, net_amount, ...)
dim_product (product_id PK) ┘
agg_daily_sales (sale_date, store_id, category)   ← pre-aggregated for dashboards
etl_run_log                                       ← one row per run: counts, reject rate, status
```

---

## 3. Project layout

```
retail-sales-etl/
├── dags/
│   ├── retail_sales_etl.py        # main DAG (orchestration only)
│   ├── retail_sales_report.py     # Asset-triggered DAG
│   └── sql/
│       ├── schema.sql             # star schema DDL
│       └── daily_summary.sql      # templated aggregate ({{ ds }})
├── include/retail_etl/            # business logic, NO Airflow imports at module level
│   ├── config.py                  # paths, conn id, asset URI
│   ├── transform.py               # pandas cleaning + validation rules
│   └── load.py                    # idempotent Postgres loads
├── scripts/generate_data.py       # creates dirty sample data
├── tests/
│   ├── test_transform.py          # unit tests for the cleaning rules
│   └── test_dag_integrity.py      # DAGs import and are wired correctly
├── docker-compose.yaml            # Airflow 3 + two Postgres databases
├── Dockerfile                     # official Airflow image + providers
├── Makefile                       # shortcuts: make data / up / test / psql
└── requirements*.txt
```

**Why split `dags/` and `include/`?** The DAG file is parsed by Airflow every
few seconds, so it should stay light and contain only wiring. All real logic
lives in `include/`, where it can be unit tested with plain pytest. Airflow
doesn't need to be installed for that.

---

## 4. Run it

**Prerequisites:** Docker Desktop (give it at least 4 GB RAM) and Python 3.10+.

```bash
# 1. Settings (on Linux, put your `id -u` in AIRFLOW_UID)
cp .env.example .env

# 2. Generate 14 days of dirty sample data (Oct 1–14, 2026)
python scripts/generate_data.py --start 2026-10-01 --days 14      # or: make data

# 3. Start Airflow (first build takes a few minutes)
docker compose up -d --build                                      # or: make up
docker compose ps                                                 # wait until all are "healthy"/"running"
```

4. Open **http://localhost:8080**. There's no login, because local dev mode
   makes everyone admin.
5. Un-pause `retail_sales_report`, then un-pause `retail_sales_etl`.
6. Because of `catchup=True`, Airflow creates one run per day from Oct 1
   onwards and works through them in order. Watch the **Grid** view.

### Check the results

```bash
make psql        # or: docker compose exec retail-db psql -U retail -d retail_dw
```
```sql
SELECT run_date, rows_raw, duplicates, rows_clean, rows_rejected, reject_rate, status
FROM etl_run_log ORDER BY run_date;

SELECT s.region, p.category, SUM(f.net_amount) AS revenue
FROM fact_sales f
JOIN dim_store s USING (store_id)
JOIN dim_product p USING (product_id)
GROUP BY 1, 2 ORDER BY revenue DESC;
```
You can also connect DBeaver or pgAdmin to `localhost:5433`, user `retail`,
password `retail`, database `retail_dw`.

Files to look at:
* `data/rejected/rejected_<date>.csv`: every bad row and its `reject_reason`
* `data/reports/`: output of the report DAG

### Run the tests

```bash
pip install -r requirements-dev.txt
pytest -q                                              # transform tests (DAG tests skip without Airflow)
docker compose exec airflow-scheduler bash -c "pip install -q pytest && pytest -q /opt/airflow/tests"   # all 15 tests
```

### Stop

```bash
docker compose down        # keep data
docker compose down -v     # wipe both databases
```

---

## 5. Practice exercises (do these in the UI)

Each one shows a concept interviewers often ask about.

1. **Idempotency.** In Grid view, select a finished run → `load_fact_sales` →
   **Clear**. Then check that `SELECT COUNT(*) FROM fact_sales WHERE sale_date = '<that day>'`
   still returns the same number, with no duplicates.
2. **Sensor + late data.** Generate a future day with
   `python scripts/generate_data.py --start 2026-10-15 --days 1 --seed 7` and
   watch the waiting run's sensor succeed on its next poke. Note that in
   reschedule mode, the task shows **up_for_reschedule** between pokes.
3. **Branching.** Go to Admin → Variables and set `max_reject_rate` to `0.02`.
   Re-run a day. You should see `quarantine_batch` run, the load tasks skipped,
   and `log_run` still runs with status `quarantined`. Set it back to `0.10`
   afterwards. (A Variable set in the UI overrides the `AIRFLOW_VAR_*` env var.)
4. **Retries and callbacks.** Run `docker compose stop retail-db`, then trigger
   a run. Watch `create_tables` go into `up_for_retry` and look for the
   `ALERT:` line in its log. Bring the DB back with `docker compose start retail-db`.
5. **Schema drift.** Delete the `quantity` column from one CSV. `transform`
   should fail fast with `Source file is missing columns`.
6. **Backfill from the CLI.**
   `docker compose exec airflow-scheduler airflow backfill create --dag-id retail_sales_etl --from-date 2026-10-01 --to-date 2026-10-03 --reprocess-behavior completed`

---

## 6. Ideas to extend it (next level)

* Swap the CSV staging files for **Parquet**, and the local folder for **S3**
  (with `S3KeySensor` and `S3Hook`). MinIO gives you a free S3 inside Docker.
* Add **dynamic task mapping**: one `transform.expand(store_id=[...])` task per store.
* Replace `daily_summary.sql` with a **dbt** project run through **Astronomer Cosmos**.
* Add **Great Expectations** or **Soda** checks instead of the hand-written rules.
* Send the failure callback to **Slack** with `SlackWebhookHook`.
* Add **GitHub Actions** that run `pytest` on every push (CI for DAGs).
* Add a **Metabase** container on top of `agg_daily_sales` for a dashboard.

---

## 7. Talking points for interviews

* **Why Airflow, not cron?** Airflow gives you dependencies, retries,
  backfills, a UI with logs, and alerting. Cron only gives you a timer.
* **How do you make tasks idempotent?** Each day is a partition. The fact load
  does DELETE + INSERT for that date in one transaction, and the dimensions use
  UPSERT. So retries and backfills can never double count.
* **What goes into XCom?** Only small metadata: file paths and row counts.
  DataFrames stay on disk or in storage, because XComs live in the metadata DB.
* **Why `mode="reschedule"`?** A poke-mode sensor holds a worker slot for the
  whole wait. Reschedule mode releases the slot between checks.
* **Why did `log_run` need a trigger rule?** After a branch, one path is always
  skipped. The default `all_success` rule would then skip `log_run` too.
* **How is the code tested?** The pandas rules are unit tested in isolation,
  and a DagBag test checks that the DAGs import cleanly and are wired correctly.

### Resume bullets

- Built a daily batch ETL pipeline with **Apache Airflow 3**, **Python/pandas** and **PostgreSQL**, containerised with **Docker Compose**. It ingests POS sales files into a star-schema warehouse (fact table, 2 dimensions, aggregate).
- Implemented 8 data-quality rules with reject-reason auditing, deduplication and a **branching quality gate** that quarantines batches above a configurable reject-rate threshold.
- Designed **idempotent loads** (partition DELETE + INSERT, dimension UPSERTs) so retries and **14-day catchup backfills** never duplicate data. Added sensors, retries with exponential backoff, failure callbacks and run-level metrics logging.
- Used **data-aware scheduling (Assets)** to trigger a downstream reporting DAG, and covered the transform logic and DAG structure with **pytest**.
