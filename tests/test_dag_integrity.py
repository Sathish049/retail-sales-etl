"""
DAG integrity tests: every DAG imports without errors and is wired correctly.
Needs Airflow installed, so run it inside the container:
    docker compose exec airflow-scheduler pytest /opt/airflow/tests -q
(or it is skipped automatically on a laptop without Airflow).
"""
from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_etl_dag_structure(dagbag):
    dag = dagbag.dags.get("retail_sales_etl")
    assert dag is not None
    assert dag.max_active_runs == 1
    assert {"wait_for_sales_file", "transform", "check_quality",
            "load_fact_sales", "build_daily_summary", "log_run"} <= set(dag.task_ids)
    branch = dag.get_task("check_quality")
    assert branch.downstream_task_ids == {"load_dimensions", "quarantine_batch"}
    assert dag.get_task("log_run").trigger_rule == "none_failed_min_one_success"


def test_report_dag_is_asset_scheduled(dagbag):
    assert dagbag.dags.get("retail_sales_report") is not None
