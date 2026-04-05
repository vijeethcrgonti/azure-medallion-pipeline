"""
medallion_dag.py
Airflow DAG orchestrating the full Bronze → Silver → Gold pipeline.
Runs daily at 02:00 UTC. Supports backfill via --conf date override.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["de-alerts@example.com"],
}

with DAG(
    dag_id="medallion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Bronze → Silver → Gold medallion pipeline for retail orders",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["medallion", "orders", "delta-lake"],
    params={"date": "{{ ds }}"},
) as dag:

    # ── INGESTION ──────────────────────────────────────────────────────────────

    ingest_orders = BashOperator(
        task_id="ingest_orders",
        bash_command=(
            "python /opt/airflow/dags/ingestion/ingest_orders.py "
            "--env {{ var.value.ENV }} "
            "--date {{ params.date }}"
        ),
    )

    ingest_products = BashOperator(
        task_id="ingest_products",
        bash_command="python /opt/airflow/dags/ingestion/ingest_products_api.py",
    )

    # ── BRONZE ─────────────────────────────────────────────────────────────────

    write_bronze_orders = BashOperator(
        task_id="write_bronze_orders",
        bash_command=(
            "python /opt/airflow/dags/bronze/write_bronze.py "
            "--source orders --date {{ params.date }}"
        ),
    )

    # ── SILVER ─────────────────────────────────────────────────────────────────

    clean_silver_orders = BashOperator(
        task_id="clean_silver_orders",
        bash_command=(
            "python /opt/airflow/dags/silver/clean_orders.py "
            "--date {{ params.date }}"
        ),
    )

    data_quality_check = BashOperator(
        task_id="data_quality_check",
        bash_command=(
            "python /opt/airflow/dags/silver/data_quality.py "
            "--layer silver --date {{ params.date }}"
        ),
    )

    # ── GOLD ───────────────────────────────────────────────────────────────────

    gold_revenue = BashOperator(
        task_id="gold_revenue_by_region",
        bash_command=(
            "python /opt/airflow/dags/gold/revenue_by_region.py "
            "--date {{ params.date }}"
        ),
    )

    gold_dbt = BashOperator(
        task_id="dbt_gold_models",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --select gold --vars '{\"run_date\": \"{{ params.date }}\"}' && "
            "dbt test --select gold"
        ),
    )

    # ── DEPENDENCIES ───────────────────────────────────────────────────────────

    [ingest_orders, ingest_products] >> write_bronze_orders
    write_bronze_orders >> clean_silver_orders
    clean_silver_orders >> data_quality_check
    data_quality_check >> [gold_revenue, gold_dbt]
