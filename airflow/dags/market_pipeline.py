from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT_ROOT = "/opt/fast-orderbook"

default_args = {
    "owner": "fast-orderbook",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="fast_orderbook_market_pipeline",
    description="Bronze to Silver to Gold market data pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["fast-orderbook", "market-data"],
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m fast_orderbook.processing.spark"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m fast_orderbook.analytics.runner"
        ),
    )

    gold_to_postgres = BashOperator(
    task_id="gold_to_postgres",
    bash_command=(
        f"cd {PROJECT_ROOT} && "
        "python -m fast_orderbook.serving.postgres"
    ),
)

    bronze_to_silver >> silver_to_gold >> gold_to_postgres
