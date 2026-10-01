from datetime import datetime, timedelta, timezone

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.sensors.external_task import (
        ExternalTaskSensor,
    )
    from airflow.sdk import DAG
except ImportError:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="gold_realestate_aggregate",
    description="Aggregate Silver real-estate data and load PostgreSQL",
    start_date=datetime(
        2026,
        8,
        1,
        tzinfo=timezone.utc,
    ),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "parkjiyeon",
        "retries": 0,
    },
    tags=[
        "q3",
        "realestate",
        "gold",
        "spark",
        "postgresql",
    ],
) as dag:

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="silver_realestate_transform",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        mode="reschedule",
        poke_interval=10,
        timeout=600,
    )

    run_gold_spark = BashOperator(
        task_id="run_gold_spark",
        bash_command=r"""
set -euo pipefail

/home/airflow/.local/bin/spark-submit \
  --master "local[*]" \
  --name gold_realestate_aggregate \
  --repositories https://repo.maven.apache.org/maven2 \
  --packages org.postgresql:postgresql:42.7.13 \
  --conf spark.jars.ivy=/tmp/q3_ivy \
  --conf spark.driver.bindAddress=0.0.0.0 \
  /opt/airflow/scripts/q3/gold_spark_sql.py \
  --bucket realestate-parkjiyeon \
  --region ap-southeast-2 \
  --jdbc-url jdbc:postgresql://postgres:5432/airflow \
  --jdbc-user airflow \
  --jdbc-password airflow
""",
        execution_timeout=timedelta(
            minutes=20
        ),
    )

    wait_for_silver >> run_gold_spark
