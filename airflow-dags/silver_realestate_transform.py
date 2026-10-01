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
    dag_id="silver_realestate_transform",
    description="Transform bronze real-estate XML into silver Parquet",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "parkjiyeon",
        "retries": 0,
    },
    tags=["q2", "realestate", "silver", "spark"],
) as dag:

    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="bronze_realestate_collect",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        mode="reschedule",
        timeout=600,
    )

    run_silver_spark = BashOperator(
        task_id="run_silver_spark",
        bash_command=r"""
set -euo pipefail

/home/airflow/.local/bin/spark-submit \
  --master local[*] \
  --name silver_realestate_transform \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.ui.port=4040 \
  --conf spark.sql.parquet.compression.codec=snappy \
  /opt/airflow/scripts/q2/silver_spark.py \
  --bucket realestate-parkjiyeon \
  --region ap-southeast-2 \
  --yyyymm "{{ dag_run.conf.get('yyyymm', data_interval_start.strftime('%Y%m')) }}" \
  --ui-hold-seconds "{{ dag_run.conf.get('ui_hold_seconds', 0) }}"
""",
        execution_timeout=timedelta(minutes=15),
    )

    wait_for_bronze >> run_silver_spark
