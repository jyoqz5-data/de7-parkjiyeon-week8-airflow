from datetime import datetime, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


with DAG(
    dag_id="hello_gitsync",
    description="Verify DAG deployment through git-sync",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "parkjiyeon",
        "retries": 0,
    },
    tags=["q4", "git-sync", "kubernetes"],
) as dag:

    say_hello = BashOperator(
        task_id="say_hello",
        bash_command="""
echo "Hello from Airflow git-sync"
date
hostname
""",
    )
