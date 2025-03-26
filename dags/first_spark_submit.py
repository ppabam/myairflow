from pprint import pprint
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonVirtualenvOperator,
)

from airflow.sensors.filesystem import FileSensor

DAG_ID = "first_spark_sbumit"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description="first spark sbumit",
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 1, 1),
    catchup=True,
    tags=["spark", "sbumit"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    spark_submit = BashOperator(task_id='submit', bash_command='/home/tom/app/spark-3.5.1-bin-hadoop3/bin/spark-submit /home/tom/app/spark-3.5.1-bin-hadoop3/bin/py.py {{ ds_nodash }}')

    start >> spark_submit >> end
