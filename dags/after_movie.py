from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        BranchPythonOperator, 
        PythonVirtualenvOperator,
)
from airflow.sensors.filesystem import FileSensor

with DAG(
    'after_movie',
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description='movie',
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 5),
    catchup=True,
    tags=['api', 'movie', 'sensor'],
) as dag:
    REQUIREMENTS = ["git+https://github.com/ppabam/movie.git@0.2.5"]
    BASE_DIR = "/home/tom/data/movies/done/dailyboxoffice"

    
    start = EmptyOperator(task_id='start')
    
    check_done = FileSensor(task_id="check.done", filepath=f"{BASE_DIR}/" + "{{ ds_nodash }}/_DONE", fs_conn_id="done_flag")
    
    end = EmptyOperator(task_id='end')
    
    start >> check_done >> end
