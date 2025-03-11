from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

# Directed Acyclic Graph
with DAG(
    "hello",
    # schedule=timedelta(days=1),
    schedule="0 * * * *",
    # schedule="@hourly",
    start_date=datetime(2025, 3, 10)
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    start >> end
