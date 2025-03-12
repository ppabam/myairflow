from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.trigger_rule import TriggerRule
import pendulum

def print_kwargs():
    from myairflow.notify import send_noti
    send_noti("python virtualenv operator : vhzkclq0705")
    
    
# Directed Acyclic Graph
with DAG(
    "vhzkclq",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 12, tz="Asia/Seoul"),
    default_args=  {
        "depends_on_past": True
    },
    max_active_runs=1
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    send_notification = PythonVirtualenvOperator(
        task_id="send_notification",
        python_callable=print_kwargs,
        requirements=[
            "git+https://github.com/vhzkclq0705/myairflow.git@0.1.0"
        ]
    )
    
    start >> send_notification >> end
    
if __name__ == "__main__":
    dag.test()