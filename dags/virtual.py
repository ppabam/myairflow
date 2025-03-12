from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator 
import pendulum


    
with DAG(
    "virtual",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 12, tz="Asia/Seoul"),
    default_args={
         "depends_on_past": False,
    },
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    def print_kwargs():
        from myairflow.send_notify import send_noti
        send_noti("python virtualenv operator : TOM")
    
    send_notification = PythonVirtualenvOperator(
            task_id="send_notification",
            python_callable=print_kwargs,
            requirements=[
                "git+https://github.com/ppabam/myairflow.git@0.1.0"
            ]
        )

    sleep = BashOperator(task_id="sleep", 
                         bash_command="sleep 5")
    
    
    start >> send_notification >> sleep >> end

if __name__ == "__main__":
    dag.test()
