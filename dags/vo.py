from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator, PythonOperator 
import pendulum

with DAG(
    "vo",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 3, 12),
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    def call_get_data(**kwargs):
        print("::group::MYLOG")
        print("HI")
        for k, v in kwargs.items():
            print(k, v)
        print("::endgroup::")
        
    get_data = PythonVirtualenvOperator(
            task_id="get_data",
            python_callable=call_get_data,
            requirements=[],
            # system_site_packages=False,
            # provide_context=True
        )
    
    start >> get_data >> end

if __name__ == "__main__":
    dag.test()
