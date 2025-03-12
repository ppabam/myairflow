from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum

def generate_bash_commands(columns: list):
    cmds = []
    max_length = max(len(c) for c in columns)
    for c in columns:
        # 가변적인 공백 추가 (최대 길이에 맞춰 정렬)
        padding = " " * (max_length - len(c))
        cmds.append(f'echo "{c}{padding} : ====> {{{{ {c} }}}}"')
    return "\n".join(cmds)

def send_noti(msg):
    import requests
    import os

    WEBHOOK_ID = os.getenv('DISCORD_WEBHOOK_ID')
    WEBHOOK_TOKEN = os.getenv('DISCORD_WEBHOOK_TOKEN')
    WEBHOOK_URL = f"https://discordapp.com/api/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}"
    data = { "content": msg }
    response = requests.post(WEBHOOK_URL, json=data)

    if response.status_code == 204:
        print("메시지가 성공적으로 전송되었습니다.")
    else:
        print(f"에러 발생: {response.status_code}, {response.text}")

def print_kwargs(dag, task, data_interval_start, **kwargs):
    ds = data_interval_start.in_tz('Asia/Seoul').format('YYYYMMDDHH')
    msg = f"{dag.dag_id} {task.task_id} {ds} OK / TOM"
    send_noti(msg)
    
# Directed Acyclic Graph
with DAG(
    "seoul",
    schedule="@hourly",
    # start_date=datetime(2025, 3, 10)
    start_date=pendulum.datetime(2025, 3, 11, tz="Asia/Seoul"),
    # default_args={
    #     "depends_on_past": True,
    # },
    # max_active_runs=1
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    # send_notification = EmptyOperator(task_id="send_notification")
    send_notification = PythonOperator(
            task_id="send_notification",
            python_callable=print_kwargs
        )
    
    
    columns_b1 = [
    "data_interval_start", "data_interval_end", "logical_date", "ds", "ds_nodash",
    # "exception",
    "ts", "ts_nodash_with_tz", "ts_nodash", "prev_data_interval_start_success",
    "prev_data_interval_end_success", "prev_start_date_success", "prev_end_date_success",
    "inlets", "inlet_events", "outlets", "outlet_events", "dag", "task", "macros",
    "task_instance", "ti", "params", "var.value", "var.json", "conn", "task_instance_key_str",
    "run_id", "dag_run", "map_index_template", "expanded_ti_count", "triggering_dataset_events"
    ]
    cmds_b1 = generate_bash_commands(columns_b1)
   
    b1 = BashOperator(
        task_id="b_1", 
        bash_command=f"""
            echo "date ====================> `date`"
            {cmds_b1}
        """)
    
    cmds_b2_1 = [
    "execution_date",
    "next_execution_date","next_ds","next_ds_nodash",
    "prev_execution_date","prev_ds","prev_ds_nodash",
    "yesterday_ds","yesterday_ds_nodash",
    "tomorrow_ds", "tomorrow_ds_nodash",
    "prev_execution_date_success",
    "conf"
    ]
    
    cmds_b2_1 = generate_bash_commands(cmds_b2_1)
   
    b2_1 = BashOperator(task_id="b_2_1", 
                        bash_command=f"""
                        {cmds_b2_1}
                        """)
    
    b2_2 = BashOperator(task_id="b_2_2", 
                        bash_command="""
                        echo "data_interval_start : {{ data_interval_start.in_tz('Asia/Seoul') }}"
                        """)
    
    mkdir_cmd = """
        YEAR={{ data_interval_start.in_tz('Asia/Seoul').year }}
        MONTH={{ data_interval_start.in_tz('Asia/Seoul').format('MM') }}
        DAY={{ data_interval_start.in_tz('Asia/Seoul').format('DD') }}
        HOUR={{ data_interval_start.in_tz('Asia/Seoul').format('HH') }}
        
        mkdir -p ~/data/seoul/$YEAR/$MONTH/$DAY/$HOUR
        
        mkdir -p ~/data/seoul/{{ data_interval_start.in_tz('Asia/Seoul').format('YYYY/MM/DD/HH') }}
        
        tree ~/data/seoul
    """
    mkdir = BashOperator(task_id="mkdir", bash_command=mkdir_cmd)
    
    start >> b1 >> [b2_1, b2_2] >> mkdir >> end
    mkdir >> send_notification

if __name__ == "__main__":
    dag.test()