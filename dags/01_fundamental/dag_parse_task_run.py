from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

print("##### This runs at DAG PARSE TIME")

def runtime_task():
    print("##### This runs at TASK EXECUTION TIME from PythonOperator task")

with DAG(
    dag_id="dag_parse_task_run",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    task_01 = PythonOperator(
        task_id="runtime_task",
        python_callable=runtime_task,
    )

    print("this is called inside DAG Context")

    @task
    def simple_task():
        print("##### This also runs at TASK EXECUTION TIME from taskflow task")

    task_02 = simple_task()

    task_01 >> task_02
    
