from airflow.sdk import DAG, chain
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

def print_hello():
    print("#### Hello from PythonOperator!")

with DAG(
    dag_id="first_dag", # dag_id는 airflow 내에서 반드시 고유한 값을 가져야 함.  
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"), # DAG의 시작 날짜/시간 
    schedule=None, # 수행 주기
    catchup=False, # 수행되지 않은 과거 Dag run 스케쥴 수행 여부
    tags=["fundamental", "tutorial"]  # dag의 tag
) as dag:
    
    # bash command을 수행하는 task operator로 bash_command 인자에 수행할 shell 명령어를 설정.
    # Operator는 반드시 task_id를 가지며, Dag내에서 고유해야 함.
    # 개별 Operator별로 특징적인 argument들을 가짐
    bash_task = BashOperator(
        task_id="print_date_bash",
        bash_command='echo "##### today: `date`"'
    )
    # python_callable로 지정된 python function을 수행하는 task operator
    python_task = PythonOperator(
        task_id="say_hello_python",
        python_callable=print_hello
    )

    # task들의 수행 dependency 설정
    bash_task >> python_task
    #chain(bash_task, python_task)