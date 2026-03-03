from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime
import pendulum

def print_hello():
    print("#### Hello from PythonOperator!")

with DAG(
    dag_id="task_flow_sample_01", # dag_id는 airflow 내에서 반드시 고유한 값을 가져야 함.  
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"), # DAG의 시작 날짜/시간 
    schedule=None, # 수행 주기
    catchup=False, # 수행되지 않은 과거 Dag run 스케쥴 수행 여부
    tags=["fundamental", "task_flow_api"]  # dag의 tag
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

    #python 함수를 task로 변환하는 decorator
    @task(task_id="say_hello_task_01")
    def say_hello():
        print("#### Hello From Taskflow")

    # say_hello taskflow API를 DAG에 등록하고, 
    # say_hello() 수행 시 반환되는 XComArg를 say_hello_task변수로 할당 
    say_hello_task = say_hello()

    @task
    def say_goodbye():
        print("#### Good Bye From Task")

    say_goodbye_task = say_goodbye()
    
    # debugging용. DAG내 일반 python 함수.
    print(f"#### python_task type:{type(python_task)}, \
          say_hello_task type:{type(say_hello_task)}")
    
    # task들의 수행 dependency 설정
    bash_task >> [python_task, say_hello_task] >> say_goodbye_task
    # bash_task >> [python_task, say_hello()] >> say_goodbye()