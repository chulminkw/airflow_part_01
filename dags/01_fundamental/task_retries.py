from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime, duration # duration은 retry_delay 설정을 위해 사용
from datetime import timedelta  # retry_delay 설정을 위해 사용. pendulum의 duration도 사용가능.

# DAG 내 모든 태스크에 적용될 Default(기본) 재시도 설정
default_args = {
    'retries': 2,                          # 실패 시 재시도 횟수
    'retry_delay': timedelta(seconds=30),   # 재시도 간 대기 시간
    'retry_exponential_backoff': True,    # 재시도 간격 점진적 증가
    'max_retry_delay': timedelta(minutes=10) # 최대 대기 시간 제한
}

def print_hello():
    inf = 1 / 0 # 0으로 나눌 경우 오류 발생. 
    print("#### Hello from PythonOperator!")

with DAG(
    dag_id="task_retries_01",
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    default_args=default_args,  # default 재시도 규칙 적용
    tags=["fundamental"]
) as dag:
    
    # Operator 레벨에서 DAG 기본 설정을 override. DAG 기본값(2회) 대신 4회 retries 시도
    python_task = PythonOperator(
        task_id="say_hello_python",
        python_callable=print_hello,
        retries=4, 
        retry_delay=timedelta(seconds=10)
    )

    # TaskFlow API의 경우 @task()에 인자로 retries 관련 parameter를 입력하면 override됨
    @task(
        task_id="say_hello_task_01",
        retries=3  # 데코레이터 내에서 직접 설정
    )
    def say_hello():
        inf = 1/0 # 0으로 나눌 경우 오류 발생.
        print("#### Hello From Taskflow")

    say_hello_task = say_hello()

     # task에서 특별한 retries 설정이 없으면 DAG 설정을 따름
    @task
    def say_goodbye():
        inf = 1/0 # 0으로 나눌 경우 오류 발생.
        print("#### Good Bye From Task")

    say_goodbye_task = say_goodbye()

    # 의존성 설정
    python_task >> say_hello_task >> say_goodbye_task
    # 의존성 없이 수행
    # [python_task, python_task, python_task]
