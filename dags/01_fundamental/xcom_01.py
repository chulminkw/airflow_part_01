from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

def push_value():
    value = "Hello From task_push"
    return value # return시 자동으로 자신의 task_id로 XCom push 수행. key는 return_value

# ti 객체는 airflow가 해당 함수 수행 시에 자동으로 입력하여 수행함. 인자명을 ti가 아닌 다른 걸로 설정하면 안됨. 
# ti = context['ti']
def pull_value(ti):
    pulled = ti.xcom_pull(task_ids='task_push') # task_ids로 xcom value를 pull한 task의 id를 입력받음
    print(f"#### pulled from Xcom {pulled}")


with DAG(
    dag_id="xcom_01",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    run_task_01 = PythonOperator(
        task_id="task_push",
        python_callable=push_value
    )

    run_task_02 = PythonOperator(
        task_id="task_pull",
        python_callable=pull_value
    )

    run_task_01 >> run_task_02

    