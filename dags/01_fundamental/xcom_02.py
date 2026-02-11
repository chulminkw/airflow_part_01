from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

def push_dict():
    return {
        'username': 'airflow_user',
        'run_status': 'success',
        'score': 99
    }

def push_dict(ti):
    ti.xcom_push(key="username", value="airflow_user")
    ti.xcom_push(key="run_status", value="success")


def pull_dict(ti):
    pulled = ti.xcom_pull(task_ids='task_push')
    print(f"#### Pulled: {pulled}")

with DAG(
    dag_id="xcom_multi_keys",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    run_task_01 = PythonOperator(
        task_id='task_push',
        python_callable=push_dict,
    )

    run_task_02 = PythonOperator(
        task_id='task_pull',
        python_callable=pull_dict,
    )

    run_task_01 >> run_task_02