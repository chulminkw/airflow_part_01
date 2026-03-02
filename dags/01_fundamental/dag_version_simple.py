from airflow.sdk import DAG, chain
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from pendulum import datetime

with DAG(
    dag_id="dag_version_simple_01",
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["fundamental"]
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command='echo "##### extract task executed"'
    )

    transform_a = BashOperator(
        task_id="transform_a",
        bash_command='echo "##### transform_a task executed"'
    )

    transform_b = BashOperator(
        task_id="transform_b",
        bash_command='echo "##### transform_b task executed"'
    )

    load = BashOperator(
        task_id="load",
        bash_command='echo "##### load task executed"'
    )

    empty_a = EmptyOperator(
        task_id="empty_a"
    )

    empty_b = EmptyOperator(
        task_id="empty_b"
    )
    
    # cyclic dependency 허용 안됨. 
    # extract >> transform_a >> extract

    # extract >> transform_a >> transform_b >> load # dependency case 1
    # extract >> [transform_a, transform_b] >> load # dependency case 2
    
    # chain 함수로 dependency 설정
    # chain(extract, transform_a, transform_b, load) # dependency case #1
    # chain(extract, [transform_a, transform_b], load) # dependency case 2

    # >> syntax는 list to list dependency 설정 안됨. 아래는 오류 발생. 
    # extract >> [transform_a, transform_b] >> [empty_a, empty_b]

    # chain은 list to list dependency 설정 가능
    # chain(extract, [transform_a, transform_b], [empty_a, empty_b])
