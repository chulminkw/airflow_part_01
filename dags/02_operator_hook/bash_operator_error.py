from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime

MOUNT_DIR = "/"
USAGE_THRESHOLD = 30 # Percent

with DAG(
    dag_id="bash_operator_error_01", # dag_id는 airflow 내에서 반드시 고유한 값을 가져야 함.  
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["operator_hook"]
) as dag:
    
    catch_error = BashOperator(
            task_id="catch_error",
            bash_command="""
                #set -e  # 에러 발생 시 즉시 종료하는 옵션
                ls /non_existent_folder
                echo "##### This should never be printed #####"
            """
    )
    # shell script 작성 시에도 set -e, pipeline 오류, if 시 exit 반환등의 로직 처리 유의
    run_bash_task = BashOperator(
        task_id="check_disk",
        bash_command=f"/usr/local/airflow/check_disk.sh {MOUNT_DIR} {USAGE_THRESHOLD}"
    )

    catch_error >> run_bash_task