from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime

MOUNT_DIR = "/"
USAGE_THRESHOLD = 30 # Percent

with DAG(
    dag_id="bash_operator_usages_01", # dag_id는 airflow 내에서 반드시 고유한 값을 가져야 함.  
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["operator_hook"]
) as dag:
    # 간단한 shell script는 bash_command에 인자로 작성
    run_bash_task_01 = BashOperator(
        task_id="check_disk_01",
        bash_command="""
        echo "Checking disk usage"
        df -h /
        """,
        do_xcom_push=True # bash 수행 출력(마지막 출력라인)을 xcom에 push
    )
    
    # 복잡한 shell 로직은 직접 bash_command에 기술하는 것보다는 
    # worker node에서 shell script file로 생성후 구현된 shell script를 호출하는 것을 권장 
    run_bash_task_02 = BashOperator(
        task_id="check_disk_02",
        bash_command=f"""
            echo "Checking mount availability for {MOUNT_DIR}"

            # 해당 mount dir이 존재하는지 확인. 문제 발생 시 exit 1
            if [ ! -d "{MOUNT_DIR}" ]; then
		  echo "ERROR: Mount directory does not exist: $MOUNT_DIR"
		  exit 1
	    fi

            # Extract usage percentage
            USAGE=$(df -P {MOUNT_DIR} | awk 'NR==2 {{gsub("%","",$5); print $5}}')

            echo "Disk usage: $USAGE%"

            # 해당 mount dir의 usage가 threshold가 지정값 이상이면 exit 1
            if [ "$USAGE" -ge {USAGE_THRESHOLD} ]; then
            echo "ERROR: Disk usage above {USAGE_THRESHOLD}%"
            exit 1
            fi
        """
    )

    # 복잡한 schell script에는 bash_command에 기술하지 않고, 구현된 shell script를 호출
    run_bash_task_03 = BashOperator(
        task_id="check_disk_03",
        bash_command=f"/usr/local/airflow/check_disk.sh {MOUNT_DIR} {USAGE_THRESHOLD}"
    )

    # @task.bash decorator는 BashOperator를 Taskflow API형태로 제공함. 
    # BashOperator의 bash_command는 return 으로 대체됨. 
    @task.bash(task_id="check_disk_04")
    def bash_task():
        return f"/usr/local/airflow/check_disk.sh {MOUNT_DIR} {USAGE_THRESHOLD}"
    
    run_bash_task_04 = bash_task()

    # 테스트를 위해 병렬로 개별 task들을 수행. 
    [run_bash_task_01, run_bash_task_02, run_bash_task_03, run_bash_task_04]