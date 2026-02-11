from airflow.sdk import DAG, task, get_current_context
from pendulum import datetime

# Airflow 3 부터 create_cron_data_intervals = False 로 지정되어서 create_cron_data_intervals = True로 변경되어야
# data_interval_start, data_interval_end가 1일 간격으로 생성됨. 
with DAG(
    dag_id="schedule_variables_01",
    start_date=datetime(2026, 1, 19, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=True, #catchup을 True로 설정하면 start_date이후로 실행이 되지 않은 Dag Run을 실행 시킴
    tags=["scheduling", "schedule_variables"],
    max_active_runs = 1 # Dag Run을 동시에 최대 active 1개씩 수행(병렬 수행하지 않음)
) as dag:
    # 아래는 taskflow 함수에 argument insert 방식으로 내장 변수들의 값을 확인
    @task
    def print_scheduling_context(**context):
        timezone_value = context["dag"].timezone
        run_type = context['dag_run'].run_type
        run_id = context["run_id"]
        logical_date = context["logical_date"]
        data_interval_start = context["data_interval_start"]
        data_interval_end = context["data_interval_end"]

        print("========== DAG RUN ID & TIMEZONE ==========")
        print(f"run_id: {run_id}")
        print(f"DAG timezone:{timezone_value}")

        print("\n========== LOGICAL_DATE & DATA INTERVAL  ==========")
        print(f"logical_date: {logical_date}, type:{type(logical_date)}")
        print(f"logical_date in timezone: {logical_date.in_timezone('Asia/Seoul')}")
        
        print(f"data_interval_start ~ data_interval_end:{data_interval_start} ~ {data_interval_end}")
        print(f"data_interval_start ~ data_interval_end in timezone: \
        {data_interval_start.in_timezone(timezone_value)} ~ {data_interval_end.in_timezone(timezone_value)}")


    print_scheduling_context()
