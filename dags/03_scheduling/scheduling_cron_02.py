from airflow.sdk import DAG, task
from pendulum import datetime
from include.utils import print_scheduling_variables
from airflow.timetables.interval import CronDataIntervalTimetable

with DAG(
    dag_id="schedule_cron_timetable_01",
    start_date=datetime(2026, 1, 12, tz="Asia/Seoul"),
    #Airflow 3.x 부터는 명확하게 cron data interval 기반의 Timetable 설정을 더 선호
    schedule=CronDataIntervalTimetable("0 22 * * 1-5", timezone="Asia/Seoul"), # 주중(weekday) 밤 10시에 daily로 수행.
    catchup=True, #catchup을 True로 설정하면 start_date이후로 실행이 되지 않은 Dag Run을 실행 시킴
    tags=["scheduling", "cron"],
    max_active_runs=1, # Dag Run을 병렬로 최대 active 5개씩 수행
    #max_active_tasks=10, # 모든 active Dag run들이 최대로 수행하는 task instance 갯수
) as dag:
    # 아래는 taskflow 함수에 argument insert 방식으로 내장 변수들의 값을 확인
    @task
    def print_scheduling_context(
        dag=None,
        run_id=None,
        logical_date=None,
        data_interval_start=None,
        data_interval_end=None,
        prev_data_interval_start=None,
        prev_data_interval_end=None
    ):
        timezone_value = dag.timezone
        print_scheduling_variables(timezone_value, run_id, logical_date, 
                                   data_interval_start, data_interval_end)
        
    print_scheduling_context()
