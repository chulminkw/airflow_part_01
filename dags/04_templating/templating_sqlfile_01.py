from airflow.sdk import DAG, task, get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import datetime

SOURCE_SCHEMA = "stage"
TARGET_SCHEMA = "summary"
LOCAL_TZ = "Asia/Seoul"

with DAG(
    dag_id="templating_sqlfile_01",
    start_date=datetime(2026, 2, 6, 
                        hour=0, minute=0, second=0, tz=LOCAL_TZ), # 영상을 보시는 날 2일전으로 설정
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=LOCAL_TZ), # @daily
    catchup=True, # 실습을 위해서 start_date가 과거 시간으로 설정하고 catchup을 True로 설정 
    end_date=datetime(2026, 2, 21,
                      hour=23, minute=59, second=59, tz=LOCAL_TZ), # 영상을 보시는 날로 설정
    tags=["templating"],
    max_active_runs=1,
    template_searchpath=["./include/sql"], # /usr/local/airflow/include/sql
    params={"tz": LOCAL_TZ,
            "src_schema": SOURCE_SCHEMA,
            "tgt_schema": TARGET_SCHEMA} # format string 대신 params를 사용.
) as dag:
       
    # SQLExecuteQueryOperator의 sql인자는 template_fields
    run_create_table_if_not = SQLExecuteQueryOperator(
        task_id="create_table_if_not",
        conn_id="postgres_delivery_conn",
        hook_params={"options": f"-c timezone={LOCAL_TZ}"},
        sql="postgres_create_summary.sql"
    )

    run_orders_upsert_summary = SQLExecuteQueryOperator(
        task_id="orders_count_params",
        conn_id="postgres_delivery_conn",
        hook_params={"options": f"-c timezone={LOCAL_TZ}"},
        sql="postgres_upsert_summary.sql"
    )

    run_create_table_if_not >> run_orders_upsert_summary
