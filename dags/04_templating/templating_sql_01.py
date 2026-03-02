from airflow.sdk import DAG, Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import datetime

SOURCE_SCHEMA = "stage"
LOCAL_TZ = "Asia/Seoul"

with DAG(
    dag_id="templating_sql_01",
    start_date=datetime(2026, 2, 1, 
                        hour=0, minute=0, second=0, tz=LOCAL_TZ), # 영상을 보시는 날 2일전으로 설정
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=LOCAL_TZ), # @daily
    catchup=True, # 실습을 위해서 start_date가 과거 시간으로 설정하고 catchup을 True로 설정 
    end_date=datetime(2026, 2, 6,
                      hour=23, minute=59, second=59, tz=LOCAL_TZ), # 영상을 보시는 날로 설정
    tags=["templating"],
    max_active_runs=1,
    params={"tz": LOCAL_TZ,
            "src_schema": Param(default=SOURCE_SCHEMA, type="string")
           } # format string 대신 params를 사용.
) as dag:
       
    # SQLExecuteQueryOperator의 sql인자는 template_fields
    run_orders_count_fstring = SQLExecuteQueryOperator(
        task_id="orders_count_fstring",
        conn_id="postgres_delivery_conn",
        hook_params={"options": f"-c timezone={LOCAL_TZ}"},
        sql=f"""
        SELECT DATE_TRUNC('hour', order_dt) AS order_hour, count(*) as cnt 
        FROM {SOURCE_SCHEMA}.orders 
        where order_dt >= '{{{{ data_interval_start.in_timezone('Asia/Seoul') }}}}' 
        and order_dt < '{{{{ data_interval_end.in_timezone('{LOCAL_TZ}') }}}}'
        group by DATE_TRUNC('hour', order_dt) 
        order by 1;
        """
    )

    run_orders_count_params = SQLExecuteQueryOperator(
        task_id="orders_count_params",
        conn_id="postgres_delivery_conn",
        hook_params={"options": f"-c timezone={LOCAL_TZ}"},
        sql="""
        SELECT DATE_TRUNC('hour', order_dt) AS order_hour, count(*) as cnt FROM 
        {{ params.src_schema }}.orders 
        where order_dt >= '{{ data_interval_start.in_timezone(params.tz) }}' 
        and order_dt < '{{ data_interval_end.in_timezone(params.tz) }}'
        group by DATE_TRUNC('hour', order_dt) 
        order by 1;
        """
    )

    [ run_orders_count_fstring, run_orders_count_params ]
