from airflow.sdk import DAG, task, Param
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import datetime
import polars as pl
import os

SOURCE_SCHEMA = "stage"
TARGET_SCHEMA = "summary"
LOCAL_TZ = "Asia/Seoul"
POSTGRES_OUTPUT_DIR = "/tmp/data/postgres"

# sql, output_filepath, file_format은 PythonOperator의 op_kwargs인자로 입력됨
# context 정보도 실행시 입력됨
def run_query_with_process(sql, output_filepath, file_format, **context):
    print(f"#### sql: {sql}, output_filepath: {output_filepath}, logical_date:{context['logical_date']}")
    hook = PostgresHook(postgres_conn_id="postgres_delivery_conn",
                        options=f"-c timezone={LOCAL_TZ}")
    conn_uri = hook.get_uri()
    polars_df = pl.read_database_uri(
        query=sql, # 함수의 인자로 들어온 sql. PythonOperator에서 op_kwargs 로 입력됨
        uri=conn_uri,
        engine="connectorx", # engine인자는 기본 connectorx
    )    
    # file_format에 따라 csv 또는 parquet으로 설정
    if file_format == "csv":
        polars_df.write_csv(output_filepath, )
    elif file_format == "parquet":
        polars_df.write_parquet(output_filepath)

# PythonOperator를 상속 받아서 SQL에 Template을 적용하는 Operator
class PythonSqlOperator(PythonOperator):
    # 1. 템플릿으로 처리할 필드 이름 등록
    template_fields = ("sql", "op_kwargs")
    # 2. sql은 sql 전용 뷰어로 렌터링, op_kwargs는 json 뷰어로 렌더링
    template_fields_renderers = {"sql": "sql", "op_kwargs": "json"}
    # 3. .sql 확장자를 등록해야 Web UI에서 SQL 뷰어로 보임
    template_ext = (".sql",)

    def __init__(self, sql, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql  # 파일 경로 또는 쿼리 저장

    def execute(self, context):
        # 4. 실행 직전에 렌더링된 self.sql을 op_kwargs에 주입하여 함수 인자로 전달
        self.op_kwargs["sql"] = self.sql
        return super().execute(context)
        
# sql인자로 sql문 또는 sql문을 담은 파일명이 올 수 있음. 
def run_query(sql, **context):
    print(f"#### sql: {sql}, logical_date:{context['logical_date']}")
    hook = PostgresHook(postgres_conn_id="postgres_delivery_conn",
                        options=f"-c timezone={LOCAL_TZ}")
    hook.run(sql=sql)

with DAG(
    dag_id="hook_with_templating_01",
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
            "src_schema": Param(default=SOURCE_SCHEMA, type="string"),
            "tgt_schema": Param(default=TARGET_SCHEMA, type="string")
           } # format string 대신 params를 사용.
) as dag:
    
    @task
    def initialize():
        if not os.path.exists(POSTGRES_OUTPUT_DIR):
            os.makedirs(POSTGRES_OUTPUT_DIR)

    run_intialize = initialize()

    run_orders_count_params_01 = PythonOperator(
        task_id="orders_count_params_01",
        python_callable=run_query_with_process,
        op_kwargs={
        "sql": f"""
                SELECT DATE_TRUNC('hour', order_dt) AS order_hour, count(*) as cnt 
                FROM {SOURCE_SCHEMA}.orders 
                where order_dt >= '{{{{ data_interval_start.in_timezone('{LOCAL_TZ}') }}}}' 
                and order_dt < '{{{{ data_interval_end.in_timezone('{LOCAL_TZ}') }}}}'
                group by DATE_TRUNC('hour', order_dt) 
                order by 1;""",
        "output_filepath": f"{POSTGRES_OUTPUT_DIR}/{{{{ logical_date.in_timezone('{LOCAL_TZ}').strftime('%Y%m%d') }}}}.csv",
        "file_format": "csv",
        }
    )

    #Custom PythonSqlOperator에 인자로 sql을 template으로 부여
    run_orders_count_params_02 = PythonSqlOperator(
        task_id="orders_count_params_02",
        python_callable=run_query_with_process,
        sql=f"""
            SELECT DATE_TRUNC('hour', order_dt) AS order_hour, count(*) as cnt 
            FROM {SOURCE_SCHEMA}.orders 
            where order_dt >= '{{{{ data_interval_start.in_timezone('{LOCAL_TZ}') }}}}' 
            and order_dt < '{{{{ data_interval_end.in_timezone('{LOCAL_TZ}') }}}}'
            group by DATE_TRUNC('hour', order_dt) 
            order by 1;""",
        op_kwargs={
            # sql 인자는 오퍼레이터가 자동으로 넣어줌
            "output_filepath": f"{POSTGRES_OUTPUT_DIR}/{{{{ logical_date.in_timezone('{LOCAL_TZ}').strftime('%Y-%m-%d') }}}}.csv",
            "file_format": "csv"
        }
    )

    run_orders_upsert_summary = PythonSqlOperator(
        task_id="orders_summary_upsert",
        python_callable=run_query,
        sql="postgres_upsert_summary.sql"
    )  

    run_intialize >> [run_orders_count_params_01, run_orders_count_params_02] >> run_orders_upsert_summary
