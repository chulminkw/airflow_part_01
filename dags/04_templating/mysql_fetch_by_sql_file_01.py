from airflow.sdk import DAG, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pendulum import datetime
from include.utils import print_scheduling_variables_01, print_scheduling_variables

MYSQL_CONN_ID = "mysql_conn_01"

with DAG(
    dag_id="orders_daily_fetch_by_sqlfile_01",
    start_date=datetime(2026, 1, 21, tz="UTC"), # 테스트를 위해 tz="UTC"로 설정
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    # sql file path의 base path. sql 파일은 worker가 접근할 수 있는 local directory에 위치해야 함. 
    # include 디렉토리가 dags와 동일하게 위치해 있으므로 ../include를 적용해도 무방.
    template_searchpath="/usr/local/airflow/include", 
    tags=["templating", "orders"]
) as dag:
    # SQLExecuteOperator를 이용하여 수행.
    # sql은 template_fields이므로 template engine에서 template variable값을 parsing해서 넣어줌
    # data_interval_start 내장 변수는 python datetime 타입임에 유의하여 sql 작성
    # data_interval_end는 1일 기준 데이터 추출 시 반드시 order_dt < data와 같이 적용되어야 함(<= 아님)
    run_fetch_op = SQLExecuteQueryOperator(
        task_id="fetch_orders_by_op",
        conn_id=MYSQL_CONN_ID,
        sql="sql/select_order_cnt.sql" #/usr/local/airflow/include/sql/select_order_cnt.sql
    )