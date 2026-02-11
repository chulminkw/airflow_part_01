from airflow.sdk import DAG, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pendulum import datetime
from include.utils import print_scheduling_variables_01, print_scheduling_variables
import os 

MYSQL_CONN_ID = "mysql_conn_01"
SQL_BASE_PATH = "/usr/local/airflow/include"
SQL_FILE_PATH = "sql/select_order_cnt.sql"

def get_rendered_sql_01(sql_path, context):
    """
    include/sql/ 경로에 있는 SQL 파일을 읽어 렌더링합니다.
    """
    with open(sql_path, 'r', encoding='utf-8') as f:
        template_sql = f.read()

    # task instance의 task에서 render_template() 메소드를 호출하여 template_sql에 rendering 적용
    return context['ti'].task.render_template(template_sql, context)

def get_rendered_sql_02(sql_file, context):
    """
    include/sql/ 경로에 있는 SQL 파일을 읽어 렌더링합니다.
    """
    template_search_path = context['dag'].template_search_path
    sql_path = os.path.join(template_search_path, sql_file)
    with open(sql_path, 'r', encoding='utf-8') as f:
        template_sql = f.read()

    # task instance의 task에서 render_template() 메소드를 호출하여 template_sql에 rendering 적용
    return context['ti'].task.render_template(template_sql, context)

with DAG(
    dag_id="orders_daily_fetch_hook_by_sqlfile_01",
    start_date=datetime(2026, 1, 21, tz="UTC"), # 테스트를 위해 tz="UTC"로 설정
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    # sql file path의 base path. sql 파일은 worker가 접근할 수 있는 local directory에 위치해야 함. 
    # include 디렉토리가 dags와 동일하게 위치해 있으므로 ../include를 적용해도 무방.
    template_searchpath="/usr/local/airflow/include", 
    tags=["templating", "orders"]
) as dag:

    @task
    def my_sql_task(**context):
        sql_full_path = os.path.join(SQL_BASE_PATH, SQL_FILE_PATH)
        # full path를 직접 입력
        sql = get_rendered_sql_01(sql_full_path, context)
        # DAG의 template_searchpath가 주어지면 이를 활용할 수 있음. sql_file만 입력
        # sql = get_rendered_sql_02(SQL_FILE_PATH, context)
        
        hook = MySqlHook(mysql_conn_id="mysql_conn_01")
        result = hook.run(sql=sql)
        print(f"#### result:{result}")

    run_my_sql_task = my_sql_task()
