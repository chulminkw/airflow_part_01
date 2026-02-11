from airflow.sdk import DAG, task, get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pendulum import datetime
import logging

with DAG(
    dag_id="sql_with_template_01",
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=True,
) as dag:
    
    # 아래 쿼리는 DAG Parsing 시 수행. 
    # templated_query = "SELECT * FROM orders where order_date_str='{{ ds }}'"
    # print("templated_query:", templated_query)
    
    # SQLExecuteQueryOperator의 sql인자는 template_fields
    run_select_orders_daily_01 = SQLExecuteQueryOperator(
        task_id="select_orders_daily_01",
        conn_id="mysql_conn_01",
        sql="""
        SELECT * FROM orders where order_date_str='{{ ds }}'
        """
    )

    run_select_orders_daily_02 = SQLExecuteQueryOperator(
        task_id="select_orders_daily_02",
        conn_id="mysql_conn_01",
        sql="""
        SELECT * FROM orders 
        where order_dt >= '{{ data_interval_start }}' and order_dt < '{{ data_interval_end }}'
        """
    )

    # 아래는 {{ ds }}를 호출 시 함수 인자로 받아서 처리
    @task
    def select_orders_daily_03(ds_arg):
        sql = f"SELECT * FROM orders where order_date_str='{ds_arg}'"
        logging.info(f"#### SQL:{sql}")

        hook = MySqlHook(mysql_conn_id="mysql_conn_01")
        hook.run(sql)

    run_select_orders_daily_03 = select_orders_daily_03("{{ ds }}") # 반드시 {{ ds }}가 아니라 "{{ ds }}" 여야함.

    # 아래는 ds를 context에서 가져옴.
    @task
    def select_orders_daily_04():
        context = get_current_context()
        ds_val = context["ds"]

        sql = f"SELECT * FROM orders where order_date_str='{ds_val}'"
        logging.info(f"#### SQL:{sql}")

        hook = MySqlHook(mysql_conn_id="mysql_conn_01")
        hook.run(sql)

    run_select_orders_daily_04 = select_orders_daily_04()