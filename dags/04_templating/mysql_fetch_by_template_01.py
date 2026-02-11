from airflow.sdk import DAG, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pendulum import datetime
from include.utils import print_scheduling_variables_01, print_scheduling_variables

MYSQL_CONN_ID = "mysql_conn_01"

with DAG(
    dag_id="orders_daily_fetch_06",
    start_date=datetime(2026, 1, 21, tz="UTC"), # 테스트를 위해 tz="UTC"로 설정
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["templating", "orders"]
) as dag:
    # SQLExecuteOperator를 이용하여 수행.
    # sql은 template_fields이므로 template engine에서 template variable값을 parsing해서 넣어줌
    # data_interval_start 내장 변수는 python datetime 타입임에 유의하여 sql 작성
    # data_interval_end는 1일 기준 데이터 추출 시 반드시 order_dt < data와 같이 적용되어야 함(<= 아님)
    run_fetch_op = SQLExecuteQueryOperator(
        task_id="fetch_orders_by_op",
        conn_id=MYSQL_CONN_ID,
        sql="""
        -- 주석은 sql 주석인 -- 또는 /* */
        SELECT count(*) as cnt
        FROM orders
        WHERE order_dt >= '{{ data_interval_start }}'
          AND order_dt <  '{{ data_interval_end }}'
        """
    )

    # Hook을 사용할 때는 run()메소드의 sql 인자가 template_fields가 아니므로 
    # 아래와 같이 {{ }} 형태의 template variable을 적용 할 수가 없음. 
    @task
    def fetch_orders_by_hook_incorrect():
        hook = MySqlHook(mysql_conn_id="mysql_conn_01")
        hook.run(sql="""
            SELECT count(*) as cnt
            FROM orders
            WHERE order_dt >= '{{ data_interval_start }}'
            AND order_dt <  '{{ data_interval_end }}'
        """
        )

    run_fetch_hook_incorrect = fetch_orders_by_hook_incorrect()

    # 아래와 같이 task flow 함수의 인자로 context variable을 지정하는 방식으로 task flow api 생성
    @task
    def fetch_orders_by_hook(data_interval_start=None, 
                             data_interval_end=None):
        hook = MySqlHook(mysql_conn_id="mysql_conn_01")
        print(f"#### data_interval_start:{data_interval_start}, data_interval_end:{data_interval_end}")

        # 아래와 같이 함수 인자로 들어온 data_interval_start, data_interval_end를 이용하여 sql 생성하고 이를 호출
        sql=f"""
        SELECT count(*) as cnt
        FROM orders
        WHERE order_dt >= '{ data_interval_start }'
          AND order_dt <  '{ data_interval_end }'
        """
        hook.run(sql=sql)

    run_fetch_hook = fetch_orders_by_hook()

    @task
    def print_context_value(dag, run_id, ds, logical_date, data_interval_start,
                            data_interval_end):
        
        timezone_value = dag.timezone

        print_scheduling_variables(timezone_value=timezone_value,
                                   run_id=run_id, logical_date=logical_date,
                                   data_interval_start=data_interval_start,
                                   data_interval_end=data_interval_end)

    run_print_context = print_context_value()

    [run_fetch_op, run_fetch_hook_incorrect, run_fetch_hook, run_print_context]