from airflow.sdk import DAG, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime
import pandas as pd
import os

OUTPUT_DIR = "/tmp/mysql_load"

with DAG(
    dag_id="sql_operator_02",
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    template_searchpath=["./include/sql"], # /usr/local/airflow/include/sql
    tags=["operator_hook", "sql"],
) as dag:
    # sql 파일은 반드시 utf-8 로 인코딩 되어 있어야 함. 
    # utf-8이 아닌데 파일내 한글이 들어가 있을 경우 문제 발생
    create_table_op = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="mysql_ecom_conn",
        sql="mysql_drop_create_users.sql"
    )

    insert_user_op = SQLExecuteQueryOperator(
        task_id="insert_users",
        conn_id="mysql_ecom_conn",
        sql="mysql_insert_users.sql"
    )

    # SELECT 쿼리는 xcom으로 select 결과를 저장함. 너무 많은 결과를 select 하지 않도록 유의
    # xcom 저장시 json 포맷으로 저장되며, 추후 output등으로 호출될 때 tuple(tuple) 타입으로 역직렬화 됨. 
    # json은 datetime 타입을 지원하지 않음. 만약에 select 컬럼에 datetime 타입이 포함되면 직렬화/역직렬화를 위한 별도의 정보를 함께 가지게됨. 
    select_users_op = SQLExecuteQueryOperator(
        task_id="select_users",
        conn_id="mysql_ecom_conn",
        sql="""
        SELECT id, name, age, created_at, updated_at FROM users;
        """
    )

    @task
    def write_users_csv(rows):
        """
        rows: SQLExecuteQueryOperator.output임. 타입은 tuple(tuple) 
        """
        print(f"##### rows: {rows} #####")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        # tuple(tuple) 을 Pandas DataFrame으로 변경 
        df = pd.DataFrame(rows, columns=["id", "name", "age", "created_at", "updated_at"])

        # worker node에 csv로 저장.
        file_path = f"{OUTPUT_DIR}/mysql_users.csv"
        df.to_csv(file_path, index=False)

        print(f"CSV written to {file_path}")
        return file_path

    # select_users_op()의 수행 결과 xcom을 write_users_csv()의 인자로 입력
    run_write_csv = write_users_csv(select_users_op.output)

    # task dependency 설정
    create_table_op >> insert_user_op >> select_users_op >> run_write_csv