from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime

with DAG(
    dag_id="sql_operator_01",
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["operator_hook", "sql"],
) as dag:
    
    # sql인자는 수행할 Query문 문자열, 개별 Query들을 담은 list, Query를 기록한 file명(확장자는 .sql)을 설정할 수 있음
    # Query문을 직접 기술할 때는 docstring이 편리. 
    # Query문내에 주석은 SQL 주석(--, /* */)을 사용
    # 수행할 여러개의 Query들을 []로 담거나 세미콜론(;)을 써서 한꺼번에 기술해도 됨. 
    # 대부분의 RDBMS에서 한번에 여러 Query들을 수행할 수 있게 허용
    # 복잡한 업무로직이 담긴 Query는 Operator당 하나씩 Query를 기술하는 게 바람직하지만
    # 반복적인 테이블들의 DDL이나 단순 DML 시에는 여러개 담아서 한번에 처리하는게 더 편리할 수 있음 
    create_table_op = SQLExecuteQueryOperator(
        task_id="create_users_table",
        conn_id="mysql_ecom_conn",
        sql="""
        -- 기존에 users 테이블이 있으면 drop table 수행
        DROP TABLE IF EXISTS users;

        /* users 테이블이 없으면 users 테이블 생성 */
        CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255),
        age INT,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """  
    )
    # SQLExecuteQueryOperator는 sql인자로 지정된 Query실행 후 종료시 자동 commit 함. 
    # 여러개의 DML Query들을 하나의 SQL로 수행할 때 autocommit와 split_statement 인자에 유의
    # autocommit=True일 경우(default가 False)는 
    # 여러개의 Query들로 구성된 SQL문을 개별적으로 수행할 때마다 commit한다는 의미이며, Connection이 close시 자동 commit을 하겠다는 의미가 아님
    # split_statements=True(default는 False)이면 
    # 여러 Query들로 구성된 SQL문을 세미콜론(;) 기준으로 각각 분리해서 차례로 개별 수행한다는 의미
    # SQL을 수행하는 단위 Task는 멱등성등을 위해 복잡한 transaction 가급적 수행하지 않아야 함.
    # autocommit=False, split_statements=False로 설정과 
    # DBMS별 서로 다른 처리를 예방하기 위해 명시적으로 SQL문장 맨 마지막에 COMMIT을 기재하는 것이 보다 효율적
    
    insert_users_op = SQLExecuteQueryOperator(
        task_id="insert_users",
        conn_id="mysql_ecom_conn",
        sql="""
        INSERT INTO users (name, age) VALUES
        ('Alice', 30),
        ('Bob', 25),
        ('Charlie', 40);

        INSERT INTO users (name, age) VALUES
        ('Min', 50),
        ('Jane', 28),
        ('John', 20);

        COMMIT;
        """
    )

    create_table_op >> insert_users_op