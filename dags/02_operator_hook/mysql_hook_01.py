from airflow.sdk import DAG, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pendulum import datetime
import pandas as pd
import os

OUTPUT_DIR = "/tmp/mysql_load"

with DAG(
    dag_id="mysql_hook_01",
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["operator_hook", "sql"],
) as dag:

    @task
    def create_table():
        hook = MySqlHook(mysql_conn_id="mysql_ecom_conn")

        # Hook을 사용할 경우 sql을 파일로 바로 설정할 수 없음. 
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
        hook.run(sql=sql) 
        
    @task
    def insert_users():
        hook = MySqlHook(mysql_conn_id="mysql_ecom_conn")
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
        hook.run(sql=sql)

    @task
    def select_and_write_csv():
        hook = MySqlHook(mysql_conn_id="mysql_ecom_conn")
        sql = """
        SELECT id, name, age, created_at, updated_at
        FROM users;
        """
        # MysqlHook은 pandas와 잘 통합되어 get_pandas_df(sql)을 수행하면 sql을 수행 후 결과를 pandas로 반환
        df = hook.get_pandas_df(sql)
        # 또는 아래와 같이 MysqlHook에서 db connection 객체를 가져 온 뒤,
        # pandas의 read_sql()에 수행 sql와 db connection 객체를 입력 하여, 
        # 해당 sql를 수행하고 결과를 DataFrame에 반환함. 대용량 Select시 메모리 과다 소모 유의
        # conn = hook.get_conn()   # DB-API connection
        # df = pd.read_sql(sql, conn)

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        file_path = f"{OUTPUT_DIR}/users_mysql_hook_01.csv"
        df.to_csv(file_path, index=False)

        # hook.get_conn()을 수행하였으면 아래 주석해제 하여 connection을 close 시킴
        # conn.close()

        print(f"CSV written to {file_path}")
        return file_path
    
    create_table() >> insert_users() >> select_and_write_csv()
