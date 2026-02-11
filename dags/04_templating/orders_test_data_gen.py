from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
from pendulum import datetime, duration
import pendulum

import random

MYSQL_CONN_ID = "mysql_conn_01"
ORDER_STATUSES = ["CREATED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"]

@dag(
    dag_id="orders_test_data_gen",
    schedule="@once",        # manual trigger only
    catchup=False,
    tags=["templating", "data_gen"],
)
def orders_test_data_generator():

    @task
    def drop_create_orders_table():
        """
        Test용: 테이블 Drop & Create
        """
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        sql = """
            DROP TABLE IF EXISTS orders;

            -- orders table 생성. 
            CREATE TABLE IF NOT EXISTS orders  (
                order_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                order_dt TIMESTAMP,
                order_status VARCHAR(20) NOT NULL,
                total_amount DECIMAL(10,2),
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX orders_order_dt_idx on orders(order_dt);
            CREATE INDEX orders_created_at_idx on orders(created_at);
            CREATE INDEX orders_updated_at_idx on orders(updated_at);
        """
        hook.run(sql)

    @task
    def insert_orders_from_range():
        """
        Insert 1 order per 3-minute interval
        Range is read from Airflow Variables
        """
        start_dt_str = Variable.get("orders_start_dt")
        end_dt_str = Variable.get("orders_end_dt")
        print(f"##### start_dt_str:{start_dt_str}, end_dt_str:{end_dt_str}")

        start_dt = pendulum.from_format(start_dt_str, 'YYYY-MM-DDTHH:mm:ss')
        end_dt = pendulum.from_format(end_dt_str, 'YYYY-MM-DDTHH:mm:ss')

        if start_dt >= end_dt:
            raise ValueError("orders_start_dt must be earlier than orders_end_dt")

        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

        insert_sql = """
            INSERT INTO orders (
                user_id,
                order_dt,
                order_status,
                total_amount,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        rows = []
        current = start_dt

        while current < end_dt:
            rows.append((
                random.randint(1, 10000), # user_id
                current, # order_dt
                random.choice(ORDER_STATUSES), #order_status
                round(random.uniform(10, 500), 2), #total_amout
                current, #created_at
                current, #updated_at
            ))
            # 3초 간격으로 order_dt, create_at, updated_at 변경하여 입력할 수 있도록 current 수정.
            current += duration(minutes=3)
            
        # Hook의 insert_rows() 메소드로 한번에 대량 insert 수행. 
        hook.insert_rows(
            table="orders",
            rows=rows,
            target_fields=[
                "user_id",
                "order_dt",
                "order_status",
                "total_amount",
                "created_at",
                "updated_at",
            ],
        )

        return f"Inserted {len(rows)} rows"

    run_drop_create = drop_create_orders_table()
    run_insert = insert_orders_from_range()

    run_drop_create >> run_insert

dag = orders_test_data_generator()
