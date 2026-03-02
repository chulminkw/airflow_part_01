from airflow.sdk import DAG, task, timezone
from airflow import settings
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime

SOURCE_SCHEMA = "stage"
TARGET_SCHEMA = "summary"

with DAG(
    dag_id="daily_orders_summary_01",
    start_date=datetime(2026, 2, 10, 
                        hour=0, minute=0, second=0, tz="Asia/Seoul"), # orders 테이블에 입력된 order_dt에 맞춰서 재 설정
    schedule="@daily",
    end_date=datetime(2026, 2, 12, 
                      hour=0, minute=0, second=0, tz="Asia/Seoul"), # orders 테이블에 입력된 order_dt에 맞춰서 재 설정
    catchup=True, #catchup을 True로 설정하면 start_date이후로 실행이 되지 않은 Dag Run을 실행 시킴
    tags=["scheduling"],
    max_active_runs = 1 # Dag Run을 동시에 최대 active 1개씩 수행(병렬 수행하지 않음)
) as dag:
    
    @task
    def create_table_if_not():
        print("Airflow core TZ:", settings.TIMEZONE)

        hook = PostgresHook(postgres_conn_id="postgres_delivery_conn")       
        tz = hook.get_first("SHOW timezone;") # SQL 수행 결과의 첫번째 row를 반환. 
        print("DB session timezone:", tz)

        sql = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.orders_daily_summary (
            summary_date DATE PRIMARY KEY, -- 일자
            order_cnt    BIGINT      NOT NULL, -- 일자별 주문 건수
            total_amount NUMERIC(18,2) NOT NULL, -- 일자별 주문 금액
            created_at   TIMESTAMP   DEFAULT NOW(),
            updated_at   TIMESTAMP   DEFAULT NOW()
        );
        """

        hook.run(sql)

    @task
    def load_orders_daily_summary(**context):
        timezone_value = context["dag"].timezone
        data_interval_start = context["data_interval_start"]
        data_interval_end = context["data_interval_end"]
        # PostgresHook 생성시 options에 connection timezone을 설정. 반드시 timezone은 DB 기본 설정과 동일하게 설정 
        hook = PostgresHook(postgres_conn_id="postgres_delivery_conn", 
                            options="-c timezone=Asia/Seoul")
        tz = hook.get_first("SHOW timezone;") # SQL 수행 결과의 첫번째 row를 반환. 

        print(f"####### DAG timezone: {timezone_value}")
        print("DB session timezone:", tz)
        print(f"####### data_interval_start:{data_interval_start}")
        print(f"####### data_interval_end:{data_interval_end}")


        # 멱등성(Idempotency)를 위해 보통은 Upsert(Insert on CONFLICT등) 쿼리를 적용
        sql = f"""
        INSERT INTO {TARGET_SCHEMA}.orders_daily_summary 
        (summary_date, order_cnt, total_amount)
        SELECT 
            DATE(order_dt)   AS summary_date,
            COUNT(*)         AS order_cnt,
            SUM(amount)      AS total_amount
        FROM {SOURCE_SCHEMA}.orders
        -- test를 위해서 timezone을 적용하지 않은 data_interval 값 최초 적용. 이후에 다시 data_interval에 timezone 적용
        WHERE order_dt >= '{data_interval_start.in_timezone(timezone_value)}'
          AND order_dt <  '{data_interval_end.in_timezone(timezone_value)}'
        GROUP BY DATE(order_dt) -- 시간 정보를 제외한 년월일 date type으로 변경후 group by 적용
        ON CONFLICT (summary_date) -- 이미 PK로 데이터가 있어서 INSERT가 실패한 데이터할 경우 아래 Update 수행
        DO UPDATE SET
            order_cnt   = EXCLUDED.order_cnt, --EXCLUDED는 이미 PK로 데이터가 있어서 INSERT가 실패한 데이터
            total_amount = EXCLUDED.total_amount,
            updated_at = NOW();
        
        COMMIT;
        """

        hook.run(sql=sql)

    create_table_if_not() >> load_orders_daily_summary()



"""
아래는 ON CONFLICT를 MERGE로 변경한 SQL. Postgres 15+ 부터 지원

MERGE INTO {TARGET_SCHEMA}.orders_daily_summary AS tgt
USING (
    SELECT 
        DATE(order_dt) AS summary_date,
        COUNT(*)       AS order_cnt,
        SUM(amount)    AS total_amount
    FROM {SOURCE_SCHEMA}.orders
    WHERE order_dt >= '{data_interval_start}'
      AND order_dt <  {data_interval_end}'
    GROUP BY DATE(order_dt)
) AS src
ON tgt.summary_date = src.summary_date
WHEN MATCHED THEN
    UPDATE SET
        order_cnt    = src.order_cnt,
        total_amount = src.total_amount,
        updated_at   = NOW()
WHEN NOT MATCHED THEN
    INSERT (summary_date, order_cnt, total_amount, created_at, updated_at)
    VALUES (src.summary_date, src.order_cnt, src.total_amount, NOW(), NOW());

"""





