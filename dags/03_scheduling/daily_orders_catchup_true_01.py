from airflow.sdk import DAG, task, timezone
from airflow import settings
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime

SOURCE_SCHEMA = "stage"
TARGET_SCHEMA = "summary"
TARGET_TABLE = "orders_daily_summary_catchup" # catchup용 summary table 신규 생성

with DAG(
    dag_id="daily_orders_catchup_true_01",
    start_date=datetime(2026, 2, 1, 
                        hour=0, minute=0, second=0, tz="Asia/Seoul"), # 영상을 보시는 날 며칠 이전으로 설정해 주세요
    schedule="@daily", 
    catchup=True, #catchup을 True로 설정하면 start_date이후로 실행이 되지 않은 Dag Run을 실행 시킴
    tags=["scheduling"],
    max_active_runs = 2 # 동시에 수행하는 DAG Run 수(자원 소모들을 감안하여 가급적 작게 설정)
) as dag:
    
    @task
    def create_table_if_not():
        print("Airflow core TZ:", settings.TIMEZONE)

        hook = PostgresHook(postgres_conn_id="postgres_delivery_conn")       
        tz = hook.get_first("SHOW timezone;") # SQL 수행 결과의 첫번째 row를 반환. 
        print("DB session timezone:", tz)

        sql = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} (
            summary_date DATE PRIMARY KEY, -- 일자
            order_cnt    BIGINT      NOT NULL, -- 일자별 주문 건수
            total_amount NUMERIC(18,2) NOT NULL, -- 일자별 주문 금액
            created_at   TIMESTAMPTZ   DEFAULT NOW(),
            updated_at   TIMESTAMPTZ   DEFAULT NOW()
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
        INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE}
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

    # initialize_orders
    create_table_if_not() >> load_orders_daily_summary()
