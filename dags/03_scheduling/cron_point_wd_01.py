from airflow.sdk import DAG, task
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
import pendulum

SOURCE_SCHEMA = "stage"
TARGET_SCHEMA = "summary"
TARGET_TABLE = "orders_daily_summary_point_wd" # weekday용 summary table 신규 생성
LOCAL_TZ = "Asia/Seoul" # 또는 pendulum.timezone("ASIA/SEOUL")

with DAG(
    dag_id="cron_point_wd_01",
    start_date=datetime(2026, 2, 13, 
                        hour=0, minute=0, second=0, tz=LOCAL_TZ), # 영상을 보시는 날 이전으로 주말이 포함될 수 있게 설정해 주세요
    schedule=CronTriggerTimetable("0 0 * * 2-6"), # DAG start_date의 tz와 동일한 timezone 설정.
    catchup=True, # 실습을 위해서 start_date가 과거 시간으로 설정하고 catchup을 True로 설정 
    tags=["scheduling"],
    max_active_runs = 2
) as dag:
    
    @task
    def create_table_if_not():
        hook = PostgresHook(postgres_conn_id="postgres_delivery_conn",
                            options="-c timezone=Asia/Seoul")       

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
        logical_date = context["logical_date"]
        data_interval_start = context["data_interval_start"]
        data_interval_end = context["data_interval_end"]
        print("\n========== logical_date, data_interval_start/end  ==========")
        print(f"logical_date:{logical_date}, data_interval_start:{data_interval_start}, data_interval_end:{data_interval_end}")
        
        # PostgresHook 생성시 options에 connection timezone을 설정. 반드시 timezone은 DB 기본 설정과 동일하게 설정 
        hook = PostgresHook(postgres_conn_id="postgres_delivery_conn", 
                            options=f"-c timezone={LOCAL_TZ}")
         
        print(f"####### DAG timezone: {timezone_value}")
        # CronTriggerTable은 data_interval_start, data_interval_end를 적용해서는 안됨
        # 기존 data_interval_start는 logical_date_tz에서 하루 이전이 되어야 함. 
        logical_date_tz = context["logical_date"].in_timezone(timezone_value)
        p_order_dt_start = logical_date_tz.subtract(days=1)
        p_order_dt_end = logical_date_tz
        print("\n========== logical_date_tz, p_order_dt_start/end  ==========")
        print(f"logical_date_tz: {logical_date_tz}")
        print(f"p_order_dt_start:{p_order_dt_start}")
        print(f"p_order_dt_end:{p_order_dt_end}")
        
        # 멱등성(Idempotency)를 위해 보통은 Upsert(Insert on CONFLICT등) 쿼리를 적용
        sql = f"""
        INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE}
        (summary_date, order_cnt, total_amount)
        SELECT 
            DATE(order_dt)   AS summary_date,
            COUNT(*)         AS order_cnt,
            SUM(amount)      AS total_amount
        FROM {SOURCE_SCHEMA}.orders
        WHERE order_dt >= '{p_order_dt_start}'
         AND order_dt <  '{p_order_dt_end}'
        GROUP BY DATE(order_dt) -- 시간 정보를 제외한 년월일 date type으로 변경후 group by 적용
        ON CONFLICT (summary_date) -- 이미 PK로 데이터가 있어서 INSERT가 실패한 데이터할 경우 아래 Update 수행
        DO UPDATE SET
            order_cnt   = EXCLUDED.order_cnt, --EXCLUDED는 이미 PK로 데이터가 있어서 INSERT가 실패한 데이터
            total_amount = EXCLUDED.total_amount,
            updated_at = NOW();
        
        COMMIT;
        """

        hook.run(sql=sql)

    # task dependency 설정
    create_table_if_not() >> load_orders_daily_summary()
