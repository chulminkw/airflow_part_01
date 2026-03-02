from airflow.sdk import DAG, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
import pendulum

SOURCE_SCHEMA = "stage"
TARGET_SCHEMA = "summary"
TARGET_TABLE = "orders_daily_summary_interval_wd" # weekday용 summary table 신규 생성
LOCAL_TZ = "Asia/Seoul" # 또는 pendulum.timezone("ASIA/SEOUL")

# 아래 수행전 가급적 TARGET_TABLE을 미리 Drop. 
# DAG를 지우고, 실습 코드를 변경하면서 다시 테스트 해볼 경우는 기존 들어가 있는 데이터를 완전 삭제하기 위해서 필요.
with DAG(
    dag_id="cron_interval_wd_01",
    start_date=datetime(2026, 1, 1, 
                        hour=0, minute=0, second=0, tz=LOCAL_TZ), # 영상을 보시는 날 이전으로 주말이 포함될 수 있게 설정해 주세요
    #day of week: 일요일:0, 월요일:1,..... 금요일:5, 토요일:6 
    schedule=CronDataIntervalTimetable("0 0 * * 2-6", timezone=LOCAL_TZ), # 먼저 1-5 적용 후, 2-6 적용.DAG start_date의 tz와 동일한 timezone 설정.
    catchup=True, # 실습을 위해서 start_date가 과거 시간으로 설정하고 catchup을 True로 설정 
    end_date=datetime(2026, 1, 6,
                      hour=23, minute=59, second=59, tz=LOCAL_TZ),
    tags=["scheduling"],
    max_active_runs = 2,
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
        data_interval_start = context["data_interval_start"]
        data_interval_end = context["data_interval_end"]
        logical_date_tz = context["logical_date"].in_timezone(timezone_value)
        # PostgresHook 생성시 options에 connection timezone을 설정. 반드시 timezone은 DB 기본 설정과 동일하게 설정 
        hook = PostgresHook(postgres_conn_id="postgres_delivery_conn", 
                            options="-c timezone=Asia/Seoul")
        db_session_tz = hook.get_first("SHOW timezone;") # SQL 수행 결과의 첫번째 row를 반환. 

        print(f"####### DAG timezone: {timezone_value}")
        print("DB session timezone:", db_session_tz)
        print(f"####### data_interval in UTC:{data_interval_start} ~ {data_interval_end}")
        d_start_tz = data_interval_start.in_timezone(timezone_value)
        d_end_tz = data_interval_end.in_timezone(timezone_value)
        print(f"####### data_interval in {timezone_value}:{d_start_tz} ~ {d_end_tz}")
        print(f"####### logical_date in {timezone_value}:{logical_date_tz}")

        # 멱등성(Idempotency)를 위해 보통은 Upsert(Insert on CONFLICT등) 쿼리를 적용
        sql = f"""
        INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE}
        (summary_date, order_cnt, total_amount)
        SELECT 
            DATE(order_dt)   AS summary_date,
            COUNT(*)         AS order_cnt,
            SUM(amount)      AS total_amount
        FROM {SOURCE_SCHEMA}.orders
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

    # task dependency 설정
    create_table_if_not() >> load_orders_daily_summary()
