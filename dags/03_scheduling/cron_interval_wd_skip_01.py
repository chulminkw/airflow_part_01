from airflow.sdk import DAG, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from pendulum import datetime
import pendulum

SOURCE_SCHEMA = "stage"
TARGET_SCHEMA = "summary"
TARGET_TABLE = "orders_daily_summary_interval_wd_skip" # weekday용 summary table 신규 생성
LOCAL_TZ = "Asia/Seoul"

# 주말외에 수행되지 말아야 할 공휴일(보통은 DB로 관리)
# 아래는 2026년 설날 일자
DO_NOT_RUN_HOLIDAYS = [
    datetime(2026, 2, 16, hour=0, minute=0, second=0, tz=LOCAL_TZ ),
    datetime(2026, 2, 17, hour=0, minute=0, second=0, tz=LOCAL_TZ ),
    datetime(2026, 2, 18, hour=0, minute=0, second=0, tz=LOCAL_TZ )              
]

with DAG(
    dag_id="cron_interval_wd_skip_01",
    start_date=datetime(2026, 2, 6, 
                        hour=0, minute=0, second=0, tz=LOCAL_TZ ), # 영상을 보시는 날 이전으로 주말이 포함될 수 있게 설정해 주세요
    # schedule=CronDataIntervalTimetable("0 0 * * 2-6", timezone=LOCAL_TZ), # DAG start_date의 tz와 동일한 timezone 설정.
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=LOCAL_TZ), # cron 주중 로직을 interval timetable에서 data_interval_end가 주말을 포함하게됨. 
    catchup=True, # 실습을 위해서 start_date가 과거 시간으로 설정하고 catchup을 True로 설정 
    end_date=datetime(2026, 2, 20,
                      hour=23, minute=59, second=59, tz=LOCAL_TZ), # 실습을 위해서 end_date 설정
    tags=["scheduling"],
    max_active_runs = 2
) as dag:
    
    @task
    def check_if_run(**context):
        timezone_value = context["dag"].timezone
        logical_date_tz = context["logical_date"].in_timezone(timezone_value)       
        print(f"####### logical_date in {timezone_value}:{logical_date_tz}")
        # pendulum 3.x 부터는 python 내장 datetime가 동일하게 day_of_week값이 월요일이 0, 일요일이 1임.
        print(f"#### pendulum version:{pendulum.__version__}")
        print(f"#### type logical_date:{type(logical_date_tz)}, logical_date_tz.day_of_week:{logical_date_tz.day_of_week}")
        
        # 만약 logical_date가 주중이 아니면 아래는 AirflowSkipException을 발생 시키고 더 이상 task를 수행하지 않음
        # AirflowSkipException을 발생 시키면 해당 task는 skip가 되며, 이어서 수행되는 downstream task도 skipped가 됨. 
        # pendulum 3.x 부터는 2.x와 day_of_week에서 더 이상 일요일이 0이 아니라, 월요일이 0임. 아래와 같이 상수로 변경 적용.
        if logical_date_tz.day_of_week in [pendulum.SATURDAY, pendulum.SUNDAY]:
            raise AirflowSkipException(f"logical_date_tz {logical_date_tz}가 주중이 아니라 주말입니다. 작업을 건너뜁니다")
        if logical_date_tz in DO_NOT_RUN_HOLIDAYS:
            raise AirflowSkipException(f"logical_date_tz {logical_date_tz}가 수행되지 말아야 할 Holiday입니다. 작업을 건너뜁니다")

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

    # 작업을 skip할지를 check하는 check_if_run() 추가
    check_if_run() >> create_table_if_not() >> load_orders_daily_summary()
