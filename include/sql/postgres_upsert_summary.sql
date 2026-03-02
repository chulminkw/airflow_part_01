-- schema명을 변수 처리하는 것이 반드시 필요한 것은 아님.
-- summary.orders_daily_summary_for_template 테이블에 upsert 실행
INSERT INTO  {{ params.tgt_schema }}.orders_daily_summary_for_template
(summary_date, order_cnt, total_amount)
SELECT 
    DATE(order_dt)   AS summary_date,
    COUNT(*)         AS order_cnt,
    SUM(amount)      AS total_amount
FROM {{ params.src_schema }}.orders
WHERE order_dt >= '{{ data_interval_start.in_timezone(params.tz) }}' 
AND order_dt < '{{ data_interval_end.in_timezone(params.tz) }}'
GROUP BY DATE(order_dt) -- 시간 정보를 제외한 년월일 date type으로 변경후 group by 적용
ON CONFLICT (summary_date) -- 이미 PK로 데이터가 있어서 INSERT가 실패한 데이터할 경우 아래 Update 수행
DO UPDATE SET
order_cnt   = EXCLUDED.order_cnt, --EXCLUDED는 이미 PK로 데이터가 있어서 INSERT가 실패한 데이터
total_amount = EXCLUDED.total_amount,
updated_at = NOW();
        
COMMIT;