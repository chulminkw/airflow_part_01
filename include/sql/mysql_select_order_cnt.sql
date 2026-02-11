-- 주석은 sql 주석인 -- 또는 /* */
-- 여러 sql 문장들의 경우 개별 sql은 ; 반드시 적용.
SELECT count(*) as cnt
FROM orders
WHERE order_dt >= '{{ data_interval_start }}'
AND order_dt <  '{{ data_interval_end }}';