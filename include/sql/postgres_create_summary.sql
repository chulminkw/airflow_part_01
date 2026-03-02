-- schema명을 변수 처리하는 것이 반드시 필요한 것은 아님.
-- summary.orders_daily_summary_for_template 테이블이 존재하지 않으면 생성
CREATE TABLE IF NOT EXISTS {{ params.tgt_schema }}.orders_daily_summary_for_template (
summary_date DATE PRIMARY KEY, -- 일자
order_cnt    BIGINT      NOT NULL, -- 일자별 주문 건수
total_amount NUMERIC(18,2) NOT NULL, -- 일자별 주문 금액
created_at   TIMESTAMPTZ   DEFAULT NOW(),
updated_at   TIMESTAMPTZ   DEFAULT NOW()
);