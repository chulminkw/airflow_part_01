
/* serialization 관련 메타 테이블 조회 */ 
create temp table dag_temp
as
select a.* 
from dag a 
where a.dag_id in (select x.dag_id from dag_tag x where name='fundamental')
;

select * from serialized_dag a where dag_id in (select dag_id from dag_temp);

select * from dag_code where dag_id in (select dag_id from dag_temp);

select * from dag_version where dag_id in (select dag_id from dag_temp);


/************************************************** 
1. generate_orders_data 프로시저:
아래는 MySQL에서 orders 테이블을 Drop/Create 후 입력 받은 시작 시간과 종료 시간 동안 
3분 주기로 random 데이터를 입력하는 프로시저

2. 반드시 ecom DB에 접속하여 수행. 수행은 아래와 같음
-- 2026년 2월 1일 0시 0분 0초부터 2월 2일 23시 59분 59초까지 3분 간격 데이터 생성
CALL generate_orders_data('2026-02-01 00:00:00', '2026-02-02 23:59:59');

-- 생성된 데이터 확인
SELECT COUNT(*) FROM orders;

SELECT 
    DATE(order_dt) AS order_date, 
    COUNT(*) AS daily_order_count,
    SUM(total_amount) AS daily_revenue
FROM orders
GROUP BY DATE(order_dt)
ORDER BY order_date;
***************************************************/

DELIMITER $$

CREATE PROCEDURE generate_orders_data(
    IN p_start_dt DATETIME,
    IN p_end_dt DATETIME
)
BEGIN
    DECLARE v_current_dt DATETIME;
    DECLARE v_user_id BIGINT;
    DECLARE v_order_status VARCHAR(20);
    DECLARE v_total_amount DECIMAL(10,2);
    DECLARE v_status_idx INT;

    -- 1. 테이블 초기화
    DROP TABLE IF EXISTS orders;

    CREATE TABLE orders (
        order_id BIGINT AUTO_INCREMENT PRIMARY KEY,
        user_id BIGINT NOT NULL,
        order_dt TIMESTAMP,
        order_status VARCHAR(20) NOT NULL,
        total_amount DECIMAL(10,2),
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX orders_order_dt_idx ON orders(order_dt);
    CREATE INDEX orders_created_at_idx ON orders(created_at);
    CREATE INDEX orders_updated_at_idx ON orders(updated_at);

    -- 2. 데이터 생성 루프
    SET v_current_dt = p_start_dt;

    -- 트랜잭션 시작
    START TRANSACTION;

    -- 시작 시간이 종료 시간보다 늦을 경우 예외 처리 대신 바로 종료
    WHILE v_current_dt < p_end_dt DO
        
        -- 임의의 user_id (1 ~ 10000)
        SET v_user_id = FLOOR(1 + (RAND() * 10000));
        
        -- 임의의 total_amount (10.00 ~ 500.00)
        SET v_total_amount = ROUND(10 + (RAND() * 490), 2);
        
        -- 임의의 order_status 선택
        SET v_status_idx = FLOOR(RAND() * 5);
        SET v_order_status = CASE v_status_idx
            WHEN 0 THEN 'CREATED'
            WHEN 1 THEN 'PAID'
            WHEN 2 THEN 'SHIPPED'
            WHEN 3 THEN 'DELIVERED'
            ELSE 'CANCELLED'
        END;

        -- 데이터 INSERT
        INSERT INTO orders (
            user_id, 
            order_dt, 
            order_status, 
            total_amount, 
            created_at, 
            updated_at
        ) 
        VALUES (
            v_user_id, 
            v_current_dt, 
            v_order_status, 
            v_total_amount, 
            v_current_dt, 
            v_current_dt
        );

        -- 3분 간격 추가
        SET v_current_dt = DATE_ADD(v_current_dt, INTERVAL 3 MINUTE);
        
    END WHILE;
    COMMIT;

END $$

DELIMITER ;

