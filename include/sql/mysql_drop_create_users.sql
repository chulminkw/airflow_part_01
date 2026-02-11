-- 한글이 포함된 경우, 파일은 utf-8 인코딩 되어 있어야 함.
-- 주석은 sql 주석인 -- 또는 /* */
-- 여러 sql 문장들의 경우 개별 sql은 ; 반드시 적용.
-- 기존에 users 테이블이 있으면 drop table 수행
DROP TABLE IF EXISTS users;

/* users 테이블이 없으면 users 테이블 생성 */
CREATE TABLE IF NOT EXISTS users (
id INT AUTO_INCREMENT PRIMARY KEY,
name VARCHAR(255),
age INT,
created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
); 