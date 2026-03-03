from airflow.sdk import DAG, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime
import polars as pl
import os

BUCKET_NAME = "airflow-demo" # bucket 명에 "_"(underscore)를 사용해서는 안됨에 유의
MYSQL_OUTPUT_DIR = "/tmp/data/mysql"
FILE_FORMAT = "parquet" # csv
LOCAL_FILENAME = "users.parquet" # users.csv
S3_PREFIX = "db_users"

# Task Dependency
# [create_bucket(), create_local_dir] -> extract_from_mysql() -> load_to_s3() 
# [S3 버킷생성, 임시 디렉토리 생성] -> mysql에서 로컬로 파일 추출 -> 추출된 파일을 s3 upload

with DAG(
    dag_id="mysql_to_s3_01",
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["operator_hook"],
) as dag:
    
    ### 지정된 이름으로 bucket 생성
    @task
    def create_bucket(bucket_name):
        hook = S3Hook(aws_conn_id="minio_conn")

        if hook.check_for_bucket(bucket_name=bucket_name):
            print(f"#### Bucket already exists: {bucket_name}")
        else:
            hook.create_bucket(bucket_name=bucket_name)
            print(f"#### Bucket created: {bucket_name}")

    # 1-1 S3 Bucket 생성
    run_create_bucket = create_bucket(bucket_name=BUCKET_NAME)
    
    ### MySQL용 임시 로컬 디렉토리 생성
    @task
    def create_local_dirs(dir_list: list[str]):
        for dir_name in dir_list:
            os.makedirs(dir_name, exist_ok=True)
    
    # 1-2 MySQL용 로컬 폴더 생성(/tmp/data/mysql)
    run_create_dirs = create_local_dirs(dir_list=[MYSQL_OUTPUT_DIR])
        
    ### MySQL에서 users 테이블의 모든 데이터를 지정된 file명과 format 형식을 가지는 local 파일로 추출
    @task
    def extract_from_mysql(output_filepath, file_format):
        hook = MySqlHook(mysql_conn_id="mysql_ecom_conn")
        raw_conn_uri = hook.get_uri()
        print(f"#### uri:{raw_conn_uri}, type:{type(raw_conn_uri)}")
        # connectorx는 connection에 있는 추가 필드인 charset=...를 해석하지 못함
        conn_uri = raw_conn_uri.split("?")[0]

        # read_database_uri()는 기본적으로 connectorx를 엔진으로 사용하여 connection용 uri 문자열을 입력 받음.
        polars_df = pl.read_database_uri(
            query="select * from users",
            uri=conn_uri,
            engine="connectorx", # engine인자는 기본 connectorx
            pre_execution_query="SET NAMES utf8mb4" # client session레벨 utf8mb4 설정
        )
        
        # file_format에 따라 csv 또는 parquet으로 설정
        if file_format == "csv":
            polars_df.write_csv(output_filepath)
        elif file_format == "parquet":
            polars_df.write_parquet(output_filepath)
    
    # 2. mysql에서 로컬로 파일 추출
    mysql_output_path=f"{MYSQL_OUTPUT_DIR}/{LOCAL_FILENAME}"
    run_extract_mysql = extract_from_mysql(output_filepath=mysql_output_path, 
                                           file_format=FILE_FORMAT)
    
    ### full path를 가지는 local 파일을 S3 지정된 bucket과 object key로 Upload
    @task
    def load_to_s3(local_filepath, bucket_name, object_key):
        hook = S3Hook(aws_conn_id="minio_conn")
        
        hook.load_file(
            filename=local_filepath, # filename은 로컬 파일명
            key=object_key, # bucket내의 object key값
            bucket_name=bucket_name, # 로컬 파일을 upload할 Object Storage 버킷명
            replace=True, #해당 object가 이미 존재할 경우 replace 여부. False이면 기존에 Object가 있을 경우 오류 발생.
        )

        print(f"#### Uploaded {local_filepath} to s3://{bucket_name}/{object_key}")

    # 3. 추출된 파일을 s3 upload
    object_key = f"{S3_PREFIX}/{LOCAL_FILENAME}"
    run_upload = load_to_s3(local_filepath=mysql_output_path, 
                            bucket_name=BUCKET_NAME, object_key=object_key)
    
    # 아래는 taskflow간 dependency 설정. 
    [run_create_bucket, run_create_dirs] >> run_extract_mysql >> run_upload
    