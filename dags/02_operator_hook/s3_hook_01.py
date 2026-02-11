from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime
import os

BUCKET_NAME = "airflow-demo" # bucket 명에 "_"(underscore)를 사용해서는 안됨에 유의
LOCAL_FILE = "/usr/local/airflow/include/user_data/sample_hook.txt" # 로컬 파일은 worker가 수행되는 서버의 파일이어야 함(여기서는 scheduler container) 
S3_RAW_KEY_PREFIX = "raw"
S3_COPY_KEY_PREFIX = "processed"

with DAG(
    dag_id="s3_hook_01",
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["operator_hook", "s3"],
):
    @task(retries=2)
    def create_bucket(bucket_name: str):
        hook = S3Hook(aws_conn_id="minio_conn")

        if hook.check_for_bucket(bucket_name):
            print(f"#### Bucket already exists: {bucket_name}")
        else:
            hook.create_bucket(bucket_name=bucket_name)
            print(f"#### Bucket created: {bucket_name}")

    @task
    def upload_file(local_filename: str, bucket_name: str, dest_key: str):
        hook = S3Hook(aws_conn_id="minio_conn")
        
        hook.load_file(
            filename=local_filename, # filename은 로컬 파일명
            key=dest_key, # bucket내의 object key값
            bucket_name=bucket_name, # 로컬 파일을 upload할 Object Storage 버킷명
            replace=True, #해당 object가 이미 존재할 경우 replace 여부. False이면 기존에 Object가 있을 경우 오류 발생.
        )

        print(f"#### Uploaded {local_filename} to {bucket_name}/{dest_key}")

    @task
    def list_object_keys(bucket_name: str, prefix: str = None) -> list[str]:
        hook = S3Hook(aws_conn_id="minio_conn")

        keys = hook.list_keys(
            bucket_name=bucket_name,
            prefix=prefix,
        )

        print(f"#### Objects in bucket {bucket_name}: {keys}")
        return keys
    
    @task
    def copy_object_file(source_bucket_name, source_key, dest_bucket_name, dest_key):
        hook = S3Hook(aws_conn_id="minio_conn")

        hook.copy_object(
            source_bucket_key=source_key, # source object가 있는 bucket내의 key값. 
            dest_bucket_key=dest_key, # destination object의 bucket내 key값
            source_bucket_name=source_bucket_name, # source object의 bucket명. 
            dest_bucket_name=dest_bucket_name, # destination object bucket내 key값
        )

        print(f"#### Copied {source_bucket_name}/{source_key} to {dest_bucket_name}/{dest_key}")

    # 아래는 taskflow 를 DAG에 등록
    run_create_bucket = create_bucket(BUCKET_NAME)

    s3_raw_key = f"{S3_RAW_KEY_PREFIX}/{os.path.basename(LOCAL_FILE)}"
    run_upload = upload_file(LOCAL_FILE, BUCKET_NAME, s3_raw_key)
    run_list = list_object_keys(BUCKET_NAME, prefix=S3_RAW_KEY_PREFIX)
    
    s3_copy_key = f"{S3_COPY_KEY_PREFIX}/{os.path.basename(LOCAL_FILE)}"
    run_copy = copy_object_file(BUCKET_NAME, s3_raw_key, BUCKET_NAME, s3_copy_key)

    # 아래는 taskflow간 dependency 설정. 
    run_create_bucket >> run_upload >> run_list >> run_copy
