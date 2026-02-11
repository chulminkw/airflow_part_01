from airflow.sdk import DAG
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3ListOperator, S3CopyObjectOperator
)
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from pendulum import datetime
import os

BUCKET_NAME = "airflow-demo"
LOCAL_FILE = "/usr/local/airflow/include/user_data/sample.txt"
S3_RAW_KEY_PREFIX = "raw"
S3_COPY_KEY_PREFIX = "processed"

with DAG(
    dag_id="s3_operator_01",
    start_date=datetime(2026, 2, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["operator_hook", "s3"],
):
    run_create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        aws_conn_id="minio_conn",
        bucket_name=BUCKET_NAME,
        retries=2
    )

    raw_key = f"{S3_RAW_KEY_PREFIX}/{os.path.basename(LOCAL_FILE)}"
    
    run_upload = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        aws_conn_id="minio_conn",
        filename=LOCAL_FILE, # filename은 로컬 파일명
        dest_bucket=BUCKET_NAME,  # 로컬 파일을 upload할 Object Storage 버킷명
        dest_key=raw_key, # dest_bucket내의 object key값
        replace=True # 해당 object가 이미 존재할 경우 replace 여부. False이면 기존에 Object가 있을 경우 오류 발생. 
    )

    run_list = S3ListOperator(
        task_id="list_keys",
        aws_conn_id="minio_conn",
        bucket=BUCKET_NAME,
        prefix=S3_RAW_KEY_PREFIX
    )
    
    copy_key = f"{S3_COPY_KEY_PREFIX}/{os.path.basename(LOCAL_FILE)}"

    run_copy = S3CopyObjectOperator(
        task_id="copy_object",
        aws_conn_id="minio_conn",
        source_bucket_name=BUCKET_NAME,
        source_bucket_key=raw_key,
        dest_bucket_name=BUCKET_NAME,
        dest_bucket_key=copy_key
    )
   

    # 아래는 task dependency 설정. 
    run_create_bucket >> run_upload >> run_list >> run_copy
