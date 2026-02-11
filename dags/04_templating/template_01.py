from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from pendulum import datetime

LOCAL_FILE = "/usr/local/airflow/include/user_data/template_test_{{ ds_nodash }}.txt"

with DAG(
    dag_id="template_01",
    start_date=datetime(2025, 12,4),
    schedule=None,
    catchup=False,
) as dag:
    print_template_task = BashOperator(
        task_id="print_template",
        bash_command="""
        echo "Date: {{ ds }}"
        echo "Date-Nodash: {{ ds_nodash }}"
        echo "Timestamp: {{ ts }}"
        echo "Timestamp-Nodash: {{ ts_nodash }}"
        echo "logical_date: {{ logical_date }}"
        """
    )

    upload_to_minio_op = LocalFilesystemToS3Operator(
        task_id="upload_local_file",
        filename=LOCAL_FILE, # LOCAL_FILE 변수에 template 할당됨
        dest_key="demo/{{ ds_nodash }}/template_test.txt", # dest_key는 template field임. 
        dest_bucket="mybucket", # load될 bucket 명
        # aws_conn_id="minio_conn" # 아이디/패스워드 권한을 통한 connection
        aws_conn_id="minio_accesskey_conn", # access key를 통한 connection
        replace=True # bucket에 해당 파일이 있으면 replace 수행. default는 False
    )

    @task
    def python_no_context():
        # 아래는 {{ ds }} 를 template engine에 전달하지 않으므로 문자열 {{ ds }} 그대로 출력함
        print("#### template {{ ds }} value ")
        print("#### LOCAL_FILE 변수값:", LOCAL_FILE)

    @task
    def python_with_context(**context):
        print(f"#### template {context['ds']}")
    
    print_context_01_task = python_no_context()
    print_context_02_task = python_with_context() # context 인자값을 넣지 않아도 됨. 


    [ print_template_task, upload_to_minio_op, print_context_01_task, print_context_02_task  ]