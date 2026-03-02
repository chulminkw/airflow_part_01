from airflow.sdk import DAG, task
from pendulum import datetime

with DAG(
    dag_id="task_flow_sample_02", # dag_id는 airflow 내에서 반드시 고유한 값을 가져야 함.  
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"), # DAG의 시작 날짜/시간 
    schedule=None, # 수행 주기
    catchup=False, # 수행되지 않은 과거 Dag run 스케쥴 수행 여부
    tags=["fundamental", "task_flow_api"]  # dag의 tag
) as dag:
    
    @task
    def extract_from_source():
        import random
        
        print("#### extracting data from source system....")
        rows = random.randint(1000, 5000)
        return {
            "rows": rows,
            "source_table": "order_tb"
        }
    
    @task
    def load_to_stage(extract_meta: dict):
        load_table = "stage_order_tb"
        schema = "stage"
        print(f"#### source rows:{extract_meta["rows"]}, source table:{extract_meta["source_table"]}")
        print(f"#### loading raw data into DW Stage {schema}.{load_table}")

        return {
            "schema": schema,
            "load_table": load_table
        }
    
    @task
    def transform_in_dw(load_meta: dict):
        final_table = "order_fact"
        schema = "dw"
        print(f"#### load table name in transform:{load_meta['load_table']}")
        
        return final_table
    
    # return 값이 있는(XCom 값) taskflow api의 dependency 설정. 
    extract_task = extract_from_source() # 자동으로 자신의 task 수행후 return값을 xcom으로 push 
    load_task = load_to_stage(extract_task) # 자동으로 extract_from_source의 xcom 값을 pull로 가져와서 수행 후, task return 값을 xcom으로 push
    final_task = transform_in_dw(load_task) # 자동으로 load_to_stage의 xcom 값을 pull로 가져와서 자신의 task 수행후, task return 값을 xcom으로 push
    
    # 또는 아래와 같이 task flow 함수들을 chain 형태로 연속적으로 음.
    # transform_in_dw(load_to_stage(extract_from_source()))


