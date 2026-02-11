from airflow.sdk import DAG, task, get_current_context
from pendulum import datetime

def print_context_info(dag_id_arg, timezone_value, 
                       dag_run_id_arg, run_id_arg, run_type_arg,
                       task_id_arg, logical_date_arg):
    
    print("========== DAG & DAG RUN ==========")
    print(f"DAG id: {dag_id_arg}, DAG timezone: {timezone_value}")
    print(f"DAG Run ID: {dag_run_id_arg}, {run_id_arg}, DAG 수행 유형:{run_type_arg}")

    print("\n========== Task Instance & Scheduling  ==========")
    print(f"task id: {task_id_arg}")
    print(f"logical_date: {logical_date_arg}, type:{type(logical_date_arg)}")
    print(f"logical_date in timezone: {logical_date_arg.in_timezone('Asia/Seoul')}")

with DAG(
    dag_id="task_context_info_01",
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False, #catchup을 True로 설정하면 start_date이후로 실행이 되지 않은 Dag Run을 실행 시킴
    tags=["fundamental"],
) as dag:
    # 아래는 taskflow 함수에 kwargs 주입 방식으로 context 변수들의 값을 확인
    @task
    def get_context_by_kwargs(**context):
        # DAG 관련 정보
        dag = context["dag"]
        dag_id = dag.dag_id
        timezone_value = dag.timezone
        
        # DagRun 관련 정보
        dag_run = context['dag_run']
        dag_run_id = dag_run.run_id
        run_type = dag_run.run_type
        run_id = context["run_id"]
        
        # task instance 정보
        ti = context['ti']
        task_id = ti.task_id
        
        # scheduling 관련 정보
        logical_date = context["logical_date"]# DAG Scheduling 수행 interval의 시작 date

        print_context_info(dag_id_arg=dag_id, timezone_value=timezone_value, 
                       dag_run_id_arg=dag_run_id, run_id_arg=run_id, run_type_arg=run_type,
                       task_id_arg=task_id, logical_date_arg=logical_date)
        
    @task
    def get_context_by_direct_args(dag, dag_run, run_id, ti, logical_date):
        dag_id = dag.dag_id
        timezone_value = dag.timezone

        dag_run_id = dag_run.run_id
        run_type = dag_run.run_type
        
        task_id = ti.task_id

        print_context_info(dag_id_arg=dag_id, timezone_value=timezone_value, 
                       dag_run_id_arg=dag_run_id, run_id_arg=run_id, run_type_arg=run_type,
                       task_id_arg=task_id, logical_date_arg=logical_date)
        
    @task
    def get_context_by_context_fn():
        #from airflow.sdk import get_current_text
        context = get_current_context()
        # DAG 관련 정보
        dag = context["dag"]
        dag_id = dag.dag_id
        timezone_value = dag.timezone
        
        # DagRun 관련 정보
        dag_run = context['dag_run']
        dag_run_id = dag_run.run_id
        run_type = dag_run.run_type
        run_id = context["run_id"]
        
        # task instance 정보
        ti = context['ti']
        task_id = ti.task_id
        
        # scheduling 관련 정보
        logical_date = context["logical_date"]# DAG Scheduling 수행 interval의 시작 date

        print_context_info(dag_id_arg=dag_id, timezone_value=timezone_value, 
                       dag_run_id_arg=dag_run_id, run_id_arg=run_id, run_type_arg=run_type,
                       task_id_arg=task_id, logical_date_arg=logical_date)

    # 호출 시 keyword arguments를 넣어주지 않아도, Task가 알아서 함수형으로 context 정보를 가져옴.    
    run_context_by_kwargs = get_context_by_kwargs()
    run_context_by_direct_args = get_context_by_direct_args()
    run_context_by_context_fn = get_context_by_context_fn()
    # get_context_by_kwargs() >> get_context_by_direct_args() >> get_context_by_context_fn()