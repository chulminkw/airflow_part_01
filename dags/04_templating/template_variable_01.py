from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime

LOCAL_TZ = "Asia/Seoul"

with DAG(
    dag_id="template_variable_01",
    start_date=datetime(2026, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    tags=["templating"],
) as dag:

    run_bash_01 = BashOperator(
        task_id="bash_template_01",
        # bash_command는 BashOperator에서 template_fields로 사용할 수 있음.
        # {{ ds }}와 같은 template은 문자열내에 위치해야 함.
        # {{ dag }} 가 반환하는 값은 문자열( dag가 Object 타입이더라도 {{ dag }}의 수행 결과는 문자열) 
        bash_command="""
        echo "===== Bash (Jinja) ====="
        echo "#### ds:{{ ds }}, ds_nodash:{{ ds_nodash }}"
        echo "#### ts:{{ ts }}, ts_nodash:{{ ts_nodash }}
        echo "#### logical_date UTC:{{ logical_date }}, Asia/Seoul:{{ logical_date.in_timezone('Asia/Seoul') }}"
        echo "#### data_interval_start UTC:{{ data_interval_start }}, Asia/Seoul:{{ data_interval_start.in_timezone('Asia/Seoul')}}"
        echo "#### data_interval_end UTC:{{ data_interval_end }}, , Asia/Seoul:{{ data_interval_end.in_timezone('Asia/Seoul')}}""
        echo "#### date string in Asia/Seoul timezone {{ logical_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }} "
        echo "#### dag: {{ dag }}, dag's timezone: {{ dag.timezone }}"
        """
    )

    run_bash_02 = BashOperator(
        task_id="bash_template_02",
        # bash_command는 BashOperator에서 template_fields로 사용할 수 있음.
        # f-string 사용시 유의 필요.  {{ }}가 아닌 {{{{ }}}}로 template variable 표시
        # f-string에서 해석하는 { } 지정된 변수값은 문자열 자체로 template variable에 적용됨
        bash_command=f"""
        echo "===== Bash (Jinja) with format string ====="
        echo "#### LOCAL_TZ:{LOCAL_TZ}"
        echo "#### incorrect logical_date templating with f-string: {{ logical_date }}"
        echo "#### correct logical_date templating with f-string:{{{{ logical_date }}}}"
        echo "#### logical_date in {LOCAL_TZ}: {{{{ logical_date.in_timezone('{LOCAL_TZ}') }}}}"
        """
    )
    
    # taskflow api는 함수 내부에서 {{ }}를 사용할 수 없음. 
    # template 변수는 Operator의 template_fields와 taskflow api의 함수 인자에서 적용됨.
    @task
    def print_context(**context): 
        print("===== Context Variables (TaskFlow) =====")
        print("logical_date:", context["logical_date"], "type:", type(context["logical_date"]))
        print("ds:", context["ds"])
        print("data_interval_start:", context["data_interval_start"])
        print("data_interval_end:", context["data_interval_end"])
        print("run_id:", context["run_id"])
        print("dag_id:", context["dag"].dag_id)
        print("task_id:", context["task"].task_id)

    run_print_context_01 = print_context()

    # taskflow api의 함수 인자로 template variable이 사용될 수 있으며, 인자들은 문자열로 입력되어야 함.
    # [주의] 인자명을 ds, logical_date와 같이 template variable 명으로 부여하지 말것. parsing 시 내부 오류 발생할 수 있음. 
    @task
    def print_template_variables(ds_arg: str, logical_date_arg:str, 
                                    interval_start_arg:str, interval_end_arg:str ):
        print("===== TaskFlow (Rendered Inputs) =====")
        print("ds:", ds_arg)
        print("logical_date:", logical_date_arg, "type:", type(logical_date_arg) )
        print("data_interval_start:", interval_start_arg)
        print("data_interval_end:", interval_end_arg)
    
    # taskflow 인자로 template 변수가 입력될 수 있음. 반드시 문자열로 처리되어야 함. 
    run_print_template_01 = print_template_variables(
        ds_arg="{{ ds }}",
        logical_date_arg="{{ logical_date }}",
        interval_start_arg="{{ data_interval_start }}",
        interval_end_arg="{{ data_interval_end }}",
    )

    [run_bash_01, run_bash_02, run_print_context_01, run_print_template_01]
