from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime

with DAG(
    dag_id="template_variable_simple",
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["template"],
) as dag:

    # 1️⃣ BashOperator (classic Jinja templating)
    run_bash_01 = BashOperator(
        task_id="bash_template_01",
        # bash_command는 BashOperator에서 template_fields로 사용할 수 있음.
        # {{ ds }}와 같은 template은 문자열내에 위치해야 함.
        # f-string 사용시 유의 필요.  
        bash_command="""
        echo "===== Bash (Jinja) ====="
        echo "#### ds={{ ds }}"
        echo "#### logical_date={{ logical_date }}"
        echo "#### data_interval_start={{ data_interval_start }}"
        echo "#### data_interval_end={{ data_interval_end }}"
        """
    )

    run_bash_02 = BashOperator(
        task_id="bash_template_02",
        # bash_command는 BashOperator에서 template_fields로 사용할 수 있음.
        # {{ ds }}와 같은 template은 문자열내에 위치해야 함.
        # f-string 사용시 유의 필요.  {{ }}가 아닌 {{{{ }}}}로 template variable 표시
        bash_command=f"""
        echo "===== Bash (Jinja) with format string ====="
        echo "#### logical_date={{{{ logical_date }}}}"
        echo "#### data_interval_start={{{{ data_interval_start }}}}"
        echo "#### data_interval_end={{{{ data_interval_end }}}}"
        """
    )

    @task
    def print_context():
        '''
        taskflow api 함수라도 함수 내부에서는 {{ }}를 사용할 수 없음. 
        template 변수는 Operator의 template_fields와 taskflow api의 함수 인자에서 적용됨.
        '''
        from airflow.sdk import get_current_context
        context = get_current_context()

        print("===== Airflow Template Variables (TaskFlow) =====")
        print("logical_date:", context["logical_date"], "type:", type(context["logical_date"]))
        print("ds:", context["ds"])
        print("data_interval_start:", context["data_interval_start"])
        print("data_interval_end:", context["data_interval_end"])
        print("run_id:", context["run_id"])
        print("dag_id:", context["dag"].dag_id)
        print("task_id:", context["task"].task_id)

    run_print_context_01 = print_context()

    # taskflow api의 함수 인자로 template variable이 사용될 수 있음
    # [주의] 인자명을 ds, logical_date와 같이 template variable 명으로 부여하지 말것. parsing 시 내부 오류 발생할 수 있음. 
    @task
    def print_template_variables(ds_arg, logical_date_arg, 
                                    interval_start_arg, interval_end_arg):
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
