from airflow.sdk import DAG, task
from airflow.decorators import task
from airflow.providers.http.operators.http import HttpOperator
from pendulum import datetime, duration
from datetime import timedelta
import pandas as pd
import json
import os

COIN_ID = "bitcoin"
OUTPUT_DIR = "/tmp/load" # scheduler(worker) container에서 os 상으로 해당 디렉토리 만들것. 

default_args = {
    "retries": 2, # 첫시도 + retries 2으로 최대 3회까지 시도. 
    "retry_delay": duration(seconds=30), # retries 시마다 delay 초. pendulum의 duration 적용도 가능
    "execution_timeout": duration(seconds=10), # 10 초동안 response가 오지 않으면 다음 retries로 넘어감
}

def create_coin_df(data):
    prices_df = pd.DataFrame(data["prices"], 
                             columns=["timestamp_ms", "price"])

    volume_df = pd.DataFrame(data["total_volumes"],
                             columns=["timestamp_ms", "volume"])

    market_cap_df = pd.DataFrame(data["market_caps"],
                                 columns=["timestamp_ms", "market_cap"])

    df = prices_df.merge(volume_df, on="timestamp_ms").merge(market_cap_df, on="timestamp_ms")
    
    # Convert timestamp
    df["datetime"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df = df.drop(columns=["timestamp_ms"])

    # Reorder columns
    df = df[["datetime", "price", "volume", "market_cap"]]

    return df

with DAG(
    dag_id="http_coin_op_01",
    start_date=datetime(2026, 1, 1, tz="UTC"), # coingecko가 UTC기반임. 현 날짜보다 2일 전으로 설정
    schedule="@daily", # 반드시 @daily 선택
    catchup=True, # catchup=True로 과거 일자 수행. 
    tags=["operator_hook", "http"],
    default_args=default_args
) as dag:

    # Fetch data from CoinGecko
    # https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range?vs_currency=usd&from=2026-01-25T00:00&to=2026-01-25T23:59
    run_fetch_bitcoin = HttpOperator(
        task_id="fetch_bitcoin_price",
        http_conn_id="coingecko_http",
        endpoint=f"api/v3/coins/{COIN_ID}/market_chart/range",
        method="GET",
        data={
            "vs_currency": "usd",
            "from": f"{{{{ ds }}}}T00:00", ## HTTP의 data 인자는 template 적용 가능한 인자
            "to": f"{{{{ ds }}}}T23:59",
        },
        log_response=True,
        #response_filter=lambda r: r.json() # 결과 xcom을 string이 아닌 json dict 형태로 입력
    )
    
    # HttpOperator의 출력 결과를 pandas dataframe으로 변환 한 뒤 csv 파일로 저장. 
    @task
    def build_coin_df(response_text: str, ds: str): # 만약 response_filter가 json dict라면 data: dict
        """
        response_text는 HttpOperator 수행 결과인 XCom에서 가져옴
        """
        data = json.loads(response_text) # 만약 response_filter가 json dict라면 해당 line은 필요 없음. 
        # 위에서 선언된 create_coin_df() 함수를 이용해서 dataframe 생성
        coin_df = create_coin_df(data)

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        file_path = f"{OUTPUT_DIR}/{COIN_ID}_{ds}.csv"
        coin_df.to_csv(file_path, index=False)

        return file_path
    
    # HTTPOperator는 수행 결과를 문자열로 xcom에 저장함.
    # HTTPOperator객체의 output는 결과 xcom으로서 taskflow api와 interface하기 위해 지원됨
    # build_coin_df의 ds 인자는 자동으로 airflow context로 입력됨. 
    coin_df = build_coin_df(run_fetch_bitcoin.output)
