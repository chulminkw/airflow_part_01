from airflow.sdk import DAG, task
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.hooks.http import HttpHook
from pendulum import datetime, duration
from datetime import timedelta
import pandas as pd
import json
import os
# include/utils.py에 create_coin_df() 함수 import
from include.utils import create_coin_df

COIN_ID = "bitcoin"
OUTPUT_DIR = "/tmp/load"

default_args = {
    "retries": 2,
    "retry_delay": duration(seconds=30),
    "execution_timeout": duration(seconds=10),
}

with DAG(
    dag_id="http_coin_hook_01_refactor",
    start_date=datetime(2026, 1, 19, tz="UTC"),
    schedule="@daily",
    catchup=True,
    tags=["operator_hook", "http"],
) as dag:

    @task
    def fetch_bitcoin_price(**context): #**context 대신 ds도 무방
        date_str = context['ds']

        hook = HttpHook(
            method="GET",
            http_conn_id="coingecko_http",
        )

        endpoint = f"api/v3/coins/{COIN_ID}/market_chart/range"

        params = {
            "vs_currency": "usd",
            "from": f"{date_str}T00:00",
            "to": f"{date_str}T23:00",
        }

        response = hook.run(endpoint=endpoint, data=params)
        print(f"#### response type:{type(response)}") # requests.models.Response
        data = response.json()

        coin_df = create_coin_df(data)

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        file_path = f"{OUTPUT_DIR}/{COIN_ID}_{date_str}_hook_refactor.csv"
        coin_df.to_csv(file_path, index=False)

        return file_path

    fetch_bitcoin_price()
