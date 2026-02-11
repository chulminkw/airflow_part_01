from airflow.sdk import DAG, task
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.hooks.http import HttpHook
from pendulum import datetime, duration
from datetime import timedelta
import pandas as pd
import json
import os

COIN_ID = "bitcoin"
OUTPUT_DIR = "/tmp/load"

default_args = {
    "retries": 2,
    "retry_delay": duration(seconds=30),
    "execution_timeout": duration(seconds=10),
}

# fetch_bitcoin_price 에서 dataframe 생성 시 호출됨. 
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
    dag_id="http_coin_hook_01",
    start_date=datetime(2026, 2, 1, tz="UTC"),
    schedule="@daily",
    catchup=True,
    tags=["operator_hook", "http"],
) as dag:
    @task
    def fetch_bitcoin_price(**context):#**context 대신 ds도 무방
        date_str = context["ds"]
        hook = HttpHook(
            method="GET",
            http_conn_id="coingecko_http"
        )        

        endpoint = f"api/v3/coins/{COIN_ID}/market_chart/range"
        params = {
            "vs_currency": "usd",
            "from": f"{date_str}T00:00",
            "to": f"{date_str}T23:59"
        }
        
        response = hook.run(endpoint=endpoint, data=params)
        print(f"#### response type:{type(response)}") # requests.models.Response
        data = response.json()

        coin_df = create_coin_df(data)

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        file_path = f"{OUTPUT_DIR}/{COIN_ID}_{date_str}_hook.csv"
        coin_df.to_csv(file_path, index=False)

        return file_path

    fetch_bitcoin_price()
