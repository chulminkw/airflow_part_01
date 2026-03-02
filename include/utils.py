import pandas as pd

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

def print_scheduling_variables(
        timezone_value=None,
        run_id=None,
        logical_date=None,
        data_interval_start=None,
        data_interval_end=None
    ):
        print("========== DAG RUN ID & TIMEZONE ==========")
        print(f"run_id: {run_id}")
        print(f"DAG timezone:{timezone_value}")

        print("\n========== LOGICAL_DATE & DATA INTERVAL  ==========")
        print(f"logical_date: {logical_date}, type:{type(logical_date)}")
        print(f"logical_date in timezone: {logical_date.in_timezone(timezone_value)}")
        
        print(f"data_interval_start ~ data_interval_end:{data_interval_start} ~ {data_interval_end}")
        print(f"data_interval_start ~ data_interval_end in timezone: \
        {data_interval_start.in_timezone(timezone_value)} ~ {data_interval_end.in_timezone(timezone_value)}")
        