import pandas as pd
from clickhouse_connect import get_client
import sys
import datetime
from config import CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DATABASE, CLICKHOUSE_TABLE

# Connect to ClickHouse
def get_clickhouse_client():
    return get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, username='default', password='')

# Fetch latest stock data
def fetch_latest_stock_data(ticker, limit=300):
    client = get_clickhouse_client()
    query = f"""
    SELECT 
        toString(Formatted_Timestamp) AS Formatted_Timestamp, 
        Open_Price, 
        Day_High, 
        Day_Low, 
        Current_Price, 
        Volume
    FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
    WHERE Name = '{ticker}'
    ORDER BY Formatted_Timestamp ASC
    LIMIT {limit}
    """
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=["Timestamp", "Open", "High", "Low", "Close", "Volume"])
    print('NDATA',result.result_rows)
    return df  # Reverse for chronological order

# Aggregate data for candlestick chart
def fetch_candlestick_data(ticker):
    client = get_clickhouse_client()
    
    # Fetch data with Industry, Sector, and Volume included
    query = f"""
        SELECT Name, Industry, Sector, Formatted_Timestamp, Current_Price, Volume
    FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
    WHERE Name = '{ticker}'
    ORDER BY Formatted_Timestamp ASC
    LIMIT 300   
    """
    
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=["Name", "Industry", "Sector", "Timestamp", "Current_Price", "Volume"])
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    # Create 5-minute bins
    df["Time Bin"] = df["Timestamp"].dt.ceil("5min")  # Ensures alignment

    
    # Aggregate to candlestick format (Include Volume)
    candlestick_df = df.groupby(["Name", "Industry", "Sector", "Time Bin"]).agg(
        Open=("Current_Price", "first"),
        Close=("Current_Price", "last"),
        High=("Current_Price", "max"),
        Low=("Current_Price", "min"),
        Volume=("Volume", "sum")  # ✅ FIX: Include Volume
    ).reset_index()

    return candlestick_df

# Get volume chart data
def fetch_volume_data(ticker):
    df = fetch_candlestick_data(ticker)
    return df[["Time Bin", "Volume", "Open", "Close"]]

# Get live stock price data
def fetch_live_price_data(ticker):
    df = fetch_latest_stock_data(ticker,300)
    return df[["Timestamp", "Close"]].rename(columns={"Timestamp": "Formatted_Timestamp", "Close": "Current_Price"})
