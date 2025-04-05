import pandas as pd
from clickhouse_connect import get_client
from config import CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DATABASE, CLICKHOUSE_TABLE

# Connect to ClickHouse
def get_clickhouse_client():
    return get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, username='default', password='')

# Fetch latest stock data
def fetch_latest_stock_data(ticker, limit=60):
    client = get_clickhouse_client()
    query = f"""
        SELECT Formatted_Timestamp, Open_Price, Day_High, Day_Low, Current_Price, Volume
        FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
        WHERE Name = '{ticker}'
        ORDER BY Formatted_Timestamp DESC
        LIMIT {limit}
    """
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=["Timestamp", "Open", "High", "Low", "Close", "Volume"])
    return df[::-1]

def fetch_candlestick_data(ticker):
    client = get_clickhouse_client()
    
    # Fetch data with Industry, Sector, and Volume included
    query = f"""
        SELECT Name, Industry, Sector, Formatted_Timestamp, Current_Price, Volume
    FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
    WHERE Name = '{ticker}'
    ORDER BY Formatted_Timestamp DESC
    LIMIT 60    
    """
    
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=["Name", "Industry", "Sector", "Timestamp", "Current_Price", "Volume"])
    
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

def fetch_live_price_data(ticker):
    df = fetch_latest_stock_data(ticker,50)
    return df[["Timestamp", "Close"]].rename(columns={"Timestamp": "Formatted_Timestamp", "Close": "Current_Price"})

    
candlestickk_df = fetch_candlestick_data("ZOMATO.NS")
live_price_df = fetch_live_price_data("ZOMATO.NS")