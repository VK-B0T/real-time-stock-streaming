# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_GROUP_ID = 'stock_data'
KAFKA_TOPICS = [
    "ZOMATO.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
    "TCS.NS", "LT.NS", "SBIN.NS", "ADANIGREEN.NS"
]

# ClickHouse Configuration
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DATABASE = "stock_data"
CLICKHOUSE_TABLE = "live_stock_data"

# List of Stock Tickers
STOCK_TICKERS = KAFKA_TOPICS

# Power BI Dashboard Embed URL
POWER_BI_EMBED_URL = "https://app.powerbi.com/reportEmbed?reportId=9ed71201-41ed-4c25-a9dd-df8b4a847077&autoAuth=true&ctid=ef894749-8256-4ae1-bb93-741f00a91fda"

# Streamlit refresh intervals
CANDLESTICK_UPDATE_INTERVAL = 300  # 5 minutes in seconds
LIVE_PRICE_UPDATE_INTERVAL = 10    # 10 seconds
