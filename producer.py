import yfinance as yf
import json
import sys
import time
import datetime
from time import sleep
from confluent_kafka import Producer
from concurrent.futures import ThreadPoolExecutor

# Handling compatibility issue with Kafka in Python 3.12+
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

# Configure Confluent Kafka producer
producer = Producer({
    'bootstrap.servers': 'localhost:9092',  # Kafka server address
})

# List of tickers to fetch data for
tickers = ["ZOMATO.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
           "TCS.NS", "LT.NS", "SBIN.NS", "ADANIGREEN.NS"]

# Delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Function to fetch stock data
def fetch_stock_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        stock_data = {
            "Name": ticker,
            "Current_Price": stock.fast_info.last_price,
            "Open_Price": stock.info.get('open', None),
            "Previous_Close": stock.info.get('previousClose', None),
            "Day_High": stock.info.get('dayHigh', None),
            "Day_Low": stock.info.get('dayLow', None),
            "Bid_Price": stock.info.get('bid', None),
            "Ask_Price": stock.info.get('ask', None),
            "Volume": stock.info.get('volume', None),
            "Company_Name": stock.info.get('longName', None),
            "Sector": stock.info.get('sector', None),
            "Industry": stock.info.get('industry', None),
            "Country": stock.info.get('country', None),
        }

        # Fetch timestamp
        timestamp_ms = round(time.time() * 1000)
        formatted_time = datetime.datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
        stock_data["Timestamp"] = timestamp_ms
        stock_data["Formatted_Timestamp"] = formatted_time

        # Fetch 1-month old historical data only on the 1st of every month
        now = datetime.datetime.now()
        if now.day == 1:
            try:
                history = stock.history(period="1mo", interval="1d")
                one_month_old_data = history.iloc[0] if len(history) > 0 else None
                stock_data["1_Month_Old_Price"] = one_month_old_data["Close"] if one_month_old_data is not None else None
                stock_data["1_Month_Old_Volume"] = one_month_old_data["Volume"] if one_month_old_data is not None else None
            except Exception as e:
                print(f"Error fetching historical data for {ticker}: {e}")
                stock_data["1_Month_Old_Price"] = None
                stock_data["1_Month_Old_Volume"] = None
        else:
            stock_data["1_Month_Old_Price"] = None
            stock_data["1_Month_Old_Volume"] = None

        return stock_data
    except Exception as e:
        print(f"Error fetching stock data for {ticker}: {e}")
        return None

# Function to run the fetching loop with error handling
def run_fetching_loop():
    counter = 0
    interval = 10  # Fetch interval in seconds
    while counter <= 9000:
        try:
            current_time = time.time()
            next_fetch_time = ((current_time // interval) + 1) * interval
            sleep_time = max(0, next_fetch_time - current_time)
            time.sleep(sleep_time)

            with ThreadPoolExecutor() as executor:
                results = list(executor.map(fetch_stock_data, tickers))
                results = [res for res in results if res is not None]

            for stock_data in results:
                print(stock_data)
                producer.produce(stock_data["Name"], key=stock_data["Name"], value=json.dumps(stock_data), callback=delivery_report)

            producer.flush()
            print(f"Iteration {counter} completed at {results[0]['Formatted_Timestamp']}")
            counter += 1
        except Exception as e:
            print(f"Error occurred in main loop: {e}. Restarting in 10 seconds...")
            time.sleep(10)
            continue  # Restart the loop if any error occurs

if __name__ == "__main__":
    while True:
        try:
            run_fetching_loop()
        except Exception as e:
            print(f"Critical error: {e}. Restarting the script in 10 seconds...")
            time.sleep(10)






# import yfinance as yf
# import json
# import sys
# import time
# import datetime
# from time import sleep
# from confluent_kafka import Producer
# from concurrent.futures import ThreadPoolExecutor

# # Handling compatibility issue with Kafka in Python 3.12+
# if sys.version_info >= (3, 12, 0):
#     import six
#     sys.modules['kafka.vendor.six.moves'] = six.moves

# # Configure Confluent Kafka producer
# producer = Producer({
#     'bootstrap.servers': 'localhost:9092',  # Kafka server address
# })

# # List of tickers to fetch data for
# tickers = ["ZOMATO.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
#            "TCS.NS", "LT.NS", "SBIN.NS", "ADANIGREEN.NS"]

# # Delivery report callback to handle success or failure of message delivery
# def delivery_report(err, msg):
#     if err is not None:
#         print(f"Message delivery failed: {err}")
#     else:
#         print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# def fetch_stock_data(ticker):
#     """Fetch real-time market data for a given ticker."""
#     stock = yf.Ticker(ticker)

#     stock_data = {
#         "Name": ticker,
#         "Current_Price": stock.fast_info.last_price,
#         "Open_Price": stock.info.get('open', None),
#         "Previous_Close": stock.info.get('previousClose', None),
#         "Day_High": stock.info.get('dayHigh', None),
#         "Day_Low": stock.info.get('dayLow', None),
#         "Bid_Price": stock.info.get('bid', None),
#         "Ask_Price": stock.info.get('ask', None),
#         "Volume": stock.info.get('volume', None),
#         "Company_Name": stock.info.get('longName', None),
#         "Sector": stock.info.get('sector', None),
#         "Industry": stock.info.get('industry', None),
#         "Country": stock.info.get('country', None),
#     }

#     # Fetch timestamp
#     timestamp_ms = round(time.time() * 1000)  # Current timestamp in milliseconds
#     formatted_time = datetime.datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

#     stock_data["Timestamp"] = timestamp_ms
#     stock_data["Formatted_Timestamp"] = formatted_time

#     # Fetch 1-month old historical data only on the 1st of every month
#     now = datetime.datetime.now()
#     if now.day == 3:
#         try:
#             history = stock.history(period="1mo", interval="1d")
#             one_month_old_data = history.iloc[0] if len(history) > 0 else None
#             stock_data["1_Month_Old_Price"] = one_month_old_data["Close"] if one_month_old_data is not None else None
#             stock_data["1_Month_Old_Volume"] = one_month_old_data["Volume"] if one_month_old_data is not None else None
#         except yf.exceptions.YFRateLimitError:
#             print(f"Rate limited for {ticker}, retrying in 10 seconds...")
#             time.sleep(10)
#         except Exception as e:
#             print(f"Error fetching historical data for {ticker}: {e}")
#             stock_data["1_Month_Old_Price"] = None
#             stock_data["1_Month_Old_Volume"] = None
#     else:
#         stock_data["1_Month_Old_Price"] = None
#         stock_data["1_Month_Old_Volume"] = None

#     return stock_data

# counter = 0
# interval = 10  # Fetch interval in seconds

# while counter <= 9000:
#     # Synchronize fetch with real-world clock to maintain fixed intervals
#     current_time = time.time()
#     next_fetch_time = ((current_time // interval) + 1) * interval
#     sleep_time = max(0, next_fetch_time - current_time)
#     time.sleep(sleep_time)

#     # Fetch stock data in parallel
#     with ThreadPoolExecutor() as executor:
#         results = list(executor.map(fetch_stock_data, tickers))

#     # Send data to Kafka
#     for stock_data in results:
#         print(stock_data)  # Print data for debugging
#         producer.produce(stock_data["Name"], key=stock_data["Name"], value=json.dumps(stock_data), callback=delivery_report)

#     # Wait for all messages to be delivered
#     producer.flush()
#     print(f"Iteration {counter} completed at {results[0]['Formatted_Timestamp']}")

#     counter += 1


# # ONLY HISTORICAL DATA
# import yfinance as yf
# import json
# import sys
# import time
# import datetime
# from confluent_kafka import Producer

# # Handling compatibility issue with Kafka in Python 3.12+
# if sys.version_info >= (3, 12, 0):
#     import six
#     sys.modules['kafka.vendor.six.moves'] = six.moves

# # Configure Confluent Kafka producer
# producer = Producer({
#     'bootstrap.servers': 'localhost:9092',  # Kafka server address
# })

# # List of tickers to fetch data for
# tickers = ["ZOMATO.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
#            "TCS.NS", "LT.NS", "SBIN.NS", "ADANIGREEN.NS"]

# # Delivery report callback
# def delivery_report(err, msg):
#     if err is not None:
#         print(f"Message delivery failed: {err}")
#     else:
#         print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# def fetch_historical_data(ticker):
#     """Fetch 1-month-old historical market data for a given ticker."""
#     stock = yf.Ticker(ticker)

#     stock_data = {
#         "Name": ticker,
#     }

#     # Fetch historical data only on the 1st of the month
#     now = datetime.datetime.now()
#     if now.day == 3:
#         try:
#             history = stock.history(period="1mo", interval="1d")
#             one_month_old_data = history.iloc[0] if len(history) > 0 else None
            
#             # Get current timestamp in milliseconds
#             timestamp_ms = round(time.time() * 1000)
#             formatted_time = datetime.datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

#             stock_data["1_Month_Old_Price"] = one_month_old_data["Close"] if one_month_old_data is not None else None
#             stock_data["1_Month_Old_Volume"] = one_month_old_data["Volume"] if one_month_old_data is not None else None
#             stock_data["Timestamp"] = timestamp_ms
#             stock_data["Formatted_Timestamp"] = formatted_time

#         except yf.exceptions.YFRateLimitError:
#             print(f"Rate limited for {ticker}, retrying in 10 seconds...")
#             time.sleep(10)
#         except Exception as e:
#             print(f"Error fetching historical data for {ticker}: {e}")
#             stock_data["1_Month_Old_Price"] = None
#             stock_data["1_Month_Old_Volume"] = None
#             stock_data["Timestamp"] = None
#             stock_data["Formatted_Timestamp"] = None
#     else:
#         stock_data["1_Month_Old_Price"] = None
#         stock_data["1_Month_Old_Volume"] = None
#         stock_data["Timestamp"] = None
#         stock_data["Formatted_Timestamp"] = None

#     return stock_data

# # Fetch historical data and send to Kafka
# def fetch_and_send_historical_data():
#     for ticker in tickers:
#         stock_data = fetch_historical_data(ticker)
#         print(stock_data)
#         producer.produce(ticker, key=ticker, value=json.dumps(stock_data), callback=delivery_report)

#     # Wait for all messages to be delivered
#     producer.flush()

# # Run the data fetching and Kafka message sending
# fetch_and_send_historical_data()





# import yfinance as yf
# import json
# from time import sleep
# import sys
# import time
# import datetime
# from confluent_kafka import Producer
# import calendar

# # Handling compatibility issue with kafka in Python 3.12+
# if sys.version_info >= (3, 12, 0):
#     import six
#     sys.modules['kafka.vendor.six.moves'] = six.moves

# # Configure Confluent Kafka producer
# producer = Producer({
#     'bootstrap.servers': 'localhost:9092',  # Kafka server address
# })

# # List of tickers to fetch data for
# tickers = ["ZOMATO.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
#            "TCS.NS", "LT.NS", "SBIN.NS", "ADANIGREEN.NS"]

# # Delivery report callback to handle success or failure of message delivery
# def delivery_report(err, msg):
#     if err is not None:
#         print(f"Message delivery failed: {err}")
#     else:
#         print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# counter = 0
# while counter <= 9000:
#     for ticker in tickers:
#         stock = yf.Ticker(ticker)
        
#         # Fetch real-time market data
#         stock_data = {}
#         stock_data["Name"] = ticker
#         stock_data["Current_Price"] = stock.fast_info.last_price
#         stock_data["Open_Price"] = stock.info.get('open', None)
#         stock_data["Previous_Close"] = stock.info.get('previousClose', None)
#         stock_data["Day_High"] = stock.info.get('dayHigh', None)
#         stock_data["Day_Low"] = stock.info.get('dayLow', None)
#         stock_data["Bid_Price"] = stock.info.get('bid', None)
#         stock_data["Ask_Price"] = stock.info.get('ask', None)
#         stock_data["Volume"] = stock.info.get('volume', None)

#         # Fetch basic company info
#         stock_data["Company_Name"] = stock.info.get('longName', None)
#         stock_data["Sector"] = stock.info.get('sector', None)
#         stock_data["Industry"] = stock.info.get('industry', None)
#         stock_data["Country"] = stock.info.get('country', None)

#         # Fetch timestamp
#         timestamp_ms = round(time.time() * 1000)  # Current timestamp in milliseconds
#         formatted_time = datetime.datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

#         stock_data["Timestamp"] = timestamp_ms
#         stock_data["Formatted_Timestamp"] = formatted_time

#         # Fetch 1-month old historical data only on the 1st of every month
#         now = datetime.datetime.now()
#         if now.day == 1:
#             try:
#                 history = stock.history(period="1mo", interval="1d")
#                 one_month_old_data = history.iloc[0] if len(history) > 0 else None
#                 if one_month_old_data is not None:
#                     stock_data["1_Month_Old_Price"] = one_month_old_data["Close"]
#                     stock_data["1_Month_Old_Volume"] = one_month_old_data["Volume"]
#                 else:
#                     stock_data["1_Month_Old_Price"] = None
#                     stock_data["1_Month_Old_Volume"] = None
#             except yf.exceptions.YFRateLimitError:
#                 print(f"Rate limited for {ticker}, retrying in 10 seconds...")
#                 time.sleep(10)
#             except Exception as e:
#                 print(f"Error fetching historical data for {ticker}: {e}")
#                 stock_data["1_Month_Old_Price"] = None
#                 stock_data["1_Month_Old_Volume"] = None
#         else:
#             stock_data["1_Month_Old_Price"] = None
#             stock_data["1_Month_Old_Volume"] = None

#         # Print the data to the console (optional)
#         print(stock_data)

#         # Send the data to Kafka topic
#         producer.produce(ticker, key=ticker, value=json.dumps(stock_data), callback=delivery_report)
    
#     # Wait for all messages to be delivered
#     producer.flush()
#     print(counter)
    
#     sleep(6)  # Sleep for 6 seconds before fetching data again
#     counter += 1

# # Close the producer after finishing the task
# producer.flush()  # Ensure any remaining messages are sent




# import yfinance as yf
# import json
# from time import sleep
# import sys
# import time
# import datetime
# from confluent_kafka import Producer

# # Handling compatibility issue with kafka in Python 3.12+
# if sys.version_info >= (3, 12, 0):
#     import six
#     sys.modules['kafka.vendor.six.moves'] = six.moves

# # Configure Confluent Kafka producer
# producer = Producer({
#     'bootstrap.servers': 'localhost:9092',  # Kafka server address
# })

# # List of tickers to fetch data for
# tickers = ["ZOMATO.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
#            "TCS.NS", "LT.NS", "SBIN.NS", "ADANIGREEN.NS"]

# # Delivery report callback to handle success or failure of message delivery
# def delivery_report(err, msg):
#     if err is not None:
#         print(f"Message delivery failed: {err}")
#     else:
#         print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# counter = 0
# while counter <= 9000:
#     for ticker in tickers:
#         stock = yf.Ticker(ticker)
        
#         # Fetch real-time market data
#         stock_data = {}
#         stock_data["Name"] = ticker
#         stock_data["Current_Price"] = stock.fast_info.last_price
#         stock_data["Open_Price"] = stock.info.get('open', None)
#         stock_data["Previous_Close"] = stock.info.get('previousClose', None)
#         stock_data["Day_High"] = stock.info.get('dayHigh', None)
#         stock_data["Day_Low"] = stock.info.get('dayLow', None)
#         stock_data["Bid_Price"] = stock.info.get('bid', None)
#         stock_data["Ask_Price"] = stock.info.get('ask', None)
#         stock_data["Volume"] = stock.info.get('volume', None)

#         # Fetch basic company info
#         stock_data["Company_Name"] = stock.info.get('longName', None)
#         stock_data["Sector"] = stock.info.get('sector', None)
#         stock_data["Industry"] = stock.info.get('industry', None)
#         stock_data["Country"] = stock.info.get('country', None)

#         # Fetch timestamp
#         timestamp_ms = round(time.time() * 1000)  # Current timestamp in milliseconds
#         formatted_time = datetime.datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

#         stock_data["Timestamp"] = timestamp_ms
#         stock_data["Formatted_Timestamp"] = formatted_time

#         # Fetch 1-month old historical data
#         try:
#             history = stock.history(period="1mo", interval="1d")
#             one_month_old_data = history.iloc[0] if len(history) > 0 else None
#             if one_month_old_data is not None:
#                 stock_data["1_Month_Old_Price"] = one_month_old_data["Close"]
#                 stock_data["1_Month_Old_Volume"] = one_month_old_data["Volume"]
#             else:
#                 stock_data["1_Month_Old_Price"] = None
#                 stock_data["1_Month_Old_Volume"] = None
#         except yf.exceptions.YFRateLimitError:
#             print(f"Rate limited for {ticker}, retrying in 10 seconds...")
#             time.sleep(10)
#         except Exception as e:
#             print(f"Error fetching historical data for {ticker}: {e}")
#             stock_data["1_Month_Old_Price"] = None
#             stock_data["1_Month_Old_Volume"] = None

#         # Print the data to the console (optional)
#         print(stock_data)

#         # Send the data to Kafka topic
#         producer.produce(ticker, key=ticker, value=json.dumps(stock_data), callback=delivery_report)
    
#     # Wait for all messages to be delivered
#     producer.flush()
#     print(counter)
    
#     sleep(6)  # Sleep for 6 seconds before fetching data again
#     counter += 1

# # Close the producer after finishing the task
# producer.flush()  # Ensure any remaining messages are sent









# import yfinance as yf
# import json
# from time import sleep
# import sys
# import time
# from confluent_kafka import Producer
# from datetime import datetime

# # Handling compatibility issue with Kafka in Python 3.12+
# if sys.version_info >= (3, 12, 0):
#     import six
#     sys.modules['kafka.vendor.six.moves'] = six.moves

# # Configure Confluent Kafka producer
# producer = Producer({
#     'bootstrap.servers': 'localhost:9092',  # Kafka server address
# })

# # List of tickers
# tickers = ["ZOMATO.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
#            "TCS.NS", "LT.NS", "SBIN.NS", "ADANIGREEN.NS"]

# # Delivery report callback
# def delivery_report(err, msg):
#     if err is not None:
#         print(f"Message delivery failed: {err}")
#     else:
#         print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# counter = 0
# while counter <= 50:
#     for ticker in tickers:
#         stock = yf.Ticker(ticker)

#         # Fetch the latest price data
#         stock_data = {}
#         stock_data["Price"] = stock.fast_info.last_price
#         stock_data["Name"] = ticker

#         # Get current timestamp in seconds
#         timestamp_millis = round(time.time() * 1000)
#         timestamp_seconds = timestamp_millis // 1000  # Convert milliseconds to seconds
#         formatted_time = datetime.utcfromtimestamp(timestamp_seconds).strftime('%Y-%m-%d %H:%M:%S')

#         stock_data["Timestamp"] = timestamp_seconds  # Send timestamp in seconds for ClickHouse
#         stock_data["Formatted_Timestamp"] = formatted_time  # Optional, for debugging

#         print(stock_data)  # Debugging output
        
#         # Send data to Kafka
#         producer.produce(ticker, key=ticker, value=json.dumps(stock_data), callback=delivery_report)

#     producer.flush()
#     sleep(6)  # Fetch new data every 6 seconds
#     counter += 1

# # Close producer
# producer.flush()























# import yfinance as yf
# import json
# from time import sleep
# import sys
# import time
# from confluent_kafka import Producer

# # Handling compatibility issue with kafka in Python 3.12+
# if sys.version_info >= (3, 12, 0):
#     import six
#     sys.modules['kafka.vendor.six.moves'] = six.moves

# # Configure Confluent Kafka producer
# producer = Producer({
#     'bootstrap.servers': 'localhost:9092',  # Kafka server address
# })

# # List of tickers you want to fetch data for
# tickers = ["ZOMATO.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
#            "TCS.NS", "LT.NS", "SBIN.NS", "ADANIGREEN.NS"]

# # Delivery report callback to handle success or failure of message delivery
# def delivery_report(err, msg):
#     if err is not None:
#         print(f"Message delivery failed: {err}")
#     else:
#         print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# counter = 0
# while counter <= 50:
#     for ticker in tickers:
#         stock = yf.Ticker(ticker)
        
#         # Fetch the latest price data
#         stock_data = {}
#         stock_data["Price"] = stock.fast_info.last_price  # Fetch the last price
#         stock_data["Name"] = ticker
#         stock_data["Timestamp"] = round(time.time() * 1000)  # Current timestamp in milliseconds
        
#         # Print the data to the console (optional)
#         print(stock_data)
        
#         # Send the data to Kafka topic for each ticker
#         producer.produce(ticker, key=ticker, value=json.dumps(stock_data), callback=delivery_report)
    
#     # Wait for all messages to be delivered
#     producer.flush()
    
#     sleep(6)  # Sleep for 12 seconds before fetching data again
#     counter += 1

# # Close the producer after finishing the task
# producer.flush()  # Ensure any remaining messages are sent
