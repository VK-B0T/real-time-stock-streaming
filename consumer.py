from confluent_kafka import Consumer, KafkaException, KafkaError
import json
from clickhouse_connect import get_client
from datetime import datetime

# Kafka configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker
    'group.id': 'stock_data',              # Consumer group ID
    'auto.offset.reset': 'earliest',      # Start consuming from the beginning if no offset is saved
}

# Configure ClickHouse client
client = get_client(host='localhost', port=8123, username='default', password='')

# Topics to subscribe
tickers = ["ZOMATO.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
           "TCS.NS", "LT.NS", "SBIN.NS", "ADANIGREEN.NS"]

# Create the Kafka consumer
consumer = Consumer(consumer_config)

# Subscribe to the topics
consumer.subscribe(tickers)

try:
    print("Consumer is starting. Waiting for messages...")

    while True:
        # Poll for messages
        msg = consumer.poll(1.0)  # Wait for 1 second for a message

        if msg is None:
            continue  # No message received

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue  # Reached the end of a partition
            else:
                raise KafkaException(msg.error())

        # Decode the message
        data = json.loads(msg.value().decode('utf-8'))
        print(f"Received message: {data}")

        # Convert `Formatted_Timestamp` from string to datetime object
        Formatted_Timestamp = datetime.strptime(data["Formatted_Timestamp"], "%Y-%m-%d %H:%M:%S")

        # Structure the data for ClickHouse insertion
        data_tuple = (
            data['Name'],
            data['Current_Price'],
            data['Open_Price'],
            data['Previous_Close'],
            data['Day_High'],
            data['Day_Low'],
            data['Bid_Price'],
            data['Ask_Price'],
            data['Volume'],
            data['Company_Name'],
            data['Sector'],
            data['Industry'],
            data['Country'],
            data['Timestamp'],  # Original timestamp in milliseconds
            Formatted_Timestamp,  # Proper datetime object
        )

        try:
            # Insert stock data into ClickHouse
            client.insert('live_stock_data', [data_tuple])
            print(f"Data inserted into ClickHouse: {data['Name']}")
            client.insert('stock_data.live_stock_data', [data_tuple])
            print(f"Data inserted into ClickHouse: {data}")
        except Exception as e:
            print(f"Failed to insert data into ClickHouse: {e}")

        # # Insert historical data only if it exists
        # if data.get("1_Month_Old_Price") is not None:  # Using get to avoid KeyError
        #     historical_data = [(
        #         data["Name"], data["1_Month_Old_Price"], data["1_Month_Old_Volume"],
        #         data['Timestamp'], Formatted_Timestamp
        #     )]
        #     try:
        #         client.insert('historical_stock_data', historical_data)
        #         print(f"Inserted historical data for {data['Name']} into ClickHouse")
        #         client.insert('stock_data.historical_stock_data', historical_data)
        #         print(f"Inserted historical data for {data['Name']} into ClickHouse")
        #     except Exception as e:
        #         print(f"❌ Error inserting data into ClickHouse: {e}")

except KeyboardInterrupt:
    print("⚠️ Consumer stopped manually.")

finally:
    # Close the consumer to commit final offsets
    consumer.close()



# #ONLY HISTORIC DATA
# from confluent_kafka import Consumer, KafkaException, KafkaError
# import json
# from clickhouse_connect import get_client
# from datetime import datetime

# # Kafka configuration
# consumer_config = {
#     'bootstrap.servers': 'localhost:9092',  # Kafka broker
#     'group.id': 'stock_data',              # Consumer group ID
#     'auto.offset.reset': 'latest',       # Start consuming from the beginning if no offset is saved
# }

# # Configure ClickHouse client
# client = get_client(host='localhost', port=8123, username='default', password='')

# # Topics to subscribe
# tickers = ["ZOMATO.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
#            "TCS.NS", "LT.NS", "SBIN.NS", "ADANIGREEN.NS"]

# # Create the Kafka consumer
# consumer = Consumer(consumer_config)

# # Subscribe to the topics
# consumer.subscribe(tickers)

# try:
#     print("Consumer is starting. Waiting for historical data messages...")

#     while True:
#         # Poll for messages
#         msg = consumer.poll(1.0)  # Wait for 1 second for a message

#         if msg is None:
#             continue  # No message received

#         if msg.error():
#             if msg.error().code() == KafkaError._PARTITION_EOF:
#                 continue  # Reached the end of a partition
#             else:
#                 raise KafkaException(msg.error())

#         # Decode the message
#         data = json.loads(msg.value().decode('utf-8'))
#         print(f"Received message: {data}")

#         # Convert `Formatted_Timestamp` from string to datetime object
#         formatted_timestamp = datetime.strptime(data["Formatted_Timestamp"], "%Y-%m-%d %H:%M:%S")

#         # Insert historical data only if it exists
#         if data.get("1_Month_Old_Price") is not None:  # Using get to avoid KeyError
#             historical_data = [(
#                 data["Name"], data["1_Month_Old_Price"], data["1_Month_Old_Volume"],
#                 data['Timestamp'], formatted_timestamp
#             )]
#             try:
#                 client.insert('historical_stock_data', historical_data)
#                 print(f"✅ Inserted historical data for {data['Name']} into ClickHouse")
#                 client.insert('stock_data.historical_stock_data', historical_data)
#                 print(f"✅ Inserted historical data for {data['Name']} into ClickHouse")
#             except Exception as e:
#                 print(f"❌ Error inserting historical data into ClickHouse: {e}")

# except KeyboardInterrupt:
#     print("⚠️ Consumer stopped manually.")

# finally:
#     # Close the consumer to commit final offsets
#     consumer.close()



# from confluent_kafka import Consumer, KafkaException, KafkaError
# import json
# from clickhouse_connect import get_client
# from datetime import datetime

# # Kafka configuration
# consumer_config = {
#     'bootstrap.servers': 'localhost:9092',  # Kafka broker
#     'group.id': 'stock_data',               # Consumer group ID
#     'auto.offset.reset': 'earliest',        # Start consuming from the beginning if no offset is saved
# }

# # Configure ClickHouse client
# client = get_client(host='localhost', port=8123, username='default', password='')

# # Topics to subscribe
# tickers = ["ZOMATO.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
#            "TCS.NS", "LT.NS", "SBIN.NS", "ADANIGREEN.NS"]

# # Create the Kafka consumer
# consumer = Consumer(consumer_config)

# # Subscribe to the topics
# consumer.subscribe(tickers)

# try:
#     print("Consumer is starting. Waiting for messages...")

#     while True:
#         # Poll for messages
#         msg = consumer.poll(1.0)  # Wait for 1 second for a message

#         if msg is None:
#             continue  # No message received

#         if msg.error():
#             if msg.error().code() == KafkaError._PARTITION_EOF:
#                 continue  # Reached the end of a partition
#             else:
#                 raise KafkaException(msg.error())

#         # Decode the message
#         data = json.loads(msg.value().decode('utf-8'))
#         print(f"Received message: {data}")

#         # Convert `Formatted_Timestamp` from string to datetime object
#         Formatted_Timestamp = datetime.strptime(data["Formatted_Timestamp"], "%Y-%m-%d %H:%M:%S")

#         # Structure the data for ClickHouse insertion
#         data_tuple = (
#             data['Name'],
#             data['Current_Price'],
#             data['Open_Price'],
#             data['Previous_Close'],
#             data['Day_High'],
#             data['Day_Low'],
#             data['Bid_Price'],
#             data['Ask_Price'],
#             data['Company_Name'],
#             data['Sector'],
#             data['Industry'],
#             data['Country'],
#             data['Timestamp'],  # Original timestamp in milliseconds
#             Formatted_Timestamp,  # Proper datetime object
#             data['Volume']
#         )

#         try:
#             # Insert stock data into ClickHouse
#             client.insert('live_stock_data', [data_tuple])
#             print(f"Data inserted into ClickHouse: {data['Name']}")
#             client.insert('stock_data.live_stock_data', [data_tuple])
#             print(f"Data inserted into ClickHouse: {data}")
#         except Exception as e:
#             print(f"Failed to insert data into ClickHouse: {e}")

#         if data["1_Month_Old_Price"] is not None:
#             historical_data = [(
#                 data["Name"], data["1_Month_Old_Price"], data["1_Month_Old_Volume"],
#                 data['Timestamp'], Formatted_Timestamp
#             )]
#             try:
#                 client.insert('historical_stock_data', historical_data)
#                 print(f"Inserted historical data for {data['Name']} into ClickHouse")
#                 client.insert('stock_data.historical_stock_data', historical_data)
#                 print(f"Inserted historical data for {data['Name']} into ClickHouse")
#             except Exception as e:
#                 print(f"Failed to insert historical data: {e}")

# except KeyboardInterrupt:
#     print("Consumer stopped manually.")

# finally:
#     # Close the consumer to commit final offsets
#     consumer.close()






# # from confluent_kafka import Consumer, KafkaException, KafkaError
# # import json
# # import datetime
# # from clickhouse_connect import get_client

# # # Kafka configuration
# # consumer_config = {
# #     'bootstrap.servers': 'localhost:9092',
# #     'group.id': 'stock_data',
# #     'auto.offset.reset': 'earliest',
# # }

# # # Configure ClickHouse client
# # client = get_client(host='localhost', port=8123, username='default', password='')

# # # Topics to subscribe
# # tickers = ["ZOMATO.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
# #            "TCS.NS", "LT.NS", "SBIN.NS", "ADANIGREEN.NS"]

# # # Create the Kafka consumer
# # consumer = Consumer(consumer_config)

# # # Subscribe to the topics
# # consumer.subscribe(tickers)

# # try:
# #     print("Consumer is starting. Waiting for messages...")

# #     while True:
# #         msg = consumer.poll(1.0)

# #         if msg is None:
# #             continue  

# #         if msg.error():
# #             if msg.error().code() == KafkaError._PARTITION_EOF:
# #                 continue
# #             else:
# #                 raise KafkaException(msg.error())

# #         # Decode the message
# #         data = json.loads(msg.value().decode('utf-8'))
# #         print(f"Received message: {data}")

# #         # Convert timestamp from epoch seconds to formatted datetime
# #         timestamp_seconds = data["Timestamp"]
# #         formatted_time = datetime.datetime.utcfromtimestamp(timestamp_seconds).strftime('%Y-%m-%d %H:%M:%S')

# #         # Structure data to match ClickHouse table
# #         data_to_insert = [(data['Name'], data['Price'], timestamp_seconds, formatted_time)]

# #         try:
# #             # Insert stock data into ClickHouse
# #             client.insert('stock_data', data_to_insert)
# #             print(f"Data inserted into ClickHouse: {data}")
# #         except Exception as e:
# #             print(f"Failed to insert data into ClickHouse: {e}")

# # except KeyboardInterrupt:
# #     print("Consumer stopped manually.")

# # finally:
# #     consumer.close()






# # from confluent_kafka import Consumer, KafkaException, KafkaError
# # import json
# # from clickhouse_connect import get_client

# # # Kafka configuration
# # consumer_config = {
# #     'bootstrap.servers': 'localhost:9092',  # Kafka broker
# #     'group.id': 'stock_data',               # Consumer group ID
# #     'auto.offset.reset': 'earliest',        # Start consuming from the beginning if no offset is saved
# # }

# # # Configure ClickHouse client
# # client = get_client(host='localhost', port=8123, username='default', password='')

# # # Topics to subscribe
# # tickers = ["ZOMATO.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
# #            "TCS.NS", "LT.NS", "SBIN.NS", "ADANIGREEN.NS"]

# # # Create the Kafka consumer
# # consumer = Consumer(consumer_config)

# # # Subscribe to the topics
# # consumer.subscribe(tickers)

# # try:
# #     print("Consumer is starting. Waiting for messages...")

# #     while True:
# #         # Poll for messages
# #         msg = consumer.poll(1.0)  # Wait for 1 second for a message

# #         if msg is None:
# #             continue  # No message received

# #         if msg.error():
# #             if msg.error().code() == KafkaError._PARTITION_EOF:
# #                 # Reached the end of a partition
# #                 continue
# #             else:
# #                 raise KafkaException(msg.error())

# #         # Decode the message
# #         data = json.loads(msg.value().decode('utf-8'))
# #         print(f"Received message: {data}")

# #         # Convert timestamp from milliseconds to seconds
# #         timestamp_ = data["Timestamp"]

# #         # Structure the data to match ClickHouse table
# #         data_to_insert = [{
# #             "Name": data["Name"],
# #             "Price": data["Price"],
# #             "Timestamp": timestamp_  # Insert timestamp as seconds (int)
# #         }]
 
# #         try:
# #             # Insert stock data into ClickHouse
# #             data_tuple = (data['Name'], data['Price'], timestamp_)  # Insert timestamp in seconds as int
# #             client.insert('stock_data', [data_tuple])  # Insert the data as a list of tuples
# #             print(f"Data inserted into ClickHouse: {data}")
# #         except Exception as e:
# #             print(f"Failed to insert data into ClickHouse: {e}")

# # except KeyboardInterrupt:
# #     print("Consumer stopped manually.")

# # finally:
# #     # Close the consumer to commit final offsets
# #     consumer.close()
