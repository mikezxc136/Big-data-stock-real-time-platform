import json

from kafka import KafkaConsumer, KafkaProducer

from .db_utils import insert_data


def consume_messages():
    try:
        consumer = KafkaConsumer(
            'stock_topic',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for message in consumer:
            print("Received message:", message.value)
            try:
                insert_data(message.value)
            except KeyError as e:
                print(f"KeyError: {e} in message {message.value}")
            except Exception as e:
                print(f"Error inserting data: {e}")
    except Exception as e:
        print(f"Error setting up Kafka consumer: {e}")

def verify_streamed_data():
    consumer = KafkaConsumer(
        'mysql-to-postgres',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='verification-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Listening to 'mysql-to-postgres' topic...")
    for message in consumer:
        print(f"Received message: {message.value}")

def send_to_kafka(data, producer, topic, ticker):
    for index, row in data.iterrows():
        datetime_key = 'Datetime' if 'Datetime' in row else 'Date'
        message = {
            'Ticker': ticker,
            'Datetime': row[datetime_key].strftime('%Y-%m-%d %H:%M:%S'),
            'Open': float(row['Open']),
            'High': float(row['High']),
            'Low': float(row['Low']),
            'Close': float(row['Close']),
            'Volume': int(row['Volume'])
        }
        producer.send(topic, value=message)
        producer.flush()
    if not data.empty:
        update_last_processed_time(ticker, data[datetime_key].max())
