import datetime
import json
import logging
import threading
import time
from typing import Dict, Optional

import mysql.connector
import pandas as pd
import yfinance as yf

from kafka import KafkaConsumer, KafkaProducer

CSV_FILE_PATH = 'D:\\Github Mikezxc\\Big-data-stock-real-time-platform\\kafka\\modeling\\tickers.csv'
MYSQL_CONFIG_PATH = 'D:\\Github Mikezxc\\Big-data-stock-real-time-platform\\kafka\\modeling\\config\\env_mysql.json'

def load_mysql_config() -> Dict:
    with open(MYSQL_CONFIG_PATH, 'r') as file:
        return json.load(file)

mysql_config = load_mysql_config()

def fetch_stock_data(ticker: str, start_date: datetime.datetime, end_date: datetime.datetime, interval: str) -> pd.DataFrame:
    stock = yf.Ticker(ticker)
    data = stock.history(start=start_date, end=end_date, interval=interval)
    data.reset_index(inplace=True)
    return data

def send_to_kafka(data: pd.DataFrame, producer: KafkaProducer, topic: str, ticker: str, timeframe: str) -> None:
    for _, row in data.iterrows():
        datetime_key = 'Datetime' if 'Datetime' in row else 'Date'
        message = {
            'Ticker': ticker,
            'Datetime': row[datetime_key].strftime('%Y-%m-%d %H:%M:%S'),
            'Open': float(row['Open']),
            'High': float(row['High']),
            'Low': float(row['Low']),
            'Close': float(row['Close']),
            'Volume': int(row['Volume']),
            'Timeframe': timeframe
        }
        producer.send(topic, value=message)
        producer.flush()
    if not data.empty:
        update_last_processed_time(ticker, data[datetime_key].max())

def update_last_processed_time(ticker: str, last_processed_time: datetime.datetime) -> None:
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()
    query = """
    INSERT INTO ticker_status (Ticker, LastProcessedTime)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        LastProcessedTime = VALUES(LastProcessedTime)
    """
    cursor.execute(query, (ticker, last_processed_time))
    conn.commit()
    cursor.close()
    conn.close()

def get_last_processed_time(ticker: str) -> Optional[datetime.datetime]:
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()
    query = "SELECT LastProcessedTime FROM ticker_status WHERE Ticker = %s"
    cursor.execute(query, (ticker,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] if result else None

def create_database_and_table() -> None:
    conn = mysql.connector.connect(
        user=mysql_config['user'],
        password=mysql_config['password'],
        host=mysql_config['host'],
        port=mysql_config['port']
    )
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS stock_stream")
    cursor.execute("USE stock_stream")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Ticker VARCHAR(10),
        Datetime DATETIME,
        Open FLOAT,
        High FLOAT,
        Low FLOAT,
        Close FLOAT,
        Volume INT,
        Timeframe VARCHAR(10)
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ticker_status (
        Ticker VARCHAR(10) PRIMARY KEY,
        LastProcessedTime DATETIME
    )
    """)
    conn.commit()
    cursor.close()
    conn.close()

def insert_data(data: Dict) -> None:
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()
    query = """
    SELECT COUNT(*) FROM stock_data
    WHERE Ticker = %s AND Datetime = %s AND Timeframe = %s
    """
    cursor.execute(query, (data['Ticker'], data['Datetime'], data['Timeframe']))
    count = cursor.fetchone()[0]
    if count == 0:
        query = """
        INSERT INTO stock_data (Ticker, Datetime, Open, High, Low, Close, Volume, Timeframe)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Open = VALUES(Open),
            High = VALUES(High),
            Low = VALUES(Low),
            Close = VALUES(Close),
            Volume = VALUES(Volume)
        """
        cursor.execute(query, (
            data['Ticker'], data['Datetime'], data['Open'],
            data['High'], data['Low'], data['Close'], data['Volume'],
            data['Timeframe']
        ))
        conn.commit()
    cursor.close()
    conn.close()

def consume_messages() -> None:
    try:
        consumer = KafkaConsumer(
            'stock_topic',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("Started consuming messages from Kafka topic: 'stock_topic'")
        for message in consumer:
            try:
                print(f"Received message: {message.value}")
                insert_data(message.value)
            except KeyError as e:
                print(f"KeyError: {e} in message {message.value}")
            except Exception as e:
                print(f"Error inserting data: {e}")
    except Exception as e:
        print(f"Error setting up Kafka consumer: {e}")

def stream_mysql_to_kafka() -> None:
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor(dictionary=True)
    query = "SELECT * FROM stock_data"
    cursor.execute(query)
    rows = cursor.fetchall()
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for row in rows:
        producer.send('mysql-to-postgres', value=row)
        producer.flush()
    cursor.close()
    conn.close()

def verify_streamed_data() -> None:
    consumer = KafkaConsumer(
        'mysql-to-postgres',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='verification-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("Started verifying streamed data from Kafka topic: 'mysql-to-postgres'")
    for message in consumer:
        print(f"Received message for verification: {message.value}")

def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    create_database_and_table()
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()
    tickers_df = pd.read_csv(CSV_FILE_PATH)
    tickers = tickers_df['Ticker'].tolist()
    end_date = datetime.datetime.now()

    for ticker in tickers:
        last_processed_time = get_last_processed_time(ticker)
        start_date = last_processed_time if last_processed_time else datetime.datetime(2015, 1, 1)
        historical_data = fetch_stock_data(ticker, start_date, end_date, interval='1d')
        if not historical_data.empty:
            send_to_kafka(historical_data, producer, 'stock_topic', ticker, '1d')

    recent_start_time = end_date - datetime.timedelta(days=7)
    intervals = ['1h', '15m', '5m', '1m']

    while True:
        try:
            current_time = datetime.datetime.now()
            for ticker in tickers:
                last_processed_time = get_last_processed_time(ticker) or recent_start_time
                for interval in intervals:
                    recent_data = fetch_stock_data(ticker, last_processed_time, current_time, interval=interval)
                    if not recent_data.empty:
                        send_to_kafka(recent_data, producer, 'stock_topic', ticker, interval)
            time.sleep(600)
        except Exception as e:
            print(f"Error fetching or sending data: {e}")

    verification_thread = threading.Thread(target=verify_streamed_data, daemon=True)
    verification_thread.start()

if __name__ == "__main__":
    main()
