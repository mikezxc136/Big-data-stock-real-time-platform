import json
import threading
import time

import psycopg2
import yfinance as yf

from kafka import KafkaConsumer, KafkaProducer


# Hàm để lấy dữ liệu cổ phiếu
def fetch_stock_data(ticker, period='1d', interval='1m'):
    stock = yf.Ticker(ticker)
    data = stock.history(period=period, interval=interval)
    data.reset_index(inplace=True)
    return data

# Hàm để gửi dữ liệu vào Kafka
def send_to_kafka(data, producer, topic):
    for index, row in data.iterrows():
        message = {
            'Datetime': str(row['Datetime']),
            'Open': row['Open'],
            'High': row['High'],
            'Low': row['Low'],
            'Close': row['Close'],
            'Volume': row['Volume']
        }
        producer.send(topic, value=message)
        producer.flush()

# Hàm để chèn dữ liệu vào PostgreSQL
def insert_data(data):
    conn = psycopg2.connect(
        dbname="test",
        user="postgres",
        password="123",
        host="localhost"
    )
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO stock_table (datetime, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (datetime) DO NOTHING;
    """, (data['Datetime'], data['Open'], data['High'], data['Low'], data['Close'], data['Volume']))
    conn.commit()
    cursor.close()
    conn.close()

# Hàm tiêu thụ dữ liệu từ Kafka và chèn vào PostgreSQL
def consume_messages():
    consumer = KafkaConsumer(
        'stock_topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',  # Đổi từ 'earliest' thành 'latest'
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer:
        insert_data(message.value)

# Hàm để tạo bảng PostgreSQL
def create_table():
    conn = psycopg2.connect(
        dbname="test",
        user="postgres",
        password="123",
        host="localhost"
    )
    cursor = conn.cursor()
    cursor.execute("""
    DROP TABLE IF EXISTS stock_table;
    CREATE TABLE stock_table (
        datetime TIMESTAMP PRIMARY KEY,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume BIGINT
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    # Tạo bảng trong PostgreSQL
    create_table()

    # Thiết lập Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Tạo một thread để tiêu thụ dữ liệu từ Kafka và chèn vào PostgreSQL
    consumer_thread = threading.Thread(target=consume_messages)
    consumer_thread.start()

    # Lấy dữ liệu cổ phiếu và gửi vào Kafka
    ticker = 'AAPL'  # Thay bằng mã cổ phiếu bạn muốn lấy
    while True:
        data = fetch_stock_data(ticker, period='1d', interval='1m')
        send_to_kafka(data, producer, 'stock_topic')
        time.sleep(60)  # Đợi 1 phút trước khi lấy dữ liệu mới
