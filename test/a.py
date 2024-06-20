import json
import threading
import time

import psycopg2
import yfinance as yf

from kafka import KafkaConsumer, KafkaProducer


# Hàm để tạo bảng
def create_tables():
    conn = psycopg2.connect(
        dbname="test",
        user="postgres",
        password="123",
        host="localhost"
    )
    cursor = conn.cursor()

    # Tạo bảng dim_stock
    cursor.execute("""
    DROP TABLE IF EXISTS dim_stock CASCADE;
    CREATE TABLE dim_stock (
        stock_id SERIAL PRIMARY KEY,
        ticker VARCHAR(10) NOT NULL UNIQUE
    );
    """)

    # Tạo bảng dim_date
    cursor.execute("""
    DROP TABLE IF EXISTS dim_date CASCADE;
    CREATE TABLE dim_date (
        date_id SERIAL PRIMARY KEY,
        date TIMESTAMP NOT NULL UNIQUE
    );
    """)

    # Tạo bảng fact_stock
    cursor.execute("""
    DROP TABLE IF EXISTS fact_stock CASCADE;
    CREATE TABLE fact_stock (
        stock_id INT REFERENCES dim_stock(stock_id),
        date_id INT REFERENCES dim_date(date_id),
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume BIGINT,
        PRIMARY KEY (stock_id, date_id)
    );
    """)

    conn.commit()
    cursor.close()
    conn.close()

# Hàm để lấy dữ liệu cổ phiếu
def fetch_stock_data(ticker, period='1d', interval='1m'):
    stock = yf.Ticker(ticker)
    data = stock.history(period=period, interval=interval)
    data.reset_index(inplace=True)
    return data

# Hàm để gửi dữ liệu vào Kafka
def send_to_kafka(data, producer, topic, ticker):
    for index, row in data.iterrows():
        message = {
            'Ticker': ticker,
            'Datetime': row['Datetime'].strftime('%Y-%m-%d %H:%M:%S'),
            'Open': float(row['Open']),
            'High': float(row['High']),
            'Low': float(row['Low']),
            'Close': float(row['Close']),
            'Volume': int(row['Volume'])
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

    # Chèn vào dim_stock nếu chưa tồn tại
    cursor.execute("""
    INSERT INTO dim_stock (ticker)
    VALUES (%s)
    ON CONFLICT (ticker) DO NOTHING;
    """, (data['Ticker'],))
    cursor.execute("SELECT stock_id FROM dim_stock WHERE ticker = %s", (data['Ticker'],))
    stock_id = cursor.fetchone()[0]

    # Chèn vào dim_date nếu chưa tồn tại
    cursor.execute("""
    INSERT INTO dim_date (date)
    VALUES (%s)
    ON CONFLICT (date) DO NOTHING;
    """, (data['Datetime'],))
    cursor.execute("SELECT date_id FROM dim_date WHERE date = %s", (data['Datetime'],))
    date_id = cursor.fetchone()[0]

    # Chèn vào fact_stock
    cursor.execute("""
    INSERT INTO fact_stock (stock_id, date_id, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (stock_id, date_id) DO NOTHING;
    """, (stock_id, date_id, data['Open'], data['High'], data['Low'], data['Close'], data['Volume']))

    conn.commit()
    cursor.close()
    conn.close()

# Hàm tiêu thụ dữ liệu từ Kafka và chèn vào PostgreSQL
def consume_messages():
    consumer = KafkaConsumer(
        'stock_topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer:
        print("Received message:", message.value)  # Thêm dòng này để kiểm tra dữ liệu nhận được
        try:
            insert_data(message.value)
        except KeyError as e:
            print(f"KeyError: {e} in message {message.value}")
        except Exception as e:
            print(f"Error inserting data: {e}")

# Hàm chính
def main():
    # Thiết lập Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Tạo bảng trong database
    create_tables()

    # Tạo một thread để tiêu thụ dữ liệu từ Kafka và chèn vào PostgreSQL
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()

    # Lấy dữ liệu cổ phiếu và gửi vào Kafka
    ticker = 'AAPL'  # Thay bằng mã cổ phiếu bạn muốn lấy
    while True:
        try:
            data = fetch_stock_data(ticker, period='1d', interval='1m')
            send_to_kafka(data, producer, 'stock_topic', ticker)
            time.sleep(60)  # Đợi 1 phút trước khi lấy dữ liệu mới
        except Exception as e:
            print(f"Error fetching or sending data: {e}")

if __name__ == "__main__":
    main()
