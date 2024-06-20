import datetime
import json
import threading
import time

import pandas as pd
import psycopg2
import yfinance as yf

from kafka import KafkaConsumer, KafkaProducer

# Path to the CSV file containing tickers
CSV_FILE_PATH = 'D:\\Github Mikezxc\\Big-data-stock-real-time-platform\\kafka\\modeling_test\\tickers.csv'

# Hàm để lấy dữ liệu cổ phiếu
def fetch_stock_data(ticker, start_date, end_date, interval='1d'):
    stock = yf.Ticker(ticker)
    data = stock.history(start=start_date, end=end_date, interval=interval)
    data.reset_index(inplace=True)
    return data

# Hàm để gửi dữ liệu vào Kafka
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

    # Chèn vào dim_price
    cursor.execute("""
    INSERT INTO dim_price (open, high, low, close)
    VALUES (%s, %s, %s, %s)
    RETURNING price_id;
    """, (data['Open'], data['High'], data['Low'], data['Close']))
    price_id = cursor.fetchone()[0]

    # Chèn vào fact_stock
    cursor.execute("""
    INSERT INTO fact_stock (stock_id, date_id, price_id, volume)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (stock_id, date_id) DO NOTHING;
    """, (stock_id, date_id, price_id, data['Volume']))

    conn.commit()
    cursor.close()
    conn.close()

# Hàm tiêu thụ dữ liệu từ Kafka và chèn vào PostgreSQL
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
            print("Received message:", message.value)  # Thêm dòng này để kiểm tra dữ liệu nhận được
            try:
                insert_data(message.value)
            except KeyError as e:
                print(f"KeyError: {e} in message {message.value}")
            except Exception as e:
                print(f"Error inserting data: {e}")
    except Exception as e:
        print(f"Error setting up Kafka consumer: {e}")

# Hàm chính
def main():
    # Thiết lập Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Tạo một thread để tiêu thụ dữ liệu từ Kafka và chèn vào PostgreSQL
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()

    # Đọc file CSV để lấy danh sách ticker
    tickers_df = pd.read_csv(CSV_FILE_PATH)
    tickers = tickers_df['Ticker'].tolist()

    # Lấy dữ liệu cổ phiếu và gửi vào Kafka
    start_date = datetime.datetime(2015, 1, 1)
    end_date = datetime.datetime.now()

    for ticker in tickers:
        # Fetch historical data in daily intervals
        historical_data = fetch_stock_data(ticker, start_date, end_date, interval='1d')
        send_to_kafka(historical_data, producer, 'stock_topic', ticker)

    # Continue fetching recent data in 1-minute intervals
    while True:
        try:
            current_time = datetime.datetime.now()
            recent_start_time = current_time - datetime.timedelta(days=1)
            for ticker in tickers:
                recent_data = fetch_stock_data(ticker, recent_start_time, current_time, interval='1m')
                if not recent_data.empty:
                    send_to_kafka(recent_data, producer, 'stock_topic', ticker)
            time.sleep(600)  # Đợi 10 phút trước khi lấy dữ liệu mới
        except Exception as e:
            print(f"Error fetching or sending data: {e}")

if __name__ == "__main__":
    main()
