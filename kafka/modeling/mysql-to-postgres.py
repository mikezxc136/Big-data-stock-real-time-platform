import datetime
import json
import threading
import time

import mysql.connector
import psycopg2

from kafka import KafkaConsumer, KafkaProducer


# Hàm để gửi dữ liệu từ MySQL vào Kafka
def stream_mysql_to_kafka():
    conn = mysql.connector.connect(
        user='mike',
        password='123',
        host='localhost',
        database='stock_stream'
    )
    cursor = conn.cursor(dictionary=True)
    
    query = "SELECT * FROM stock_data"
    cursor.execute(query)
    rows = cursor.fetchall()
    
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )
    
    for row in rows:
        # Convert datetime objects to strings
        if 'Datetime' in row and isinstance(row['Datetime'], datetime.datetime):
            row['Datetime'] = row['Datetime'].strftime('%Y-%m-%d %H:%M:%S')
        producer.send('mysql-to-postgres', value=row)
        producer.flush()
    
    cursor.close()
    conn.close()

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
            'mysql-to-postgres',
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
    # Tạo một thread để tiêu thụ dữ liệu từ Kafka và chèn vào PostgreSQL
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()

    # Gửi dữ liệu từ MySQL tới Kafka
    stream_mysql_to_kafka()

    # Keep the main thread running to allow the consumer thread to keep processing messages
    while True:
        time.sleep(6000)

if __name__ == "__main__":
    main()
