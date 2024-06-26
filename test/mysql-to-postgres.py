import datetime
import json
import logging
import threading
import time

import mysql.connector
import psycopg2

from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO)

def create_tables():
    conn = psycopg2.connect(
        dbname="test",
        user="postgres",
        password="123",
        host="localhost"
    )
    cursor = conn.cursor()

    cursor.execute("""
    DROP TABLE IF EXISTS dim_stock CASCADE;
    CREATE TABLE dim_stock (
        stock_id SERIAL PRIMARY KEY,
        ticker VARCHAR(10) NOT NULL UNIQUE
    );
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS dim_date CASCADE;
    CREATE TABLE dim_date (
        date_id SERIAL PRIMARY KEY,
        date TIMESTAMP NOT NULL UNIQUE,
        timeframe VARCHAR(10) NOT NULL
    );
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS dim_price CASCADE;
    CREATE TABLE dim_price (
        price_id SERIAL PRIMARY KEY,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC
    );
    """)

    cursor.execute("""
    DROP TABLE IF EXISTS fact_stock CASCADE;
    CREATE TABLE fact_stock (
        stock_id INT REFERENCES dim_stock(stock_id),
        date_id INT REFERENCES dim_date(date_id),
        price_id INT REFERENCES dim_price(price_id),
        volume BIGINT,
        PRIMARY KEY (stock_id, date_id)
    );
    """)

    conn.commit()
    cursor.close()
    conn.close()

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
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        batch_size=16384,
        linger_ms=10,
        retries=5,
        compression_type='gzip'
    )

    for row in rows:
        if 'Datetime' in row and isinstance(row['Datetime'], datetime.datetime):
            row['Datetime'] = row['Datetime'].strftime('%Y-%m-%d %H:%M:%S')
        try:
            producer.send('mysql-to-postgres', value=row)
        except Exception as e:
            logging.error(f"Error sending message to Kafka: {e}")
        producer.flush()

    cursor.close()
    conn.close()

def insert_data(data):
    conn = psycopg2.connect(
        dbname="test",
        user="postgres",
        password="123",
        host="localhost"
    )
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO dim_stock (ticker)
    VALUES (%s)
    ON CONFLICT (ticker) DO NOTHING;
    """, (data['Ticker'],))
    cursor.execute("SELECT stock_id FROM dim_stock WHERE ticker = %s", (data['Ticker'],))
    stock_id = cursor.fetchone()[0]

    cursor.execute("""
    INSERT INTO dim_date (date, timeframe)
    VALUES (%s, %s)
    ON CONFLICT (date) DO NOTHING;
    """, (data['Datetime'], data['Timeframe']))
    cursor.execute("SELECT date_id FROM dim_date WHERE date = %s", (data['Datetime'],))
    date_id = cursor.fetchone()[0]

    cursor.execute("""
    INSERT INTO dim_price (open, high, low, close)
    VALUES (%s, %s, %s, %s)
    RETURNING price_id;
    """, (data['Open'], data['High'], data['Low'], data['Close']))
    price_id = cursor.fetchone()[0]

    cursor.execute("""
    INSERT INTO fact_stock (stock_id, date_id, price_id, volume)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (stock_id, date_id) DO NOTHING;
    """, (stock_id, date_id, price_id, data['Volume']))

    conn.commit()
    cursor.close()
    conn.close()

def consume_messages():
    try:
        consumer = KafkaConsumer(
            'mysql-to-postgres',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='my-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            max_poll_records=500
        )
        for message in consumer:
            logging.info(f"Received message: {message.value}")
            try:
                insert_data(message.value)
                consumer.commit()
            except KeyError as e:
                logging.error(f"KeyError: {e} in message {message.value}")
            except Exception as e:
                logging.error(f"Error inserting data: {e}")
    except Exception as e:
        logging.error(f"Error setting up Kafka consumer: {e}")

def main():
    create_tables()

    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()

    stream_mysql_to_kafka()

    while True:
        time.sleep(6000)

if __name__ == "__main__":
    main()
