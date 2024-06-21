import datetime
import json
import threading
import time

import mysql.connector
import psycopg2

from kafka import KafkaConsumer, KafkaProducer


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
        date TIMESTAMP NOT NULL UNIQUE
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
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )

    for row in rows:
        if 'Datetime' in row and isinstance(row['Datetime'], datetime.datetime):
            row['Datetime'] = row['Datetime'].strftime('%Y-%m-%d %H:%M:%S')
        producer.send('mysql-to-postgres', value=row)
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
    INSERT INTO dim_date (date)
    VALUES (%s)
    ON CONFLICT (date) DO NOTHING;
    """, (data['Datetime'],))
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


def main():
    create_tables()

    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()

    stream_mysql_to_kafka()

    while True:
        time.sleep(6000)


if __name__ == "__main__":
    main()
