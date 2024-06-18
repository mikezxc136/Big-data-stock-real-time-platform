import json

import psycopg2

from kafka import KafkaConsumer


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

if __name__ == "__main__":
    consume_messages()
