import json

from kafka import KafkaConsumer


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

if __name__ == "__main__":
    verify_streamed_data()
