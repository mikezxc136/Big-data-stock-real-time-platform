import json

from kafka import KafkaConsumer


def verify_streamed_data():
    consumer = KafkaConsumer(
        'mysql-to-postgres',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='verification-group-new',  # Ensure this is a new group_id
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Listening to 'mysql-to-postgres' topic...")

    with open('output.txt', 'a') as file:
        for message in consumer:
            output_message = f"Received message: {message.value}\n"
            print(output_message)
            file.write(output_message)

if __name__ == "__main__":
    verify_streamed_data()
