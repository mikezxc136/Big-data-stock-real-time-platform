from kafka import KafkaProducer


def check_kafka_broker():
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        print("Kafka broker is running")
        producer.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_kafka_broker()
