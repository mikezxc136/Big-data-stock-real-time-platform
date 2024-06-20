import json

import yfinance as yf

from kafka import KafkaProducer


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

if __name__ == "__main__":
    # Thiết lập Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Lấy dữ liệu cổ phiếu
    ticker = 'AAPL'  # Thay bằng mã cổ phiếu bạn muốn lấy
    data = fetch_stock_data(ticker, period='1d', interval='1m')

    # Gửi dữ liệu vào Kafka topic
    send_to_kafka(data, producer, 'stock_topic')
