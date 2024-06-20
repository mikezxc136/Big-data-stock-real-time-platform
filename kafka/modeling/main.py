import datetime
import json
import threading
import time

import pandas as pd
import utils
from utils_mysql.db_utils import (create_database_and_table,
                                  get_last_processed_time,
                                  stream_mysql_to_kafka)
from utils_mysql.kafka_utils import (consume_messages, send_to_kafka,
                                     verify_streamed_data)
from utils_mysql.stock_utils import fetch_stock_data

from kafka import KafkaProducer

# Path to the CSV file containing tickers
CSV_FILE_PATH = 'D:\\Github Mikezxc\\Big-data-stock-real-time-platform\\kafka\\modeling\\tickers.csv'

# Hàm chính
def main():
    # Thiết lập Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Tạo database và bảng nếu chưa tồn tại
    create_database_and_table()

    # Tạo một thread để tiêu thụ dữ liệu từ Kafka và chèn vào MySQL
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()

    # Đọc file CSV để lấy danh sách ticker
    tickers_df = pd.read_csv(CSV_FILE_PATH)
    tickers = tickers_df['Ticker'].tolist()

    # Lấy dữ liệu cổ phiếu và gửi vào Kafka
    end_date = datetime.datetime.now()

    for ticker in tickers:
        last_processed_time = get_last_processed_time(ticker)
        start_date = last_processed_time if last_processed_time else datetime.datetime(2015, 1, 1)
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

    # Tạo một thread để xác minh dữ liệu đã được stream tới topic mới của Kafka
    verification_thread = threading.Thread(target=verify_streamed_data, daemon=True)
    verification_thread.start()

if __name__ == "__main__":
    main()
