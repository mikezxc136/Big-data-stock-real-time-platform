import datetime
import json

import mysql.connector

from kafka import KafkaProducer

MYSQL_CONFIG_PATH = 'D:\\Github Mikezxc\\Big-data-stock-real-time-platform\\kafka\\modeling\\config\\env_mysql.json'

def load_mysql_config():
    with open(MYSQL_CONFIG_PATH, 'r') as file:
        return json.load(file)

mysql_config = load_mysql_config()

def update_last_processed_time(ticker, last_processed_time):
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()
    query = """
    INSERT INTO ticker_status (Ticker, LastProcessedTime)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        LastProcessedTime = VALUES(LastProcessedTime)
    """
    cursor.execute(query, (ticker, last_processed_time))
    conn.commit()
    cursor.close()
    conn.close()

def get_last_processed_time(ticker):
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()
    query = "SELECT LastProcessedTime FROM ticker_status WHERE Ticker = %s"
    cursor.execute(query, (ticker,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] if result else None

def create_database_and_table():
    conn = mysql.connector.connect(
        user=mysql_config['user'],
        password=mysql_config['password'],
        host=mysql_config['host'],
        port=mysql_config['port']
    )
    cursor = conn.cursor()

    cursor.execute("CREATE DATABASE IF NOT EXISTS stock_stream")
    cursor.execute("USE stock_stream")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Ticker VARCHAR(10),
        Datetime DATETIME,
        Open FLOAT,
        High FLOAT,
        Low FLOAT,
        Close FLOAT,
        Volume INT
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ticker_status (
        Ticker VARCHAR(10) PRIMARY KEY,
        LastProcessedTime DATETIME
    )
    """)

    conn.commit()
    cursor.close()
    conn.close()

def insert_data(data):
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    query = """
    SELECT COUNT(*) FROM stock_data
    WHERE Ticker = %s AND Datetime = %s
    """
    cursor.execute(query, (data['Ticker'], data['Datetime']))
    count = cursor.fetchone()[0]

    if count == 0:
        query = """
        INSERT INTO stock_data (Ticker, Datetime, Open, High, Low, Close, Volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Open = VALUES(Open),
            High = VALUES(High),
            Low = VALUES(Low),
            Close = VALUES(Close),
            Volume = VALUES(Volume)
        """
        values = (
            data['Ticker'],
            data['Datetime'],
            data['Open'],
            data['High'],
            data['Low'],
            data['Close'],
            data['Volume']
        )

        cursor.execute(query, values)
        conn.commit()

    cursor.close()
    conn.close()

def stream_mysql_to_kafka():
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor(dictionary=True)

    query = "SELECT * FROM stock_data"
    cursor.execute(query)
    rows = cursor.fetchall()

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for row in rows:
        producer.send('mysql-to-postgres', value=row)
        producer.flush()

    cursor.close()
    conn.close()

def get_start_of_current_day():
    now = datetime.datetime.now()
    return datetime.datetime(now.year, now.month, now.day)

def consolidate_minute_data_to_daily(ticker):
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    query = """
    SELECT 
        Ticker, 
        DATE(Datetime) as Date, 
        MIN(Open) as Open, 
        MAX(High) as High, 
        MIN(Low) as Low, 
        MAX(Datetime) as CloseTime,
        (SELECT Close FROM stock_data WHERE Ticker = %s AND Datetime = CloseTime) as Close,
        SUM(Volume) as Volume
    FROM stock_data
    WHERE Ticker = %s AND DATE(Datetime) = CURDATE() - INTERVAL 1 DAY
    GROUP BY Ticker, Date
    """
    
    cursor.execute(query, (ticker, ticker))
    consolidated_data = cursor.fetchall()
    
    if consolidated_data:
        for row in consolidated_data:
            consolidated_row = {
                'Ticker': row[0],
                'Datetime': row[1].strftime('%Y-%m-%d 00:00:00'),
                'Open': row[2],
                'High': row[3],
                'Low': row[4],
                'Close': row[6],
                'Volume': row[7]
            }
            insert_data(consolidated_row)
        
        delete_query = """
        DELETE FROM stock_data
        WHERE Ticker = %s AND DATE(Datetime) = CURDATE() - INTERVAL 1 DAY
        """
        cursor.execute(delete_query, (ticker,))
        conn.commit()

    cursor.close()
    conn.close()
