import datetime
import json
import logging
import os

import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType, TimestampType)

from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session():
    try:
        spark = SparkSession.builder \
            .appName("KafkaToPostgres") \
            .config("spark.jars", "file:///C:/Spark/spark-3.5.1-bin-hadoop3/jars/spark-sql-kafka-0-10_2.13-3.5.1.jar") \
            .getOrCreate()
        logging.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        raise

def stream_mysql_to_kafka():
    try:
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
                logging.info(f"Sent row to Kafka: {row}")
            except Exception as e:
                logging.error(f"Error sending message to Kafka: {e}")
            producer.flush()

        cursor.close()
        conn.close()
        logging.info("MySQL to Kafka streaming completed successfully.")
    except Exception as e:
        logging.error(f"Error in stream_mysql_to_kafka: {e}")
        raise

def process_stream():
    try:
        spark = create_spark_session()

        schema = StructType([
            StructField("Ticker", StringType(), True),
            StructField("Datetime", TimestampType(), True),
            StructField("Timeframe", StringType(), True),
            StructField("Open", DoubleType(), True),
            StructField("High", DoubleType(), True),
            StructField("Low", DoubleType(), True),
            StructField("Close", DoubleType(), True),
            StructField("Volume", LongType(), True)
        ])

        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "mysql-to-postgres") \
            .load()

        kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

        data_df = kafka_df \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

        data_df.writeStream \
            .foreachBatch(insert_data) \
            .start() \
            .awaitTermination()
    except Exception as e:
        logging.error(f"Error in process_stream: {e}")
        raise

def insert_data(df, epoch_id):
    try:
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/test") \
            .option("dbtable", "fact_stock") \
            .option("user", "postgres") \
            .option("password", "123") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        logging.info(f"Batch {epoch_id} written to PostgreSQL successfully.")
    except Exception as e:
        logging.error(f"Error inserting data into PostgreSQL: {e}")
        raise

def main():
    try:
        stream_mysql_to_kafka()
        process_stream()
    except Exception as e:
        logging.error(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    main()
