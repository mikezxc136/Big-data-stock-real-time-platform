import json
import logging
import os

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType, TimestampType)

logging.basicConfig(level=logging.INFO)

def create_spark_session():
    spark = SparkSession.builder \
        .appName("KafkaToPostgres") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,org.postgresql:postgresql:42.2.23") \
        .getOrCreate()
    logging.info("Spark session created.")
    return spark

def create_tables():
    try:
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
        logging.info("PostgreSQL tables created successfully.")
    except Exception as e:
        logging.error(f"Error creating tables in PostgreSQL: {e}")
        raise

def process_stream():
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

    def write_to_postgres(df, epoch_id):
        try:
            conn = psycopg2.connect(
                dbname="test",
                user="postgres",
                password="123",
                host="localhost"
            )
            cursor = conn.cursor()

            for row in df.collect():
                logging.info(f"Inserting row: {row.asDict()}")

                cursor.execute("""
                INSERT INTO dim_stock (ticker)
                VALUES (%s)
                ON CONFLICT (ticker) DO NOTHING;
                """, (row['Ticker'],))
                cursor.execute("SELECT stock_id FROM dim_stock WHERE ticker = %s", (row['Ticker'],))
                stock_id = cursor.fetchone()[0]

                cursor.execute("""
                INSERT INTO dim_date (date, timeframe)
                VALUES (%s, %s)
                ON CONFLICT (date) DO NOTHING;
                """, (row['Datetime'], row['Timeframe']))
                cursor.execute("SELECT date_id FROM dim_date WHERE date = %s", (row['Datetime'],))
                date_id = cursor.fetchone()[0]

                cursor.execute("""
                INSERT INTO dim_price (open, high, low, close)
                VALUES (%s, %s, %s, %s)
                RETURNING price_id;
                """, (row['Open'], row['High'], row['Low'], row['Close']))
                price_id = cursor.fetchone()[0]

                cursor.execute("""
                INSERT INTO fact_stock (stock_id, date_id, price_id, volume)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (stock_id, date_id) DO NOTHING;
                """, (stock_id, date_id, price_id, row['Volume']))

            conn.commit()
            cursor.close()
            conn.close()
            logging.info("Batch processed and written to PostgreSQL.")
        except Exception as e:
            logging.error(f"Error writing to PostgreSQL: {e}")
            raise

    query = data_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .start()

    query.awaitTermination()

def main():
    create_tables()  # Create necessary tables in PostgreSQL
    process_stream()  # Process the Kafka stream and write to PostgreSQL

if __name__ == "__main__":
    main()
