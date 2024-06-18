import psycopg2


def create_tables():
    conn = psycopg2.connect(
        dbname="test",
        user="postgres",
        password="123",
        host="localhost"
    )
    cursor = conn.cursor()

    # Tạo bảng dim_stock
    cursor.execute("""
    DROP TABLE IF EXISTS dim_stock CASCADE;
    CREATE TABLE dim_stock (
        stock_id SERIAL PRIMARY KEY,
        ticker VARCHAR(10) NOT NULL UNIQUE
    );
    """)

    # Tạo bảng dim_date
    cursor.execute("""
    DROP TABLE IF EXISTS dim_date CASCADE;
    CREATE TABLE dim_date (
        date_id SERIAL PRIMARY KEY,
        date TIMESTAMP NOT NULL UNIQUE
    );
    """)

    # Tạo bảng fact_stock
    cursor.execute("""
    DROP TABLE IF EXISTS fact_stock CASCADE;
    CREATE TABLE fact_stock (
        stock_id INT REFERENCES dim_stock(stock_id),
        date_id INT REFERENCES dim_date(date_id),
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume BIGINT,
        PRIMARY KEY (stock_id, date_id)
    );
    """)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_tables()
