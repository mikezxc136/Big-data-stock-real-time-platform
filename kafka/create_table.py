import psycopg2


def create_table():
    conn = psycopg2.connect(
        dbname="test",
        user="postgres",
        password="123",
        host="localhost"
    )
    cursor = conn.cursor()
    cursor.execute("""
    DROP TABLE IF EXISTS stock_table;
    CREATE TABLE stock_table (
        datetime TIMESTAMP PRIMARY KEY,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume BIGINT
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_table()
