import json
from kafka import KafkaConsumer
import mysql.connector
from mysql.connector import Error

KAFKA_SERVER = "localhost:9092"
STOCK_TOPIC = "stock-topic"
MARKET_TOPIC = "market-topic"
KAFKA_GROUP_ID = "stock-market-sql-consumer-v2"

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "Space1996@"
MYSQL_DATABASE = "stock_market_db"


def create_connection():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASSWORD
        )

        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}`")
        cur.close()
        conn.close()

        conn = mysql.connector.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        print(f"MySQL connected: {MYSQL_DATABASE}")
        return conn

    except Error as e:
        print(f"MySQL connection failed: {e}")
        return None


def create_tables(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            timestamp DATETIME NOT NULL,
            open_price DECIMAL(15,2),
            current_price DECIMAL(15,2),
            close_price DECIMAL(15,2),
            percentage_change DECIMAL(10,4),
            received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_stock_name (name),
            INDEX idx_stock_timestamp (timestamp)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            market_name VARCHAR(50) NOT NULL,
            timestamp DATETIME NOT NULL,
            open_price DECIMAL(15,2),
            current_price DECIMAL(15,2),
            close_price DECIMAL(15,2),
            percentage_change DECIMAL(10,4),
            received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_market_name (market_name),
            INDEX idx_market_timestamp (timestamp)
        )
    """)

    conn.commit()

    # If an older market_data table has "name", migrate it.
    cur.execute("""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME='market_data'
        AND COLUMN_NAME='name'
    """, (MYSQL_DATABASE,))
    old_name = cur.fetchone()[0] > 0

    cur.execute("""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME='market_data'
        AND COLUMN_NAME='market_name'
    """, (MYSQL_DATABASE,))
    new_name = cur.fetchone()[0] > 0

    if old_name and not new_name:
        print("Renaming market_data.name to market_name...")
        cur.execute("""
            ALTER TABLE market_data
            CHANGE COLUMN name market_name VARCHAR(50) NOT NULL
        """)
        conn.commit()

    cur.close()
    print("Tables ready: stock_data, market_data")


def insert_stock_data(conn, data):
    query = """
        INSERT INTO stock_data
        (name, timestamp, open_price, current_price, close_price, percentage_change)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    values = (
        data.get("name"),
        data.get("timestamp"),
        data.get("open"),
        data.get("current"),
        data.get("close"),
        data.get("percentage_change", 0)
    )

    cur = conn.cursor()
    cur.execute(query, values)
    conn.commit()
    cur.close()


def insert_market_data(conn, data):
    query = """
        INSERT INTO market_data
        (market_name, timestamp, open_price, current_price, close_price, percentage_change)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    values = (
        data.get("name"),
        data.get("timestamp"),
        data.get("open"),
        data.get("current"),
        data.get("close"),
        data.get("percentage_change", 0)
    )

    cur = conn.cursor()
    cur.execute(query, values)
    conn.commit()
    cur.close()


def main():
    print("=" * 90)
    print("STOCK MARKET KAFKA -> MYSQL CONSUMER")
    print("=" * 90)
    print(f"Kafka Server : {KAFKA_SERVER}")
    print(f"Stock Topic  : {STOCK_TOPIC}")
    print(f"Market Topic : {MARKET_TOPIC}")
    print(f"MySQL DB     : {MYSQL_DATABASE}")
    print("=" * 90)

    conn = create_connection()
    if conn is None:
        return

    consumer = None

    try:
        create_tables(conn)

        consumer = KafkaConsumer(
            STOCK_TOPIC,
            MARKET_TOPIC,
            bootstrap_servers=[KAFKA_SERVER],
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )

        print("\nConsumer started. Waiting for Kafka messages...\n")
        count = 0

        for msg in consumer:
            data = msg.value

            if msg.topic == STOCK_TOPIC:
                insert_stock_data(conn, data)
                label = "STOCK"
            elif msg.topic == MARKET_TOPIC:
                insert_market_data(conn, data)
                label = "MARKET"
            else:
                continue

            count += 1
            print(
                f"[{label} -> MYSQL] "
                f"Name: {data.get('name')} | "
                f"Timestamp: {data.get('timestamp')} | "
                f"Open: {data.get('open')} | "
                f"Current: {data.get('current')} | "
                f"Close: {data.get('close')} | "
                f"Change: {data.get('percentage_change')}%"
            )
            print(f"Message #{count} saved to MySQL.\n")

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
    except Exception as e:
        print(f"Consumer error: {e}")
    finally:
        if consumer:
            consumer.close()
        if conn:
            conn.close()
        print("Kafka and MySQL connections closed.")


if __name__ == "__main__":
    main()