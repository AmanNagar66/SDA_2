import json
import time
import yfinance as yf
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC = "stock-topic"

STOCKS = {
    "TCS": "TCS.NS",
    "RELIANCE": "RELIANCE.NS",
    "INFOSYS": "INFY.NS",
    "HDFCBANK": "HDFCBANK.NS",
    "ICICIBANK": "ICICIBANK.NS"
}

RECORDS_PER_STOCK = 10
STREAM_DELAY_SECONDS = 1

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    key_serializer=lambda x: x.encode("utf-8"),
    retries=5,
    acks="all"
)

def clean_columns(data):
    if getattr(data.columns, "nlevels", 1) > 1:
        data.columns = data.columns.get_level_values(0)
    return data

def fetch_stock(symbol, ticker):
    print(f"Fetching {symbol} ({ticker})...")

    try:
        data = yf.download(
            ticker,
            period="1d",
            interval="1m",
            auto_adjust=False,
            progress=False,
            threads=False
        )

        if data.empty:
            print(f"WARNING: No data returned for {symbol}")
            return []

        data = clean_columns(data)
        data = data.dropna(subset=["Open", "High", "Low", "Close"])
        data = data.tail(RECORDS_PER_STOCK)

        records = []
        previous_close = None

        for timestamp, row in data.iterrows():
            current = float(row["Close"])

            if previous_close is None or previous_close == 0:
                pct = 0.0
            else:
                pct = ((current - previous_close) / previous_close) * 100

            records.append({
                "name": symbol,
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "open": round(float(row["Open"]), 2),
                "current": round(current, 2),
                "close": round(current, 2),
                "percentage_change": round(pct, 2)
            })

            previous_close = current

        return records

    except Exception as e:
        print(f"ERROR fetching {symbol}: {e}")
        return []

def main():
    print("=" * 100)
    print("STOCK DATA KAFKA PRODUCER")
    print("=" * 100)

    stock_data = {}

    for symbol, ticker in STOCKS.items():
        stock_data[symbol] = fetch_stock(symbol, ticker)
        print(f"{symbol}: {len(stock_data[symbol])} records loaded.")

    available = [s for s in STOCKS if stock_data[s]]

    print("\nAvailable stocks:", ", ".join(available))
    print("\nStarting round-robin Kafka streaming...\n")

    for row_number in range(RECORDS_PER_STOCK):
        for symbol in available:
            records = stock_data[symbol]

            if row_number >= len(records):
                continue

            message = records[row_number]

            producer.send(TOPIC, key=symbol, value=message)
            producer.flush()

            print(
                f"[STOCK] Name: {message['name']} | "
                f"Timestamp: {message['timestamp']} | "
                f"Open: {message['open']} | "
                f"Current: {message['current']} | "
                f"Close: {message['close']} | "
                f"Percentage Change: {message['percentage_change']}%"
            )

            time.sleep(STREAM_DELAY_SECONDS)

    producer.flush()
    producer.close()
    print("\nAll available stocks streamed successfully.")

if __name__ == "__main__":
    main()
