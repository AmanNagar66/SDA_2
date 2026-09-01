import json
import time
import yfinance as yf
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC = "market-topic"

INDICES = {
    "NIFTY 50": "^NSEI",
    "SENSEX": "^BSESN"
}

RECORDS_PER_INDEX = 10
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

def fetch_index(name, ticker):
    print(f"Fetching {name} ({ticker})...")

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
            print(f"WARNING: No data returned for {name}")
            return []

        data = clean_columns(data)
        data = data.dropna(subset=["Open", "Close"])
        data = data.tail(RECORDS_PER_INDEX)

        # Previous trading-day close
        daily = yf.download(
            ticker,
            period="5d",
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False
        )
        daily = clean_columns(daily)
        closes = daily["Close"].dropna()

        if len(closes) >= 2:
            previous_close = float(closes.iloc[-2])
        elif len(closes) == 1:
            previous_close = float(closes.iloc[-1])
        else:
            return []

        # Opening value of today's market session
        day_open = float(data["Open"].iloc[0])

        records = []

        for timestamp, row in data.iterrows():
            current = float(row["Close"])
            pct = ((current - previous_close) / previous_close) * 100

            records.append({
                "name": name,
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "open": round(day_open, 2),
                "current": round(current, 2),
                "close": round(previous_close, 2),
                "percentage_change": round(pct, 2)
            })

        return records

    except Exception as e:
        print(f"ERROR fetching {name}: {e}")
        return []

def main():
    print("=" * 100)
    print("MARKET DATA KAFKA PRODUCER")
    print("=" * 100)

    market_data = {}

    for name, ticker in INDICES.items():
        market_data[name] = fetch_index(name, ticker)
        print(f"{name}: {len(market_data[name])} records loaded.")

    available = [name for name in INDICES if market_data[name]]

    print("\nAvailable indices:", ", ".join(available))
    print("\nStarting round-robin Kafka streaming...\n")

    for row_number in range(RECORDS_PER_INDEX):
        for name in available:
            records = market_data[name]

            if row_number >= len(records):
                continue

            message = records[row_number]

            producer.send(TOPIC, key=name, value=message)
            producer.flush()

            print(
                f"[MARKET] Name: {message['name']} | "
                f"Timestamp: {message['timestamp']} | "
                f"Open: {message['open']} | "
                f"Current: {message['current']} | "
                f"Close: {message['close']} | "
                f"Percentage Change: {message['percentage_change']}%"
            )

            time.sleep(STREAM_DELAY_SECONDS)

    producer.flush()
    producer.close()
    print("\nNIFTY 50 and SENSEX streamed successfully.")

if __name__ == "__main__":
    main()
