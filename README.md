# Real-Time Stock Market Analytics using Kafka, MySQL & Grafana

## Project Overview

This project implements a real-time stock market analytics pipeline
using **Yahoo Finance (`yfinance`)**, **Apache Kafka**, **Python**,
**MySQL**, and **Grafana**.

The system captures stock and market index data, streams it through
Kafka, consumes the messages using Python, stores the data in MySQL, and
presents the results through an interactive Grafana dashboard.

### Project Flow

``` text
Yahoo Finance / yfinance
          │
          ▼
   Python Producers
     │          │
     ▼          ▼
stock-topic  market-topic
     │          │
     └──────┬───┘
            ▼
      Python Consumer
            │
            ▼
          MySQL
     ┌──────┴──────┐
     ▼             ▼
stock_data    market_data
     │             │
     └──────┬──────┘
            ▼
         Grafana
```

------------------------------------------------------------------------

## Objectives

-   Stream stock market data in real time.
-   Publish stock and market data to separate Kafka topics.
-   Consume Kafka messages using Python.
-   Store streaming data in MySQL.
-   Build an interactive Grafana dashboard.
-   Filter stock and market data using Grafana variables.
-   Analyse current price, opening price, closing price and percentage
    change.

------------------------------------------------------------------------

## Technologies Used

  Technology     Purpose
  -------------- -----------------------------------
  Python         Producers and Kafka consumer
  yfinance       Stock and market data source
  Apache Kafka   Real-time message streaming
  MySQL          Persistent data storage
  Grafana        Dashboard and visualization
  Docker         Kafka infrastructure support
  SQL            Data analysis and Grafana queries

------------------------------------------------------------------------

## Data Sources

The project uses Yahoo Finance through the `yfinance` Python library.

### Stock Data

The following stocks are included:

-   TCS --- `TCS.NS`
-   RELIANCE --- `RELIANCE.NS`
-   INFOSYS --- `INFY.NS`
-   HDFCBANK --- `HDFCBANK.NS`
-   ICICIBANK --- `ICICIBANK.NS`

### Market Data

The following market indices are included:

-   NIFTY 50 --- `^NSEI`
-   SENSEX --- `^BSESN`

------------------------------------------------------------------------

## Kafka Topics

Two Kafka topics are used.

### `stock-topic`

Contains stock-level records.

Example:

``` json
{
  "name": "INFOSYS",
  "timestamp": "2026-09-30 12:03:00",
  "open": 1010.30,
  "current": 1010.50,
  "close": 1010.50,
  "percentage_change": 0.00
}
```

### `market-topic`

Contains market/index records.

Example:

``` json
{
  "name": "SENSEX",
  "timestamp": "2026-09-30 11:48:00",
  "open": 72761.32,
  "current": 72761.38,
  "close": 72529.07,
  "percentage_change": 0.32
}
```

------------------------------------------------------------------------

# Project Files

A typical project structure is:

``` text
stock-market-streaming/
│
├── stock_producer.py
├── market_producer.py
├── consumer_sql_updated.py
├── generate_surge_data.py
├── README.md
│
└── grafana/
    └── dashboard.json
```

The core files for this stock market project are:

### `stock_producer.py`

Generates stock records using `yfinance` and publishes them to:

``` text
stock-topic
```

### `market_producer.py`

Generates NIFTY 50 and SENSEX records using `yfinance` and publishes
them to:

``` text
market-topic
```

### `consumer_sql_updated.py`

Consumes both Kafka topics and stores:

``` text
stock-topic  → stock_data
market-topic → market_data
```

in MySQL.

------------------------------------------------------------------------

# Prerequisites

Install the following:

-   Python 3.10+
-   Apache Kafka
-   MySQL
-   Grafana
-   Docker Desktop (if Kafka is running through Docker)

------------------------------------------------------------------------

# Python Libraries

Install the required packages:

``` bash
pip install kafka-python yfinance mysql-connector-python pandas
```

------------------------------------------------------------------------

# MySQL Configuration

The consumer uses the following database configuration:

``` text
Host     : localhost
Port     : 3306
User     : root
Database : stock_market_db
```

Update the password in the Python consumer according to your local MySQL
configuration.

> **Security recommendation:** Do not commit real database passwords to
> GitHub. For a public repository, use environment variables instead.

------------------------------------------------------------------------

# Database Structure

The consumer automatically creates:

``` text
stock_market_db
```

with two tables.

## `stock_data`

  Column              Description
  ------------------- -----------------------------
  id                  Auto-increment record ID
  name                Stock name
  timestamp           Market data timestamp
  open_price          Opening price
  current_price       Current price
  close_price         Closing/reference price
  percentage_change   Percentage change
  received_at         MySQL record insertion time

## `market_data`

  Column              Description
  ------------------- -----------------------------
  id                  Auto-increment record ID
  market_name         Market/index name
  timestamp           Market data timestamp
  open_price          Opening value
  current_price       Current value
  close_price         Closing/reference value
  percentage_change   Percentage change
  received_at         MySQL record insertion time

------------------------------------------------------------------------

# Running the Project

## Step 1 --- Start Kafka Infrastructure

If Kafka is configured through Docker Compose:

``` bash
docker compose up -d zookeeper kafka
```

Check containers:

``` bash
docker ps
```

------------------------------------------------------------------------

## Step 2 --- Start MySQL

Make sure MySQL is running on:

``` text
localhost:3306
```

The database and tables are created automatically by the consumer.

------------------------------------------------------------------------

## Step 3 --- Start the Stock Producer

Open a terminal:

``` bash
python stock_producer.py
```

The producer publishes stock records to:

``` text
stock-topic
```

Expected output:

``` text
[STOCK] Name: INFOSYS | Timestamp: ... | Open: ... | Current: ... | Close: ... | Percentage Change: ...%
```

------------------------------------------------------------------------

## Step 4 --- Start the Market Producer

Open another terminal:

``` bash
python market_producer.py
```

The producer publishes NIFTY 50 and SENSEX records to:

``` text
market-topic
```

Expected output:

``` text
[MARKET] Name: SENSEX | Timestamp: ... | Open: ... | Current: ... | Close: ... | Percentage Change: ...%
```

------------------------------------------------------------------------

## Step 5 --- Start the Consumer

Open another terminal:

``` bash
python consumer_sql_updated.py
```

Expected output:

``` text
MySQL connected: stock_market_db
Tables ready: stock_data, market_data

Consumer started. Waiting for Kafka messages...
```

When messages arrive:

``` text
[STOCK -> MYSQL] Name: INFOSYS | ...
Message #1 saved to MySQL.

[MARKET -> MYSQL] Name: SENSEX | ...
Message #2 saved to MySQL.
```

------------------------------------------------------------------------

# Verify Data in MySQL

Open MySQL Workbench.

``` sql
USE stock_market_db;
```

Check stock data:

``` sql
SELECT * FROM stock_data;
```

Check market data:

``` sql
SELECT * FROM market_data;
```

Count records:

``` sql
SELECT COUNT(*) FROM stock_data;
```

``` sql
SELECT COUNT(*) FROM market_data;
```

------------------------------------------------------------------------

# Grafana Configuration

Connect Grafana to the MySQL database.

### MySQL Data Source

Use:

``` text
Host: localhost:3306
Database: stock_market_db
User: root
Password: <your-password>
```

------------------------------------------------------------------------

# Grafana Dashboard Filters

Two dashboard variables are used.

## Stock Filter

Variable name:

``` text
name
```

Variable query:

``` sql
SELECT DISTINCT name
FROM stock_data
ORDER BY name;
```

Example values:

``` text
TCS
RELIANCE
INFOSYS
HDFCBANK
ICICIBANK
```

## Market Filter

Variable name:

``` text
market_name
```

Variable query:

``` sql
SELECT DISTINCT market_name
FROM market_data
ORDER BY market_name;
```

Example values:

``` text
NIFTY 50
SENSEX
```

------------------------------------------------------------------------

# Grafana Queries

## Selected Stock Price

``` sql
SELECT
    name,
    current_price
FROM stock_data
WHERE name IN (${name:sqlstring});
```

## Selected Stock Percentage Change

``` sql
SELECT
    name,
    percentage_change
FROM stock_data
WHERE name IN (${name:sqlstring});
```

## Stock Open vs Current

``` sql
SELECT
    name,
    open_price,
    current_price
FROM stock_data
WHERE name IN (${name:sqlstring});
```

## Stock Current vs Close

``` sql
SELECT
    name,
    current_price,
    close_price
FROM stock_data
WHERE name IN (${name:sqlstring});
```

## Selected Market Price

``` sql
SELECT
    market_name,
    current_price
FROM market_data
WHERE market_name IN (${market_name:sqlstring});
```

## Selected Market Percentage Change

``` sql
SELECT
    market_name,
    percentage_change
FROM market_data
WHERE market_name IN (${market_name:sqlstring});
```

## Market Current vs Close

``` sql
SELECT
    market_name,
    current_price,
    close_price
FROM market_data
WHERE market_name IN (${market_name:sqlstring});
```

------------------------------------------------------------------------

# Dashboard Panels

The dashboard can contain:

  Panel                      Visualization
  -------------------------- ---------------
  Stock Price                Bar / Stat
  Stock Percentage Change    Gauge / Bar
  Stock Open vs Current      Bar
  Stock Current vs Close     Bar
  Market Current Price       Stat / Bar
  Market Percentage Change   Gauge / Bar
  Market Current vs Close    Bar
  Percentage Change          Line / Bar
  Market Price Difference    Table
  Stock Data                 Table

For queries that do not return a time field, use **Bar, Gauge, Stat or
Table** visualizations rather than Grafana Time series.

------------------------------------------------------------------------

# Sample Dashboard Output

The implemented dashboard contains filters for:

``` text
Stock: INFOSYS
Market: SENSEX
```

The dashboard provides:

-   Stock price
-   Percentage change
-   Open/current comparison
-   Market current price
-   Market percentage change
-   Market price difference
-   Percentage-change visualization

------------------------------------------------------------------------

# Data Validation

The MySQL output confirms that the streaming pipeline contains records
for:

### Stocks

``` text
TCS
RELIANCE
INFOSYS
HDFCBANK
ICICIBANK
```

### Markets

``` text
NIFTY 50
SENSEX
```

Example database queries:

``` sql
SELECT
    name,
    current_price,
    percentage_change
FROM stock_data;
```

``` sql
SELECT
    market_name,
    current_price,
    percentage_change
FROM market_data;
```

------------------------------------------------------------------------

# Key Insights

The dashboard enables:

1.  Monitoring stock prices for selected companies.
2.  Comparing current price with opening and closing values.
3.  Monitoring percentage movements.
4.  Comparing NIFTY 50 and SENSEX values.
5.  Filtering the dashboard by individual stock or market.
6.  Validating streamed data after it reaches the database.

------------------------------------------------------------------------

# Troubleshooting

## Kafka connection error

Check that Kafka is running:

``` bash
docker ps
```

Check the Kafka broker:

``` text
localhost:9092
```

## MySQL connection error

Verify:

``` text
MySQL host     = localhost
MySQL port     = 3306
MySQL username = root
```

and confirm that the password in the consumer matches the local MySQL
password.

## No data in Grafana

First verify MySQL:

``` sql
SELECT COUNT(*) FROM stock_data;
```

``` sql
SELECT COUNT(*) FROM market_data;
```

If the counts are zero, check the Kafka consumer and producers before
troubleshooting Grafana.

## Grafana says "Data is missing a time field"

If the query does not return a timestamp, do not use a Time series
visualization.

Use:

``` text
Bar chart
Gauge
Stat
Table
```

instead.

------------------------------------------------------------------------

# Learning Outcomes

This project demonstrates practical understanding of:

-   Real-time data streaming
-   Apache Kafka topics and consumers
-   Python-based data producers
-   JSON message processing
-   MySQL data persistence
-   SQL-based analytics
-   Grafana dashboard development
-   Dashboard variables and filtering
-   End-to-end streaming architecture

------------------------------------------------------------------------

# Future Enhancements

Possible extensions include:

-   Real-time alert generation for large price movements.
-   Integration with additional market indices.
-   Adding trading volume and market capitalization.
-   Using Kafka consumer groups for scalable processing.
-   Adding anomaly detection.
-   Creating real-time time-series panels using timestamp data.
-   Moving database credentials to environment variables.
-   Deploying the complete pipeline using Docker Compose.

------------------------------------------------------------------------

# Author

**Aman Nagar**

PGDM -- Big Data Analytics\
FORE School of Management

**Project:** Real-Time Stock Market Analytics using Apache Kafka, MySQL
and Grafana
