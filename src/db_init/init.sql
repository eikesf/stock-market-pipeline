-- ============================================================
-- ClickHouse DDL - Gold Layer (OLAP)
-- Engine: MergeTree
--
-- Deduplication is handled in the Silver layer (PySpark
-- window functions)
-- ============================================================

-- Database creation
CREATE DATABASE IF NOT EXISTS stock_market;

-- 1. Dimension Table (Company Metadata)
CREATE TABLE IF NOT EXISTS stock_market.dim_companies
(
    ticker              LowCardinality(String)        COMMENT 'Stock ticker symbol (e.g., AAPL, GOOGL)',
    short_name          String                        COMMENT 'Company display name',
    sector              LowCardinality(String)        COMMENT 'GICS sector classification',
    industry            LowCardinality(String)        COMMENT 'Industry classification',
    country             LowCardinality(String)        COMMENT 'Country where the company is based',
    isin                String                        COMMENT 'International Securities Identification Number',
    full_time_employees UInt32                        COMMENT 'Number of full-time employees',
    exchange            LowCardinality(String)        COMMENT 'Exchange where the ticker is listed (NASDAQ, NYSE, B3)',
    market_cap          UInt64                        COMMENT 'Market capitalization in USD',
    currency            LowCardinality(String)        COMMENT 'Currency in which the stock is traded (e.g., USD, BRL)',
    dividend_yield      Decimal(10,2)                 COMMENT 'Annual dividend yield as a percentage',
    extraction_date     Date                          COMMENT 'Date when the metadata was extracted from yfinance',
    ingestion_timestamp DateTime                      COMMENT 'Timestamp of ingestion into the Bronze layer'
)
ENGINE = MergeTree()
ORDER BY ticker;

-- 2. Fact Table (Daily Prices)
CREATE TABLE IF NOT EXISTS stock_market.fact_prices
(
    date                Date                           COMMENT 'Trading date',
    ticker              LowCardinality(String)         COMMENT 'Stock ticker symbol (e.g. AAPL, GOOGL)',
    open                Decimal(10, 2)                 COMMENT 'Opening price for the trading day',
    high                Decimal(10, 2)                 COMMENT 'Highest price during the trading day',
    low                 Decimal(10, 2)                 COMMENT 'Lowest price during the trading day',
    close               Decimal(10, 2)                 COMMENT 'Closing price for the trading day',
    adj_close           Decimal(10, 2)                 COMMENT 'Adjusted closing price for the trading day',
    volume              UInt64                         COMMENT 'Total number of shares traded during the day',
    dividends           Decimal(10, 2)                 COMMENT 'Total dividends paid during the day',
    stock_splits        Decimal(10, 4)                 COMMENT 'Stock split ratio for the trading day',
    ingestion_timestamp DateTime                       COMMENT 'Timestamp of ingestion into the Bronze layer'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (ticker, date);