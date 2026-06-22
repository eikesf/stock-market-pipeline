-- ============================================================================
-- ClickHouse Migration: Issue #17 - SCD Type 2 for Company Metadata (dim_companies)
-- ============================================================================
--
-- PURPOSE:
-- This script migrates the existing `stock_market.dim_companies` table in ClickHouse
-- to add Slowly Changing Dimension (SCD) Type 2 tracking columns:
--   - `start_date` (Date32)
--   - `end_date` (Nullable(Date32))
--   - `is_active` (UInt8)
--
-- It also updates the sorting key from `ORDER BY ticker` to `ORDER BY (ticker, start_date)`.
-- Since ClickHouse does not allow modifying the sorting key to use columns that were
-- added after initial table creation, we use an atomic table swap pattern.
--
-- EXECUTION:
-- Run this script using clickhouse-client or via a database console:
--   clickhouse-client --queries-file migration_v2_scd.sql
--
-- ============================================================================

-- 1. Create a staging table with the new schema and updated ORDER BY clause
CREATE TABLE IF NOT EXISTS stock_market.dim_companies_new
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
    ingestion_timestamp DateTime                      COMMENT 'Timestamp of ingestion into the Bronze layer',
    start_date          Date32                        COMMENT 'Date when the record becomes active',
    end_date            Nullable(Date32)              COMMENT 'Date when the record becomes inactive',
    is_active           UInt8                         COMMENT 'Flag indicating whether the record is currently active (1 = active, 0 = inactive)'
)
ENGINE = MergeTree()
ORDER BY (ticker, start_date);

-- 2. Copy the existing data to the new table, populating the new SCD columns with default/initial values
INSERT INTO stock_market.dim_companies_new
SELECT
    ticker,
    short_name,
    sector,
    industry,
    country,
    isin,
    full_time_employees,
    exchange,
    market_cap,
    currency,
    dividend_yield,
    extraction_date,
    ingestion_timestamp,
    toDate32(extraction_date) AS start_date,
    CAST(NULL, 'Nullable(Date32)') AS end_date,
    1 AS is_active
FROM stock_market.dim_companies;

-- 3. Atomically swap the new and old tables
RENAME TABLE stock_market.dim_companies TO stock_market.dim_companies_old,
             stock_market.dim_companies_new TO stock_market.dim_companies;

-- 4. Clean up the old table
DROP TABLE IF EXISTS stock_market.dim_companies_old;

-- 5. Create the active companies view (Current Snapshot)
CREATE VIEW IF NOT EXISTS stock_market.v_companies_active AS
SELECT *
FROM stock_market.dim_companies
WHERE is_active = 1;
