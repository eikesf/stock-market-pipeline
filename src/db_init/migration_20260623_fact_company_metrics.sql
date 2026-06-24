-- ============================================================================
-- ClickHouse Migration: Issue #30 - Company Investment Metrics & Separated Table
-- ============================================================================
--
-- PURPOSE:
-- 1. Remove volatile metrics (market_cap, dividend_yield) from `dim_companies` to prevent SCD Type 2 bloat.
-- 2. Recreate `v_companies_active` to reflect the updated schema of `dim_companies`.
-- 3. Create the new `fact_company_metrics` table to hold historical financial snapshot metrics.
-- 4. Create the new `v_companies_performance` view to calculate financial ratios.
--
-- EXECUTION:
-- clickhouse-client --queries-file migration_20260623_fact_company_metrics.sql
-- ============================================================================

-- 1. Remove volatile metrics from dim_companies
ALTER TABLE stock_market.dim_companies DROP COLUMN IF EXISTS market_cap;
ALTER TABLE stock_market.dim_companies DROP COLUMN IF EXISTS dividend_yield;
-- 2. Deduplicate dim_companies records (merge identical consecutive versions)
-- Now that market_cap and dividend_yield are removed, we have redundant history rows.
-- We use a staging table to rebuild dim_companies with grouped/merged periods.
DROP TABLE IF EXISTS stock_market.dim_companies_dedup;
CREATE TABLE stock_market.dim_companies_dedup AS stock_market.dim_companies;

INSERT INTO stock_market.dim_companies_dedup
SELECT
    ticker,
    short_name,
    sector,
    industry,
    country,
    isin,
    full_time_employees,
    exchange,
    currency,
    extraction_date,
    ingestion_timestamp,
    start_date,
    if(is_active = 1, CAST(NULL, 'Nullable(Date32)'), end_date) AS end_date,
    is_active
FROM (
    SELECT
        ticker,
        short_name,
        sector,
        industry,
        country,
        isin,
        full_time_employees,
        exchange,
        currency,
        min(extraction_date) AS extraction_date,
        max(ingestion_timestamp) AS ingestion_timestamp,
        min(start_date) AS start_date,
        max(end_date) AS end_date,
        max(is_active) AS is_active
    FROM stock_market.dim_companies
    GROUP BY
        ticker,
        short_name,
        sector,
        industry,
        country,
        isin,
        full_time_employees,
        exchange,
        currency
);

RENAME TABLE stock_market.dim_companies TO stock_market.dim_companies_old,
             stock_market.dim_companies_dedup TO stock_market.dim_companies;

DROP TABLE IF EXISTS stock_market.dim_companies_old;

-- 2. Recreate the active companies view to refresh its schema
DROP VIEW IF EXISTS stock_market.v_companies_active;
CREATE VIEW stock_market.v_companies_active AS
SELECT *
FROM stock_market.dim_companies
WHERE is_active = 1;

-- 3. Create the new fact table for investment metrics
CREATE TABLE IF NOT EXISTS stock_market.fact_company_metrics
(
    extraction_date                Date                           COMMENT 'Date when the metrics were extracted from yfinance',
    ticker                         LowCardinality(String)         COMMENT 'Stock ticker symbol (e.g. AAPL, GOOGL)',
    dividend_yield                 Decimal(10,4)                  COMMENT 'Annual dividend yield as a percentage (fraction)',
    trailing_pe                    Decimal(10,2)                  COMMENT 'Trailing price-to-earnings ratio',
    peg_ratio                      Decimal(10,4)                  COMMENT 'PEG Ratio',
    price_to_book                  Decimal(10,4)                  COMMENT 'Price to Book Value (P/VP)',
    enterprise_to_ebitda           Decimal(10,4)                  COMMENT 'EV/EBITDA',
    enterprise_to_ebit             Decimal(10,4)                  COMMENT 'EV/EBIT',
    book_value                     Decimal(10,4)                  COMMENT 'Book Value Per Share (VPA)',
    trailing_eps                   Decimal(10,4)                  COMMENT 'Earnings Per Share (LPA)',
    price_to_sales                 Decimal(10,4)                  COMMENT 'Price to Sales Ratio (P/SR)',
    operating_margins              Decimal(10,4)                  COMMENT 'Operating margin (EBIT margin)',
    asset_turnover                 Decimal(10,4)                  COMMENT 'Asset turnover ratio',
    shares_outstanding             UInt64                         COMMENT 'Total number of shares outstanding',
    market_cap                     UInt64                         COMMENT 'Market capitalization in original currency',
    ebitda                         Int64                          COMMENT 'Earnings before interest, taxes, depreciation, and amortization',
    total_debt                     UInt64                         COMMENT 'Total debt',
    total_cash                     UInt64                         COMMENT 'Total cash',
    debt_to_equity                 Decimal(10,4)                  COMMENT 'Debt-to-equity ratio',
    roa                            Decimal(10,4)                  COMMENT 'Return on assets',
    roe                            Decimal(10,4)                  COMMENT 'Return on equity',
    current_ratio                  Decimal(10,4)                  COMMENT 'Current ratio',
    gross_margins                  Decimal(10,4)                  COMMENT 'Gross margins',
    ebitda_margins                 Decimal(10,4)                  COMMENT 'EBITDA margins',
    profit_margins                 Decimal(10,4)                  COMMENT 'Profit margins',
    net_income_to_common           Int64                          COMMENT 'Net income to common shareholders',
    ingestion_timestamp            DateTime                       COMMENT 'Timestamp of ingestion into the Bronze layer'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(extraction_date)
ORDER BY (ticker, extraction_date);

-- 4. Create the performance view calculating financial ratios
DROP VIEW IF EXISTS stock_market.v_companies_performance;
CREATE VIEW stock_market.v_companies_performance AS
WITH latest_metrics AS (
    -- Get only the most recent fundamentals snapshot for each company
    SELECT *
    FROM stock_market.fact_company_metrics
    LIMIT 1 BY ticker
)
SELECT
    c.ticker AS ticker,
    c.short_name AS short_name,
    c.sector AS sector,
    c.industry AS industry,
    c.country AS country,
    c.exchange AS exchange,
    c.currency AS currency,
    m.extraction_date AS extraction_date,
    m.market_cap AS market_cap,
    m.ebitda AS ebitda,
    m.total_debt AS total_debt,
    m.total_cash AS total_cash,
    m.net_income_to_common AS net_income_to_common,
    
    -- Valuation Indicators
    m.dividend_yield AS dividend_yield,
    m.trailing_pe AS trailing_pe,
    m.peg_ratio AS peg_ratio,
    m.price_to_book AS price_to_book,
    m.enterprise_to_ebitda AS enterprise_to_ebitda,
    m.enterprise_to_ebit AS enterprise_to_ebit,
    if(m.ebitda != 0, m.market_cap / m.ebitda, NULL) AS p_ebitda,
    if(m.operating_margins != 0, m.price_to_sales / m.operating_margins, NULL) AS p_ebit,
    m.book_value AS book_value,
    m.trailing_eps AS trailing_eps,
    m.price_to_sales AS price_to_sales,
    
    -- Debt Indicators
    if(m.shares_outstanding * m.book_value != 0, (CAST(m.total_debt, 'Int64') - CAST(m.total_cash, 'Int64')) / (m.shares_outstanding * m.book_value), NULL) AS net_debt_equity,
    if(m.ebitda != 0, (CAST(m.total_debt, 'Int64') - CAST(m.total_cash, 'Int64')) / m.ebitda, NULL) AS net_debt_ebitda,
    if(m.operating_margins != 0 AND m.market_cap != 0 AND m.price_to_sales != 0, (CAST(m.total_debt, 'Int64') - CAST(m.total_cash, 'Int64')) / ((m.market_cap / m.price_to_sales) * m.operating_margins), NULL) AS net_debt_ebit,
    if(m.roe != 0, m.roa / m.roe, NULL) AS equity_assets,
    if(m.roe != 0, 1 - (m.roa / m.roe), NULL) AS liabilities_assets,
    m.current_ratio AS current_ratio,
    m.debt_to_equity AS debt_to_equity,

    -- Efficiency Indicators
    m.gross_margins AS gross_margins,
    m.ebitda_margins AS ebitda_margins,
    m.operating_margins AS operating_margins,
    m.profit_margins AS profit_margins,
    
    -- Profitability Indicators
    m.roe AS roe,
    m.roa AS roa,
    m.asset_turnover AS asset_turnover
FROM stock_market.v_companies_active c
LEFT JOIN latest_metrics m ON c.ticker = m.ticker;
