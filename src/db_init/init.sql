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
    full_time_employees Nullable(UInt32)              COMMENT 'Number of full-time employees',
    exchange            LowCardinality(String)        COMMENT 'Exchange where the ticker is listed (NASDAQ, NYSE, B3)',
    currency            LowCardinality(String)        COMMENT 'Currency in which the stock is traded (e.g., USD, BRL)',
    extraction_date     Date                          COMMENT 'Date when the metadata was extracted from yfinance',
    ingestion_timestamp DateTime                      COMMENT 'Timestamp of ingestion into the Bronze layer',
    start_date          Date32                        COMMENT 'Date when the record becomes active',
    end_date            Nullable(Date32)              COMMENT 'Date when the record becomes inactive',
    is_active           UInt8                         COMMENT 'Flag indicating whether the record is currently active (1 = active, 0 = inactive)'
)
ENGINE = MergeTree()
ORDER BY (ticker, start_date);

-- 1.1 Active Companies View (Current Snapshot)
CREATE VIEW IF NOT EXISTS stock_market.v_companies_active AS
SELECT *
FROM stock_market.dim_companies
WHERE is_active = 1;

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

-- 3. Fact Table (Company investment metrics)
CREATE TABLE IF NOT EXISTS stock_market.fact_company_metrics
(
    extraction_date                Date                           COMMENT 'Date when the metrics were extracted from yfinance',
    ticker                         LowCardinality(String)         COMMENT 'Stock ticker symbol (e.g. AAPL, GOOGL)',
    dividend_yield                 Nullable(Decimal(10,4))        COMMENT 'Annual dividend yield as a percentage',
    trailing_pe                    Nullable(Decimal(10,2))        COMMENT 'Trailing price-to-earnings ratio',
    peg_ratio                      Nullable(Decimal(10,4))        COMMENT 'PEG Ratio',
    price_to_book                  Nullable(Decimal(10,4))        COMMENT 'Price to Book Value (P/VP)',
    enterprise_to_ebitda           Nullable(Decimal(10,4))        COMMENT 'EV/EBITDA',
    enterprise_to_ebit             Nullable(Decimal(10,4))        COMMENT 'EV/EBIT',
    book_value                     Nullable(Decimal(10,4))        COMMENT 'Book Value Per Share (VPA)',
    trailing_eps                   Nullable(Decimal(10,4))        COMMENT 'Earnings Per Share (LPA)',
    price_to_sales                 Nullable(Decimal(10,4))        COMMENT 'Price to Sales Ratio (P/SR)',
    operating_margins              Nullable(Decimal(10,4))        COMMENT 'Operating margin (EBIT margin)',
    asset_turnover                 Nullable(Decimal(10,4))        COMMENT 'Asset turnover ratio',
    shares_outstanding             Nullable(UInt64)               COMMENT 'Total number of shares outstanding',
    market_cap                     Nullable(UInt64)               COMMENT 'Market capitalization in original currency',
    ebitda                         Nullable(Int64)                COMMENT 'Earnings before interest, taxes, depreciation, and amortization',
    total_debt                     Nullable(UInt64)               COMMENT 'Total debt',
    total_cash                     Nullable(UInt64)               COMMENT 'Total cash',
    debt_to_equity                 Nullable(Decimal(10,4))        COMMENT 'Debt-to-equity ratio',
    roa                            Nullable(Decimal(10,4))        COMMENT 'Return on assets',
    roe                            Nullable(Decimal(10,4))        COMMENT 'Return on equity',
    current_ratio                  Nullable(Decimal(10,4))        COMMENT 'Current ratio',
    gross_margins                  Nullable(Decimal(10,4))        COMMENT 'Gross margins',
    ebitda_margins                 Nullable(Decimal(10,4))        COMMENT 'EBITDA margins',
    profit_margins                 Nullable(Decimal(10,4))        COMMENT 'Profit margins',
    net_income_to_common           Nullable(Int64)                COMMENT 'Net income to common shareholders',
    ingestion_timestamp            DateTime                       COMMENT 'Timestamp of ingestion into the Bronze layer'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(extraction_date)
ORDER BY (ticker, extraction_date);

-- 4. Analytical View (Calculated Investment Ratios)
CREATE VIEW IF NOT EXISTS stock_market.v_companies_performance AS
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
    CAST(m.extraction_date, 'Nullable(Date)') AS extraction_date,
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
    if(coalesce(m.ebitda, 0) != 0, m.market_cap / m.ebitda, NULL) AS p_ebitda,
    if(coalesce(m.operating_margins, 0) != 0, CAST(CAST(m.price_to_sales, 'Nullable(Float64)') / CAST(m.operating_margins, 'Nullable(Float64)'), 'Nullable(Decimal(18, 4))'), NULL) AS p_ebit,
    m.book_value AS book_value,
    m.trailing_eps AS trailing_eps,
    m.price_to_sales AS price_to_sales,
    
    -- Debt Indicators
    if(coalesce(m.shares_outstanding * m.book_value, 0) != 0, CAST((CAST(m.total_debt, 'Nullable(Float64)') - CAST(m.total_cash, 'Nullable(Float64)')) / (CAST(m.shares_outstanding, 'Nullable(Float64)') * CAST(m.book_value, 'Nullable(Float64)')), 'Nullable(Decimal(10, 4))'), NULL) AS net_debt_equity,
    if(coalesce(m.ebitda, 0) != 0, (CAST(m.total_debt, 'Int64') - CAST(m.total_cash, 'Int64')) / m.ebitda, NULL) AS net_debt_ebitda,
    if(coalesce(m.operating_margins, 0) != 0 AND coalesce(m.market_cap, 0) != 0 AND coalesce(m.price_to_sales, 0) != 0, CAST((CAST(m.total_debt, 'Nullable(Float64)') - CAST(m.total_cash, 'Nullable(Float64)')) / ((CAST(m.market_cap, 'Nullable(Float64)') / CAST(m.price_to_sales, 'Nullable(Float64)')) * CAST(m.operating_margins, 'Nullable(Float64)')), 'Nullable(Decimal(18, 8))'), NULL) AS net_debt_ebit,
    if(coalesce(m.roe, 0) != 0, CAST(CAST(m.roa, 'Nullable(Float64)') / CAST(m.roe, 'Nullable(Float64)'), 'Nullable(Decimal(18, 4))'), NULL) AS equity_assets,
    if(coalesce(m.roe, 0) != 0, CAST(1.0 - (CAST(m.roa, 'Nullable(Float64)') / CAST(m.roe, 'Nullable(Float64)')), 'Nullable(Decimal(18, 4))'), NULL) AS liabilities_assets,
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
INNER JOIN latest_metrics m ON c.ticker = m.ticker;

-- 5. USD/BRL Exchange Rate view
CREATE VIEW IF NOT EXISTS stock_market.v_fact_prices_converted AS
WITH exchange_rates AS (
    SELECT
        date,
        close AS usd_brl_rate,
        1 AS dummy
    FROM 
        stock_market.fact_prices
    WHERE
        ticker = 'USDBRL'
)
SELECT
    p.ticker AS ticker,
    dc.currency AS currency,
    p.date AS date,
    p.volume AS volume,
    if(er.usd_brl_rate > 0, er.usd_brl_rate, NULL) AS usd_brl,

    -- If stock is in BRL, keep original values, else convert to BRL
    if(dc.currency = 'BRL', toDecimal64(p.open, 4), toDecimal64(p.open * usd_brl, 4)) AS open_brl,
    if(dc.currency = 'BRL', toDecimal64(p.high, 4), toDecimal64(p.high * usd_brl, 4)) AS high_brl,
    if(dc.currency = 'BRL', toDecimal64(p.low, 4), toDecimal64(p.low * usd_brl, 4)) AS low_brl,
    if(dc.currency = 'BRL', toDecimal64(p.close, 4), toDecimal64(p.close * usd_brl, 4)) AS close_brl,
    if(dc.currency = 'BRL', toDecimal64(p.adj_close, 4), toDecimal64(p.adj_close * usd_brl, 4)) AS adj_close_brl,
    
    -- If stock is in USD, keep original values, else convert to USD
    if(dc.currency = 'USD', toDecimal64(p.open, 4), if(usd_brl > 0, toDecimal64(p.open / usd_brl, 4), NULL)) AS open_usd,
    if(dc.currency = 'USD', toDecimal64(p.high, 4), if(usd_brl > 0, toDecimal64(p.high / usd_brl, 4), NULL)) AS high_usd,
    if(dc.currency = 'USD', toDecimal64(p.low, 4), if(usd_brl > 0, toDecimal64(p.low / usd_brl, 4), NULL)) AS low_usd,
    if(dc.currency = 'USD', toDecimal64(p.close, 4), if(usd_brl > 0, toDecimal64(p.close / usd_brl, 4), NULL)) AS close_usd,
    if(dc.currency = 'USD', toDecimal64(p.adj_close, 4), if(usd_brl > 0, toDecimal64(p.adj_close / usd_brl, 4), NULL)) AS adj_close_usd
FROM
    (
        SELECT *, 1 AS dummy
        FROM stock_market.fact_prices
    ) p
INNER JOIN
    stock_market.v_companies_active dc ON p.ticker = dc.ticker
ASOF LEFT JOIN
    exchange_rates er ON p.dummy = er.dummy AND p.date >= er.date
WHERE
    p.ticker != 'USDBRL';