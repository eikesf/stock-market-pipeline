-- ============================================================================
-- ClickHouse Migration: Fix stale "latest metrics" selection in
-- v_companies_performance (freshness check false failures)
-- ============================================================================
--
-- PURPOSE:
-- `latest_metrics` used `LIMIT 1 BY ticker` with no `ORDER BY` beforehand.
-- Without an ORDER BY, ClickHouse's `LIMIT n BY` returns an arbitrary row per
-- group (whatever order the MergeTree parts happen to be scanned in), not
-- necessarily the row with the most recent extraction_date. This let the view
-- silently serve a stale snapshot per ticker and made the Soda freshness check
-- ("most recent metric extraction is not older than 7 days") fail even though
-- fact_company_metrics itself had fresh data.
--
-- FIX: add `ORDER BY extraction_date DESC` before `LIMIT 1 BY ticker` so the
-- CTE deterministically keeps the newest snapshot per ticker.
--
-- EXECUTION:
-- clickhouse-client --queries-file migration_20260822_fix_v_companies_performance_freshness.sql
-- ============================================================================

DROP VIEW IF EXISTS stock_market.v_companies_performance;
CREATE OR REPLACE VIEW stock_market.v_companies_performance AS
WITH latest_metrics AS (
    -- Get only the most recent fundamentals snapshot for each company
    SELECT *
    FROM stock_market.fact_company_metrics
    ORDER BY extraction_date DESC
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
