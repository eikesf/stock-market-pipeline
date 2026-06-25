-- 5. USD/BRL Exchange Rate view
DROP VIEW IF EXISTS stock_market.v_fact_prices_converted;
CREATE OR REPLACE VIEW stock_market.v_fact_prices_converted AS
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