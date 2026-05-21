---
name: Data Quality Incident
about: Report a data quality issue (e.g., null values, schema mismatch, duplicate rows, stale data).
title: '[DQ INCIDENT] '
labels: 'data-quality, incident'
assignees: ''
---

## Incident Summary
A clear, high-level summary of the data quality issue (e.g., "Sector field is NULL for all NASDAQ stocks since May 20th", "Duplicate price records in ClickHouse fact table").

## Impacted Layers / Tables
Identify where the bad data is present:
- [ ] **Landing Zone** (raw parquet files)
- [ ] **Bronze Layer** (raw Delta tables)
- [ ] **Silver Layer** (deduplicated Delta tables)
- [ ] **Gold Layer** (ClickHouse OLAP tables: `fact_prices`, `dim_companies`)

## Symptoms & Detection
- **Detection Date:** YYYY-MM-DD
- **How was it detected?** (e.g., SQL query, Spark warning, BI dashboard anomaly, manual audit)
- **Error symptoms / SQL queries showing the issue:**
  ```sql
  -- Example query demonstrating the issue
  SELECT count(*) FROM stock_market.dim_companies WHERE sector IS NULL;
  ```

## Expected Data Behavior
Describe what the data *should* look like (e.g., "Sector should be a non-empty string representing the GICS sector, such as 'Technology'").

## Root Cause Analysis (if known)
Why did this data quality issue occur? (e.g., yfinance returned incomplete payload, extraction script failed to handle NaN values, Spark deduplication logic skipped a key column).

## Cleanup / Recovery Action Plan
Steps required to fix the existing data:
- [ ] Step 1: Delete bad records or truncate table.
- [ ] Step 2: Rerun extraction / backfill.
- [ ] Step 3: Run sanity tests to confirm repair.
