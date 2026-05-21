---
name: Backfill & Migration
about: Plan for historical data backfills, schema upgrades, or data migrations.
title: '[MIGRATION/BACKFILL] '
labels: 'migration, backfill'
assignees: ''
---

## Description
A clear and concise description of the backfill or migration task (e.g., "Backfill B3 and NASDAQ daily price data for the entire calendar year of 2025", "Migrate fact_prices table in ClickHouse to add a new sector column").

## Scope & Boundaries
- **Exchanges/Tickers:** (e.g., All NASDAQ tickers, or B3 only)
- **Time Window / Date Range:** Start Date (YYYY-MM-DD) to End Date (YYYY-MM-DD)
- **Data Volume Estimate:** (e.g., 5 years of daily data for ~100 tickers)

## Migration / Backfill Execution Strategy
How will the data be processed? 
* Will we use an isolated Python script or pass start/end parameters to `generator.py`?
* Do we need to drop/recreate ClickHouse tables or run a Delta Lake history restore?
* State if this can run concurrent with live daily pipeline executions.

## Action Plan / Tasks
- [ ] Step 1: Draft extraction scripts / SQL migration DDL.
- [ ] Step 2: Test migration on local staging container.
- [ ] Step 3: Run full backfill execution.
- [ ] Step 4: Verify counts and deduplicate results.

## Risk Assessment & Rollback Plan
- **Risk:** What happens if the run fails mid-process? (e.g., partial database loading, API rate limit locks)
- **Rollback:** How do we revert to a clean state? (e.g., restore Delta Lake time travel snapshot, truncate table, rerun deduplication)
