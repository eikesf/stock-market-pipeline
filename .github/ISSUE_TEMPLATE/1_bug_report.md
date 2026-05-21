---
name: Bug Report
about: Report a bug in the data pipeline, transformation steps, or database queries.
title: '[BUG] '
labels: 'bug, triaged'
assignees: ''
---

## Description
A clear and concise description of the bug.

## Pipeline Stage / Component
Identify where the failure occurs:
- [ ] **Landing Zone** (yfinance extraction)
- [ ] **Bronze Layer** (Delta Lake ingestion)
- [ ] **Silver Layer** (Spark deduplication & cleaning)
- [ ] **Gold Layer** (ClickHouse load/star schema DDL)
- [ ] **Orchestration** (Airflow DAGs)
- [ ] **CI/CD** (GitHub Actions / Linting)
- [ ] **Infrastructure / Local Dev** (Docker, Docker Compose, Makefile)
- [ ] **Other** (please specify)

## Steps to Reproduce
Steps to reproduce the behavior:
1. Run command: `make run_...` or specify execution command.
2. Provide details about the state (e.g., "First run on empty ClickHouse database", "Resetting data folder").
3. See error.

## Expected Behavior
A clear and concise description of what you expected to happen.

## Actual Behavior / Logs / Stack Traces
Please paste the full console output, PySpark exceptions, or ClickHouse query error messages:
```text
(Paste logs here)
```

## Environment & Context
- **OS:** (e.g., macOS Sequoia, Ubuntu 22.04)
- **Docker / Docker Compose Version:** (e.g., Docker v25.0.3, Compose v2.24.6)
- **Any modifications to `.env` or Tickers?** (e.g., Custom tickers in `tickers.py`, custom DB credentials)

## Additional Context
Add any other context about the problem here (e.g., did it fail during a weekend/holiday when the market was closed?).

## Possible Solution / Workaround
If you have a suggestion on how to fix the issue or a temporary workaround, please describe it here (e.g., code snippet, required dependency, configuration tweak).

