---
name: Feature Request
about: Suggest a new feature or pipeline enhancement for this project.
title: '[FEATURE] '
labels: 'enhancement, triaged'
assignees: ''
---

## Problem Statement
Is your feature request related to a problem or limitation? (e.g., "Currently we cannot ingest real-time data", "ClickHouse queries on sector are slow because we lack a projection"). Please describe it clearly.

## Proposed Feature
A clear and concise description of what you want to happen and the value it adds.

## Affected Components
Identify which parts of the project this feature affects:
- [ ] **Landing Zone** (new data sources, API changes)
- [ ] **Bronze Layer** (Delta Lake ingestion / raw tables)
- [ ] **Silver Layer** (PySpark transformations / deduplication)
- [ ] **Gold Layer** (ClickHouse schemas, indexing, OLAP logic)
- [ ] **Orchestration** (Airflow DAGs, schedule adjustments)
- [ ] **CI/CD** (linting, Docker builds, automation)
- [ ] **Infrastructure / Local Dev** (Docker, volumes, Makefiles)
- [ ] **Other** (please specify)

## Proposed Implementation Details
If you have a design in mind, please share technical details (e.g., PySpark code structures, ClickHouse table engines, new third-party Python packages, or configurations).

## Alternatives Considered
A clear and concise description of any alternative solutions or workarounds you've considered.

## Additional Context
Add any other context, mockup diagrams, or screenshots about the feature request here.
