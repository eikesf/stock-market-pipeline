---
name: Performance Tuning
about: Track slow pipelines, query latency bottlenecks, or resource-heavy transformations.
title: '[PERF] '
labels: 'performance'
assignees: ''
---

## Performance Bottleneck Description
What is slow or consuming excessive resources (CPU, Memory, Disk IO)? (e.g., "Silver layer deduplication Spark shuffle phase is taking over 15 minutes", "ClickHouse group by query on sector takes 3 seconds").

## Metrics & Evidence
Please share timing statistics, query plans, or profile logs:
- **Current execution time / resources used:** 
- **Target execution time / resources:** 
- **Query plan or console logs:**
  ```sql
  -- For ClickHouse: EXPLAIN query...
  ```
  *(Or paste Spark UI screenshots/metrics here)*

## Affected Layers / Components
Identify which components are contributing to the performance issue:
- [ ] **Extraction** (yfinance rate-limiting, API payload overhead)
- [ ] **Ingestion** (Spark writing Parquet / schema overhead)
- [ ] **Silver Transformation** (Deduplication, sorting, join/shuffles)
- [ ] **Gold Load** (ClickHouse load speed, batch size)
- [ ] **OLAP Queries** (ClickHouse index usage, sorting keys, partition pruning)
- [ ] **Other** (please specify)

## Proposed Optimization
What strategies should we test? (e.g., updating ClickHouse primary/sorting key, adjusting Spark session configuration like `spark.sql.shuffle.partitions`, optimizing PySpark window functions, scaling Docker memory allocations).

## Verification Criteria
How will we prove the optimization works? (e.g., "Rerun full pipeline and observe Spark job duration < 5 minutes").
