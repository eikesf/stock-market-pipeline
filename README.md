# End-to-End Batch Stock Market Pipeline

![Python](https://img.shields.io/badge/Python-3.13-blue?style=flat-square&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-4.1.1-e25a1c?style=flat-square&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-4.2.0-00AAD2?style=flat-square)
![ClickHouse](https://img.shields.io/badge/ClickHouse-25.8_LTS-FFCC01?style=flat-square&logo=clickhouse&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=flat-square&logo=docker&logoColor=white)
![CI/CD](https://github.com/eikesf/stock-market-pipeline/actions/workflows/ci.yml/badge.svg)
![Coverage](https://img.shields.io/badge/Coverage-80%25+-brightgreen?style=flat-square)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-3.2.2-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)

An end-to-end Data Engineering platform that ingests financial asset data and metadata from **B3, NASDAQ, and NYSE**, processes it through a **Medallion Architecture** (Landing → Bronze → Silver → Gold) using PySpark and Delta Lake, and delivers analytics-ready data via ClickHouse in a **Star Schema**.

The ingestion and Medallion steps are fully orchestrated using **Apache Airflow 3.x** DAGs via direct Python task execution (`@task`), while also maintaining standalone `make` command access for granular execution and local testing. Tickers for each exchange are defined in `src/producer/tickers.py`.

## Table of Contents
- [Architecture & Pipelines](#architecture--pipelines)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Code Quality](#code-quality)
- [Airflow Orchestration](#airflow-orchestration)
- [Executing the Pipelines](#executing-the-pipelines)
- [Running the Tests](#running-the-tests)
- [ClickHouse Analytics Star Schema](#clickhouse-analytics-star-schema)
- [CI/CD Pipeline](#cicd-pipeline)
- [Roadmap & Future Improvements](#roadmap--future-improvements)

---

## Architecture & Pipelines

Two parallel ELT pipelines populate a Star Schema in ClickHouse (`fact_prices` as the Fact Table and `dim_companies` as the Dimension Table).

```mermaid
graph LR
    %% ── Data Sources ─────────────────────────────────────────────
    subgraph Sources ["External Data & Discovery"]
        Wiki["Wikipedia"]
        BRAPI["BRAPI"]
        Tickers["Tickers List"]
        YF_API[("yfinance API")]

        Wiki --> Tickers
        BRAPI --> Tickers
        Tickers --> YF_API
    end

    %% ── Control Plane ────────────────────────────────────────────
    Airflow{{"Apache Airflow<br/>Orchestrator"}}

    %% Invisible link to keep Airflow in a middle layer and avoid edge crossings
    YF_API ~~~ Airflow

    %% ── Daily Pipeline ───────────────────────────────────────────
    subgraph DAG_Prices ["DAG: dag_stock_prices"]
        T_EXT_P["task_extract_prices<br/>(Landing · Parquet)"]
        T_BRZ_P["task_ingest_bronze_prices<br/>(Bronze · Delta)"]
        T_SLV_P["task_deduplicate_silver_prices<br/>(Silver · Delta)"]
        T_GLD_P["task_load_gold_prices<br/>(Gold · Direct)"]

        T_EXT_P -->|"PySpark"| T_BRZ_P
        T_BRZ_P -->|"PySpark"| T_SLV_P
        T_SLV_P -->|"PySpark"| T_GLD_P
    end

    %% ── Monthly Pipeline ─────────────────────────────────────────
    subgraph DAG_Metadata ["DAG: dag_stock_metadata"]
        T_EXT_M["task_extract_metadata<br/>(Landing · JSON)"]
        T_BRZ_M["task_ingest_bronze_metadata<br/>(Bronze · Delta)"]
        T_SLV_M["task_deduplicate_silver_metadata<br/>(Silver · Delta)"]
        T_GLD_M["task_load_gold_metadata<br/>(Gold · Staging)"]

        T_EXT_M -->|"PySpark"| T_BRZ_M
        T_BRZ_M -->|"PySpark"| T_SLV_M
        T_SLV_M -->|"PySpark"| T_GLD_M
    end

    %% ── Weekly Maintenance Pipeline ──────────────────────────────
    subgraph DAG_Maintenance ["DAG: dag_delta_maintenance"]
        T_MNT["task_optimize_and_vacuum<br/>(Delta Lake Maintenance)"]
    end

    %% ── OLAP Storage ─────────────────────────────────────────────
    subgraph ClickHouse ["Gold Layer  ·  ClickHouse (OLAP)"]
        G_FACT["table: fact_prices"]
        G_DIM["table: dim_companies"]
        G_FACT -.->|"Star Schema"| G_DIM
    end

    %% ── Control & Data Flows ─────────────────────────────────────
    %% Orchestration (dashed) — schedule on the arrows
    Airflow -.->|"Mon–Fri · 22:00 UTC"| T_EXT_P
    Airflow -.->|"@monthly"| T_EXT_M
    Airflow -.->|"@weekly"| T_MNT

    %% Extraction (bold)
    YF_API ==>|"OHLCV Data"| T_EXT_P
    YF_API ==>|"Metadata"| T_EXT_M

    %% Loading (bold)
    T_GLD_P ==>|"Load"| G_FACT
    T_GLD_M ==>|"Load"| G_DIM

    %% Maintenance (dashed)
    T_MNT -.->|"Compaction & Vacuum"| T_BRZ_P
    T_MNT -.->|"Compaction & Vacuum"| T_BRZ_M
    T_MNT -.->|"Compaction & Vacuum"| T_SLV_P
    T_MNT -.->|"Compaction & Vacuum"| T_SLV_M

    %% ── Class Definitions ────────────────────────────────────────
    classDef orchestrator fill:#e62464,stroke:#c2185b,stroke-width:2px,color:#fff
    classDef api         fill:#1565c0,stroke:#0d47a1,stroke-width:2px,color:#fff
    classDef source      fill:#e1f5fe,stroke:#039be5,color:#01579b
    classDef tickerList  fill:#b3e5fc,stroke:#039be5,color:#01579b
    classDef landing     fill:#eceff1,stroke:#607d8b,color:#263238
    classDef bronze      fill:#ffe0b2,stroke:#fb8c00,color:#bf360c
    classDef silver      fill:#cfd8dc,stroke:#546e7a,color:#212121
    classDef gold        fill:#fff9c4,stroke:#f9a825,color:#e65100
    classDef dagBox      fill:#01579b,stroke:#003c75,stroke-width:2px,color:#fff
    classDef sourceBox   fill:#0288d1,stroke:#01579b,stroke-width:2px,color:#fff
    classDef olapBox     fill:#e65100,stroke:#bf360c,stroke-width:2px,color:#fff
    classDef olapTable   fill:#ffffff,stroke:#f9a825,color:#e65100

    %% ── Apply Classes ────────────────────────────────────────────
    class Airflow orchestrator
    class YF_API api
    class Wiki,BRAPI source
    class Tickers tickerList
    class T_EXT_P,T_EXT_M landing
    class T_BRZ_P,T_BRZ_M bronze
    class T_SLV_P,T_SLV_M silver
    class T_GLD_P,T_GLD_M,T_MNT gold
    class Sources sourceBox
    class DAG_Prices,DAG_Metadata,DAG_Maintenance dagBox
    class ClickHouse olapBox
    class G_FACT,G_DIM olapTable
```


**Pipeline A - Daily Stock Prices (Fact):** extracts daily OHLCV time-series from `yfinance`, stores raw Parquet files in the Landing zone, ingests into Delta Lake (Bronze), deduplicates via PySpark window functions (Silver), and loads into ClickHouse as `fact_prices`.

**Pipeline B - Monthly Metadata (Dimension):** extracts company information (sector, industry, country, isin, full_time_employees, exchange, currency, market cap, dividend yield) from `yfinance`, follows the same Bronze/Silver medallion flow, and loads into ClickHouse as `dim_companies`.

---

## Tech Stack


| Layer | Technology | Version | Why |
|---|---|---|---|
| **Language** | Python | `3.13-slim` | Current stable release; minimal Docker footprint via slim image. |
| **Java Environment** | OpenJDK | `21` | JVM runtime required by Apache Spark 4.x. |
| **Bronze / Silver** | PySpark + Delta Lake | `Spark 4.1.1` · `Delta 4.2.0` | ACID transactions, time travel, schema enforcement, and window-based deduplication. |
| **Gold / OLAP** | ClickHouse | `25.8 LTS (Alpine)` | Columnar OLAP database providing sub-second aggregations on large datasets. |
| **Database Driver** | clickhouse-connect | `1.0.0` | Official lightweight Python connector; no JDBC dependency required. |
| **Orchestration** | Apache Airflow | `3.2.2` | Decoupled architecture (`api-server`, `scheduler`, `dag-processor`) orchestrating Medallion tasks. |
| **Metastore** | PostgreSQL | `18-alpine` | Metadata database for Apache Airflow. |
| **Package Manager** | uv | `0.11.19` | Fast, deterministic Python dependency resolution with lockfile support. |
| **Type Checking** | Mypy | `>=2.1.0` | Static type analysis enforced in CI for production code safety. |
| **Linting / Formatting** | Ruff | `0.15.15` | Fast all-in-one linter and formatter replacing flake8, isort, and black. |
| **Logging** | Loguru | `0.7.3` | Structured, zero-config logger with rich formatting and rotation support. |
| **Testing** | Pytest + Coverage | `8.3.4` | Test framework with 80% coverage enforcement via `pytest-cov`. |

---

## Project Structure

```text
stock_market_pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml           # CI/CD GitHub Actions workflow
├── .env.example             # Environment variables template
├── Makefile                 # CLI task runner
├── pyproject.toml           # Unified Python config and dependencies (PEP 621/735)
├── uv.lock                  # Dependency lockfile (fully resolved)
├── airflow/                 # Centralized Airflow orchestration directory
│   ├── dags/                # Python DAG definitions
│   │   ├── dag_delta_maintenance.py # Weekly Delta maintenance DAG
│   │   ├── dag_stock_metadata.py # Monthly metadata ingestion DAG
│   │   └── dag_stock_prices.py   # Daily stock prices ingestion DAG
│   ├── config/              # Autogenerated configs and admin secrets
│   └── logs/                # Task execution logs
├── data/                    # Shared data volume (created at runtime)
│   ├── bronze/              # Delta Bronze layer (prices/ & metadata/)
│   ├── landing/             # Raw extractions (prices/ & metadata/)
│   └── silver/              # Delta Silver layer (prices/ & metadata/)
├── docker/
│   ├── Dockerfile           # Python 3.13 + Java 21 image
│   └── docker-compose.yml   # Full multi-service stack (Airflow, ClickHouse, Python)
├── src/
│   ├── db_init/init.sql     # ClickHouse DDL (auto-run on first boot)
│   ├── producer/            # Landing layer (extraction)
│   │   ├── config.py        # Configs and path mappings
│   │   ├── generator.py     # Pipeline A: prices extractor
│   │   ├── metadata_generator.py # Pipeline B: metadata extractor
│   │   └── tickers.py       # Monitored tickers (NASDAQ, B3, NYSE)
│   ├── streaming/           # Medallion layers (Spark/Delta/ClickHouse)
│   │   ├── spark_session.py # SparkSession factory
│   │   ├── utils.py         # Shared IO utilities (Delta read/write & ClickHouse)
│   │   ├── bronze.py        # Bronze: prices ingestion
│   │   ├── silver.py        # Silver: prices deduplication
│   │   ├── bronze_metadata.py # Bronze: metadata ingestion
│   │   ├── silver_metadata.py # Silver: metadata deduplication
│   │   ├── gold.py          # Gold: ClickHouse writer
│   │   └── maintenance.py   # Delta Lake table maintenance (compaction + vacuum)
│   └── utils/
│       └── logger.py        # Centralized Loguru logger configuration
└── tests/                   # Pytest suite
    ├── conftest.py          # PySpark and local delta test fixtures
    ├── producer/            # Tests for generator and ticker fetcher
    └── streaming/           # Medallion layer and ClickHouse integration tests
        ├── test_maintenance.py   # Tests for Delta maintenance module
        └── test_gold.py     # Tests for Gold loading integration
```

---

## Getting Started

### Prerequisites
- [Docker Desktop](https://docker.com) installed and running.
- `make` installed (`brew install make` on macOS / Linux).

### 1 — Configure environment variables

Duplicate the environment variables file and configure your credentials in the new `.env` file:

```bash
cp .env.example .env
```


### 2 — Build and start the containers

Start the multi-container environment (Airflow services, PostgreSQL metastore, ClickHouse, and Python workspace):

```bash
make build
```
This starts the following services:
- `stock_clickhouse`: ClickHouse server on HTTP port `8123` and native TCP port `9000`. Runs `src/db_init/init.sql` automatically on first boot.
- `airflow_postgres`: PostgreSQL 18 database metastore for Airflow.
- `airflow_init`: One-off database migration and admin user creation task.
- `airflow_apiserver`: Airflow 3 API Server and Web UI on port `8080`.
- `airflow_scheduler`: Airflow Scheduler orchestrating DAGs.
- `airflow_dag_processor`: Standalone Dag Processor parsing DAG definitions.
- `python_finance`: Isolated Python 3.13 workspace containing Java 21, Spark 4.1.1, and dev tools.

### 3 — Tear down the environment

```bash
make down  # Stop and remove containers (data is preserved)
make clean # Stop containers and remove Docker volumes (ClickHouse data is lost)
make reset # Full reset: clean + remove local data/ directory
```

### 4 — Control Orchestration Modularly (Optional)

If you are developing locally and wish to stop only the resource-heavy Airflow orchestration containers to free up system resources while keeping ClickHouse and the Python workspace active:

```bash
make airflow_down # Stop Airflow Postgres, Webserver, Scheduler, and Dag Processor
make airflow_up   # Start only the Airflow orchestration services
```

---

## Code Quality

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting, 
configured in `pyproject.toml`.

**Line length:** 120 characters, enforced by `ruff format`. `E501` is disabled in 
the linter to avoid duplicate reporting.

To run checks inside the container:
```bash
make lint       # Check for style/lint issues
make lint_fix   # Check and apply auto-fixes for safe violations
make format     # Format files according to style guidelines
```

Or locally (outside the container, using uv):
```bash
uv run ruff check .          # linter
uv run ruff format --check . # verify formatting without applying changes
uv run ruff format .         # apply formatting
```

The CI pipeline runs both checks automatically on every push and pull request.

---

## Airflow Orchestration

The project runs an Apache Airflow 3.2.2 cluster inside Docker, utilizing a decoupled architecture:
- `airflow_apiserver`: Exposes the Web UI and API.
- `airflow_scheduler`: Schedules and triggers task runs.
- `airflow_dag_processor`: Parses DAG files.
- `airflow_postgres`: Metastore database.

### Accessing the Web UI
The Airflow Web UI is accessible at [http://localhost:8080](http://localhost:8080).

**Credentials** (as defined in your `.env`):
*   **Username:** value of `AIRFLOW_ADMIN_USER`
*   **Password:** value of `AIRFLOW_ADMIN_PASSWORD`

### Scheduled DAGs
1.  **`dag_stock_prices`** (Daily Pipeline):
    *   **Schedule:** Every weekday (Monday to Friday) at 22:00 UTC (`0 22 * * 1-5`).
    *   **Flow:** Extracts prices to Landing → Ingests to Bronze → Deduplicates to Silver → Loads `fact_prices` in ClickHouse.
2.  **`dag_stock_metadata`** (Monthly Pipeline):
    *   **Schedule:** Runs monthly (`@monthly`).
    *   **Flow:** Extracts company profiles to Landing → Ingests to Bronze → Deduplicates to Silver → Loads `dim_companies` in ClickHouse.
3.  **`dag_delta_maintenance`** (Weekly Maintenance Pipeline):
    *   **Schedule:** Runs weekly on Sundays (`@weekly`).
    *   **Flow:** Runs compaction (`OPTIMIZE`) and cleanup (`VACUUM`) on the Bronze and Silver Delta tables to resolve the small file problem and manage storage.

---

## Executing the Pipelines

### Run the full Medallion Pipeline
Triggers the entire workflow sequentially (Landing → Bronze → Silver → Gold):

```bash
make run          # Run the full pipeline (both prices and metadata) for local testing
make run_prices   # Run only the Prices pipeline (Daily)
make run_metadata # Run only the Metadata pipeline (Monthly)
```

### Run Layer-Specific Commands

#### Ingestion (Landing)

```bash
make run_landing_prices     # Extract daily prices from yfinance
make run_landing_metadata   # Extract company metadata from yfinance
```

#### Bronze

```bash
make run_bronze_prices      # Load prices parquet -> Delta Bronze
make run_bronze_metadata    # Load metadata parquet -> Delta Bronze
```

#### Silver

```bash
make run_silver_prices      # Deduplicate prices -> Delta Silver
make run_silver_metadata    # Deduplicate metadata -> Delta Silver
```

#### Gold

```bash
make run_gold               # Load Silver data into ClickHouse (both tables)
make run_gold_prices        # Load only fact_prices into ClickHouse
make run_gold_metadata      # Load only dim_companies into ClickHouse
```

#### Maintenance

```bash
make run_maintenance        # Run Delta table maintenance (compaction + vacuum) manually
```

### Utility Commands

```bash
make up    # Start the Docker environment (without rebuilding)
make shell # Access the Python container bash shell
```

---

## Running the Tests

The project includes a comprehensive test suite using `pytest` and `unittest.mock`
to validate data quality, pipeline flows, exchange standardization, Spark
deduplications, and ClickHouse transactional safety without requiring live database
connections.

### Running inside the container
```bash
make test
```
Or directly:
```bash
docker exec python_finance pytest
```

### Running with coverage report
```bash
make test_cov
```
Or directly:
```bash
docker exec python_finance pytest --cov=src --cov-report=term-missing --cov-fail-under=80
```

The CI enforces a minimum coverage threshold of **80%**. Test configuration is 
defined in `pyproject.toml` under `[tool.pytest.ini_options]`.

---

## ClickHouse Analytics Star Schema

Connect with DataGrip, DBeaver, or any ClickHouse-compatible client to `localhost:8123` using the credentials from your `.env` file.

### Dimension Table: `stock_market.dim_companies`

| Column | Type | Sorting Key | Description |
|---|---|---|---|
| `ticker` | LowCardinality(String) | Yes | Stock ticker symbol |
| `short_name` | String | | Company display name |
| `sector` | LowCardinality(String) | | GICS sector classification |
| `industry` | LowCardinality(String) | | Industry classification |
| `country` | LowCardinality(String) | | Country where the company is based |
| `isin` | String | | International Securities Identification Number |
| `full_time_employees` | UInt32 | | Number of full-time employees |
| `exchange` | LowCardinality(String) | | Exchange (NASDAQ, NYSE, B3) |
| `market_cap` | UInt64 | | Market capitalization in USD |
| `currency` | LowCardinality(String) | | Currency in which the stock is traded |
| `dividend_yield` | Decimal(10,2) | | Annual dividend yield (%) |
| `extraction_date` | Date | | Date of yfinance extraction |
| `ingestion_timestamp` | DateTime | | Ingestion timestamp |
 
### Fact Table: `stock_market.fact_prices`

| Column | Type | Sorting Key | Description |
|---|---|---|---|
| `date` | Date | Yes | Trading date |
| `ticker` | LowCardinality(String) | Yes | Stock ticker symbol |
| `open` | Decimal(10,2) | | Opening price |
| `high` | Decimal(10,2) | | Daily high price |
| `low` | Decimal(10,2) | | Daily low price |
| `close` | Decimal(10,2) | | Closing price |
| `adj_close` | Decimal(10,2) | | Adjusted closing price |
| `volume` | UInt64 | | Shares traded |
| `dividends` | Decimal(10,2) | | Total dividends paid during the day |
| `stock_splits` | Decimal(10,4) | | Stock split ratio for the trading day |
| `ingestion_timestamp` | DateTime | | Ingestion timestamp |
 
> In ClickHouse, `MergeTree` does not have primary keys or foreign keys in the relational sense. The `ORDER BY` clause defines the **sorting key**,
which also serves as the implicit sparse index used to skip data blocks during queries. Deduplication is handled upstream in the Silver layer before data reaches ClickHouse.

> `fact_prices` is partitioned by `toYYYYMM(date)`, enabling partition
pruning on date range filters and efficient monthly data management.

### Example queries

Aggregate trading volume by sector:
```sql
SELECT
    c.short_name,
    c.sector,
    MAX(p.close) AS peak_price,
    SUM(p.volume) AS total_traded_volume
FROM stock_market.fact_prices p
JOIN stock_market.dim_companies c ON p.ticker = c.ticker
GROUP BY c.sector, c.short_name
ORDER BY total_traded_volume DESC;
```

30-day moving average with window functions (leverages partition pruning on `toYYYYMM(date)`):
```sql
SELECT
    c.short_name,
    p.date,
    p.close,
    AVG(p.close) OVER (
        PARTITION BY p.ticker
        ORDER BY p.date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS moving_avg_30d
FROM stock_market.fact_prices p
JOIN stock_market.dim_companies c ON p.ticker = c.ticker
WHERE p.date >= today() - INTERVAL 90 DAY
ORDER BY p.ticker, p.date;
```

---

## CI/CD Pipeline

The project includes a GitHub Actions pipeline with two sequential jobs:

**`lint-and-test`** — runs on every push and pull request to `main` and `develop`:
- Ruff linter and formatter check
- Mypy strict type checking across `src/`
- pytest with 80% coverage enforcement

**`deploy`** — runs only on push to `main`, after `lint-and-test` passes:
- Builds the Docker image from `docker/Dockerfile`
- Pushes to GitHub Container Registry (GHCR) with two tags:
  - `latest` — always points to the most recent `main` build
  - `sha-<commit>` — immutable tag for full traceability

```bash
# Pull the latest published image
docker pull ghcr.io/eikesf/stock-market-pipeline:latest
```

---

## Roadmap & Future Improvements
- [x] **Orchestration:** Implement Apache Airflow DAGs to replace `make` command execution.
- [ ] **Data Quality:** Integrate Great Expectations or Soda for automated data quality assertions in the Silver layer.
- [ ] **Type Safety:** Achieve full Mypy strict compliance across the `src/` module.
- [ ] **Observability:** Add structured logging with correlation IDs per pipeline run.

