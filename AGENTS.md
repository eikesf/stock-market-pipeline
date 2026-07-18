# Data Engineering Coding Standards and AI Guidelines

This document defines the strict engineering standards, architectural patterns, and workflow guidelines for all personal data engineering projects. As an AI assistant, you must read, understand, and adhere to these directives on every code modification, implementation plan, and repository operation. Do not deviate from these rules.

**This is a generic, cross-project reference**, not a spec for one specific repository. Sections 1, 2, 4, 5, 6 (Git/SQL/style/testing/CI-CD/best-practice conventions), and 7 apply universally across personal projects. Section 3 (Tool-Specific Integration Protocols) only applies to the subset of tools actually present in the current repository — see "Project Context Detection" below.

### Project Context Detection

Before applying any tool-specific rule (Section 3 or the per-tool notes in Section 6), inspect the actual repository first to determine which tools are genuinely in use — do not assume every project uses the full stack from any one past project (e.g., `stock-market-pipeline`'s PySpark + Delta + ClickHouse + Airflow + dbt combination). Concretely, check for:

- `pyproject.toml` / `uv.lock` / `requirements*.txt` → confirms Python tooling and dependency manager in use.
- `dbt_project.yml` → dbt rules apply.
- `airflow/dags/` (or similar) → Airflow rules apply.
- Spark/Delta imports in source code (`from pyspark...`, `delta-spark` in deps) → Spark rules apply.
- `Dockerfile` / `docker-compose.yml` → Docker rules apply.
- `.github/workflows/` → CI/CD rules apply.
- `terraform/` or AWS SDK usage/IAM references → AWS rules apply.

Apply only the relevant subsections for the tools detected. If a project introduces a tool not yet covered here, flag it and propose an addition to Section 6 rather than improvising undocumented conventions (ties into the Section 7 self-learning loop).


## 1. Language, Git, and Repository Standards

### Strict Language Consistency

- **English Only**: All code elements, including variables, classes, functions, databases, schemas, logging messages, comments, docstrings, git commit messages, GitHub Issues, and Pull Requests must be written exclusively in English.

### Issue Management & Ownership

- **Predefined Templates**: When creating a new issue, always use the matching configuration located in `.github/ISSUE_TEMPLATE/`.
- **Metadata & Assignment**:
  - Associate relevant labels (`bug`, `enhancement`, `refactor`) to every created issue.
  - Always assign the repository owner (`eikesf`) as the sole assignee.

### Pull Request (PR) Workflow

- **PR Formatting**: When opening a Pull Request, always use the template in `.github/pull_request_template.md`. Ensure relevant labels and the repository owner (`eikesf`) are assigned.
- **Branch Strategy**:
  - Target the `develop` branch for all active developments, enhancements, or refactoring.
  - Direct PRs to the `main` branch only when releasing stable, tested changes from `develop` (Release Merges).
- **PR Readiness Check**: Perform a complete project-wide review before opening any PR. Guarantee that the root `README.md` is updated to accurately reflect any new changes, configuration adjustments, or run commands.

### Repository Cleanliness

- **Git Version Control Safeguards**: All temporary folders, local caches, databases, credentials, and raw analytical datasets must be strictly kept out of version control. The project's `.gitignore` file must be meticulously maintained and aligned with the toolset stack.

---

## 2. Code Quality, Style, and Safety

### Style & Static Analysis Standards

- **Python Version Alignment**: Target Python 3.13+ as the project standard runtime.
- **Python Style**: Adhere strictly to the Google Python Style Guide.
- **Modern Tooling Suite**: Use `ruff` for linting and auto-formatting, and `mypy` for strict static type-checking.
- **Format Constraints**:
  - Line Length Limit: 120 characters max (`tool.ruff.line-length = 120`).
  - Quote/Indent Style: Enforce double-quotes (`"`) and standard spaces for indentation.
- **SOLID & DRY Principles**: Keep code highly modular and abstracted. Never write monolithic scripts; break logical steps into small, single-purpose, pure Python functions.

### Linter & Type Safety

- **Type Hinting & Validation**: Use strict static type signatures (`from typing import Dict, List, Any`). Validate complex configuration payloads and incoming data structures using Pydantic schemas.
- **Strict MyPy Configurations**: Ensure package bases are explicitly typed and imports are fully validated. Do not write code that breaks type checks.

### Strict Production Bans

- **No Print Statements**: Production-level pipeline scripts must never contain `print()` or `pprint()` calls (enforced by Ruff rule `T20`). Use structured logging instead.
- **No Debugger Leftovers**: Code must be completely clear of interactive debuggers (enforced by Ruff rule `T10`).

### Linting Bypass & Overrides Restrictions

- Never bypass warnings using generic tags (bare `# noqa` or `# type: ignore`). Declare the exact rule being ignored (e.g., `# noqa: PLR0913` or `# type: ignore[assignment]`).
- Respect file-specific linter exclusions configured in the project (`per-file-ignores`):
  - Test files (`tests/**/*.py`): Allowed `assert` (S101), mock password comparisons (S105/S106), and inline/delayed imports (PLC0415).
  - Airflow DAG files (`airflow/dags/**/*.py`): Allowed inline imports (PLC0415) and dynamic contexts (ANN401).
  - PySpark Streaming scripts (e.g., `src/streaming/*.py`): Allowed inline imports (PLC0415) when task serialization requires execution scope resolution.

### Absolute Secrets & Credentials Safety

- **Zero-Secrets Policy**: Never insert credentials, administrative keys, database passwords, private certificates, API keys, or AI LLM provider tokens directly into code files, environment templates, or configuration run-scripts.
- **Production Isolation**: Never read or access `.env` files directly inside production runtimes or orchestration tasks. Use secure secret management APIs (e.g., AWS Secrets Manager, Databricks Secrets, or securely injected OS environment variables).
- **Local Reference Template**: Use `.env.example` strictly as a clean, credential-free reference schema for setting up local variables.

---

## 3. Tool-Specific Integration Protocols

### Apache Spark & Databricks (PySpark 4+ & Delta 4+)

- **Stateless Transforms**: Keep business transformations decoupled from live global Spark sessions. Write pure transformation functions that take a DataFrame as input and return a modified DataFrame as output:

```python
from pyspark.sql import DataFrame

def calculate_metrics(df: DataFrame) -> DataFrame:
    return df.withColumn("total_value", df["quantity"] * df["unit_price"])
```

This strategy allows easy execution of localized unit tests with `pytest` using tiny mock DataFrames without connection overhead.

- **Schema Evolution & Migrations**: Never apply breaking schema changes (dropping/renaming columns, changing types) directly against production Delta tables without a migration path.
  - Prefer additive changes first (add new column as nullable, backfill, then deprecate the old one) over destructive changes.
  - Use Delta Lake's built-in schema evolution (`mergeSchema`) only for additive cases; document any manual `ALTER TABLE` or type-change operation as a versioned migration script (e.g., `migrations/00XX_<description>.py`), not as an ad-hoc one-off command.
  - For SCD Type 2 tables specifically, changes to tracked/versioned columns must be reviewed carefully since they affect historical row validity — never mutate closed (non-current) rows.

### AWS (Amazon Web Services)

- **Principle of Least Privilege**: Services (such as Glue jobs or ECS tasks) must execute using dedicated, isolated IAM roles with explicit, minimal S3 bucket and database access.
- **Cost Minimization for Personal Projects**: Since this is solo, personal-use infrastructure (not production for a paying customer), always prioritize minimizing cost over convenience or "best practice at scale":
  - Prefer always-free or low-cost services first: Lambda (serverless compute), S3 (storage), DynamoDB, and CloudWatch have permanent monthly free allowances that don't expire with account age.
  - Be cautious with services that carry ongoing costs regardless of usage — e.g., an always-on EMR cluster, an RDS instance, or a NAT Gateway are common sources of unexpected bills, since some carry hourly charges even when idle.
  - When a design choice has multiple viable AWS services (e.g., Glue vs. a scheduled Lambda vs. a local/Airflow-orchestrated Spark job for the same transformation), present the cost trade-offs as options with reasoning — do not silently pick the most "enterprise-standard" one. Include an estimated cost range for the current project's expected data volume when possible, and flag that AWS free-tier limits and pricing can change, so exact figures should be checked against the official AWS Free Tier page before committing to an architecture.
  - Set a billing alarm (AWS Budgets) as a standard first step whenever a new AWS service is introduced into a personal project.

### Apache Airflow (Airflow 3.x)

- **DAG Parse Time Optimization (Inline Imports)**: Always perform inline/delayed imports (inside PythonCallables/Operators rather than at the top of the file) for heavy packages such as `pyspark`, `pandas`, or database connectors. This keeps Airflow Scheduler parse times to milliseconds, avoiding CPU spikes and execution lag.
- **Idempotency & Re-runs**: Ensure every task behaves deterministically. If a DAG task is run multiple times for the same logical execute date (`{{ ds }}`), it must overwrite or upsert downstream records instead of appending duplicates.
- **Orchestration vs. Computation**: Airflow must act solely as an orchestrator, not a heavy computation worker. Offload heavy processing to external scaling runtimes using targeted operators (e.g., `DatabricksSubmitRunOperator`, `DockerOperator`, or `EmrCreateClusterOperator`).

### Artificial Intelligence & LLMOps

- **Metadata & Prompt Versioning**: Treat LLM prompts, hyper-parameters, and model target paths as code artifacts. Record run evaluations within metadata tracking servers like MLflow.
- **Structured Enforcements**: Restrict LLM completions using schema constraints (JSON Mode, or libraries like Pydantic/Instructor) to avoid malformed text structures crashing downstream processing jobs.

---

## 4. Testing, Observability, and DataOps

### Automated Testing Strategy

- **Unit Testing**: Use `pytest` to validate isolated Python helpers and PySpark transformations against small, hardcoded local DataFrames. Configure test warning suppressions (e.g., `DeprecationWarning`, `FutureWarning`) to avoid polluting outputs.
- **Integration Testing**: Validate end-to-end data pipelines within a localized Docker container ecosystem mimicking target staging configurations (PostgreSQL, ClickHouse, etc.).
- **Data Quality Assertions**: Embed proactive data quality tests (using dbt expectations, Soda Core Spark, or Great Expectations) directly into data ingestion pipelines. In dbt, verify that every staging model explicitly declares `unique` and `not_null` primary keys.

### Package & CI/CD Deployment

- **Dependency Management**: Use `uv` as the ultra-fast dependency manager. All project dependencies must have exact, pinned versions inside requirements or lockfiles (e.g., `pyspark==<exact-version>`, `delta-spark==<exact-version>`, `pandas==<exact-version>`) to prevent unexpected upstream breaks. Document dependency overrides explicitly. Never assume a version number — check the installed/locked version in the project itself (`uv pip list`, `pyproject.toml`, or the lockfile) before referencing it in code, docs, or commit messages.
- **Automated Pipelines**: Establish CI/CD automated runners (e.g., GitHub Actions) to run strict quality gatekeepers before code is merged:
  - Run `ruff check` and `ruff format --check` to catch syntax, style, and layout discrepancies.
  - Run `mypy` to guarantee type safety and prevent downstream signature conflicts.
  - Run `pytest` suites on every active pull request to test unit and transformation logic.
- **Infrastructure as Code (IaC)**: Standardize environments (S3 buckets, IAM roles, EMR clusters) across Dev, Staging, and Production layers using clean, declarative Terraform configurations.

### Data Governance & Lineage

- **Lineage Traceability**: Every table/dataset produced by a pipeline must be traceable back to its source — document, at minimum, which upstream table/API/file each downstream model or table derives from (e.g., in dbt via `ref()`/`source()`, or in a `docs/lineage.md` for non-dbt pipelines).
- **Ownership & Freshness Metadata**: For medallion-architecture layers (bronze/silver/gold), record in a lightweight manifest (or dbt `schema.yml`) the expected refresh cadence and the last successful load timestamp for each table, so staleness is easy to detect at a glance.
- **Right-Sized for Solo Use**: Since this is a personal project, skip heavyweight governance tooling (e.g., a dedicated data catalog service) unless the project's scope justifies it — a well-maintained `lineage.md` or dbt DAG visualization is sufficient.

### Observability & Incident Response

- **Logging Standard (Loguru)**: Implement `loguru` as the unified, structured logging client. Do not use Python's raw `print` or base `logging` packages directly. Ensure structured logging records operational stats (processing duration, read/write row count).
- **Proactive Alerts**: Connect system monitoring endpoints to immediate notification channels to alert on schema drift, query time-outs, or execution failures. Examples include:
  - Real-time notification hooks to Slack, Discord Webhooks, or PagerDuty on-call rotations.
  - Detailed diagnostic Email alerts (e.g., AWS SES, SendGrid, or secure SMTP) containing tracebacks, execution timestamps, and direct run links for the on-call engineer.

---

## 5. AI Assistant Execution Directives

As an AI Assistant supporting this project, enforce the following execution behaviors during every user interaction:

- **Scoped Documentation Search**: Perform web searches in the official documentation of the target tool (e.g., Python, PySpark, Airflow, dbt, Pydantic, AWS, or Databricks) only when the change involves:
  - A new or unfamiliar API/function/method signature from a library (to confirm it still exists and current syntax).
  - Introducing a new architectural pattern (e.g., a new orchestration strategy, a new medallion layer, a new streaming approach) not already established in the repo.
  - A tool/library major-version bump that could change behavior.
  For routine edits within already-established patterns (renaming variables, adjusting tests, small refactors, fixing typos), skip the search — it adds latency without adding value.
- **Obsidian Knowledge Base Integration**: Always prompt the user or construct precise semantic queries targeting the user's Obsidian MCP (Model Context Protocol) storage when addressing complex data architecture patterns. Actively cross-reference personal study notes, book summaries, and course materials to build highly contextualized solutions matching pre-documented preferences.

---

## 6. Language & Tool-Specific Best Practice Notes

This section codifies concrete, actionable practices per technology in the stack, grounded in each tool's own documentation and widely-adopted community conventions. These complement (not replace) the rules in Sections 1-4.

### Python

- Prefer `pathlib.Path` over `os.path` for filesystem operations — it's more readable and less error-prone across OSes.
- Use `dataclasses` or Pydantic models instead of raw dicts for structured config/data objects passed between functions.
- Never use mutable default arguments (`def f(x=[])`) — use `None` and initialize inside the function body.
- Favor context managers (`with` statements) for any resource that must be closed (files, DB connections, Spark sessions in tests).
- Keep functions pure where possible (same input → same output, no hidden side effects) — this is what makes the `pytest` mock-DataFrame testing strategy in Section 3 actually work.
- Use list/dict comprehensions only when they stay readable in one line or two; fall back to explicit loops for anything more complex — clarity beats cleverness.

### SQL

- Write keywords (`SELECT`, `FROM`, `WHERE`, `JOIN`, `GROUP BY`) in uppercase, and table/column identifiers in lowercase `snake_case`, for visual separation between SQL syntax and data references.
- One clause per line for anything beyond a trivial query; one column per line when the `SELECT` list is long.
- Always alias tables when more than one table is involved, and always qualify columns with their table alias in joins.
- Prefer CTEs (`WITH ... AS`) over deeply nested subqueries — each CTE should do one clear logical step, named descriptively, with a comment above it if the logic isn't obvious.
- Avoid `SELECT *` in any model or pipeline code — always name the columns you need.
- Perform aggregations as early as possible, on the smallest dataset possible, before joining to other tables.

### dbt

- Follow the canonical layer structure: `staging` (1:1 cleanup/renaming of source data, light casting, no joins) → `intermediate` (joins/logic reused across marts) → `marts` (final, consumption-ready models).
- Every model should select from `ref()`/`source()` only — never hardcode a raw table/schema name directly in a model's `FROM` clause. This is what lets dbt infer the DAG and dependency order automatically.
- At minimum, every model's primary key must have `unique` and `not_null` tests declared in `schema.yml` (already reflected in Section 4, reinforced here as a dbt-specific convention).
- Use a `dev` target for local development and only point at a `prod` target from the production/CI deployment — never develop directly against production tables.
- Materialize marts as `table` by default; only switch a model to `incremental` when data volume or rebuild cost justifies the added complexity.
- Use `sqlfluff` or `sqlfmt` to auto-lint/format SQL consistently, matching the project's chosen style.

### Apache Spark / PySpark

- Default shuffle partitions (200) are tuned for large Hadoop-era clusters — for smaller local/personal-scale jobs, tune `spark.sql.shuffle.partitions` down to match actual data size (rule of thumb: aim for ~128-200 MB of data per partition after a shuffle).
- Use `coalesce()` to reduce partitions without a full shuffle (e.g., before a final write); use `repartition()` only when you need to increase partitions or redistribute skewed data, since it triggers a full shuffle.
- Prefer broadcast joins when one side of a join is small enough to fit in executor memory — this avoids an expensive shuffle-based sort-merge join entirely.
- Enable and rely on Adaptive Query Execution (AQE) where available — it dynamically coalesces post-shuffle partitions and can rebalance skewed joins at runtime.
- Cache/`persist()` a DataFrame only when it's reused across multiple actions; an unreused cache just wastes memory.
- Avoid Python UDFs when an equivalent native Spark SQL function exists — native functions run in the JVM without the Python serialization overhead.

### Apache Airflow

- Never put non-deterministic code (e.g., `datetime.now()`, live API calls) at the top level of a DAG file — top-level code re-executes on every scheduler heartbeat and breaks idempotency (a DAG re-run for a past date should always produce the same result). Push such logic inside task callables, or use Airflow Variables/templated fields instead.
- Prefer the TaskFlow API (`@task` decorators) for Python-heavy DAGs — it's more readable than manually wiring `PythonOperator` + `XCom` push/pull.
- Never delete a task from a DAG once it has run — historical task instance data disappears from the UI. If a task is truly obsolete, create a new DAG version instead.
- Use deferrable operators for tasks that wait on external systems for a long time — they free the worker slot while waiting, instead of blocking it.
- Write at least a basic DAG-integrity test (import the DAG via `DagBag` and assert there are no import errors) as part of the test suite — this catches broken DAGs before they ever hit the scheduler.

### Docker

- Structure Dockerfiles as multi-stage builds: a build stage with full toolchain/compilers, and a slim final stage that copies over only the runtime artifacts needed.
- Order Dockerfile instructions from least-to-most frequently changing: install system/OS deps first, then Python deps (`requirements`/lockfile), then application code last — this maximizes Docker's layer cache reuse on rebuilds.
- Prefer `python:<version>-slim` as the default base image; only move to `alpine` if you've verified all dependencies (especially ones with C extensions) are compatible with musl libc.
- Always include a `.dockerignore` (mirroring `.gitignore` where relevant) to keep the build context small and avoid leaking local caches/credentials into the image.
- Run the container process as a non-root user in the final image.
- Pin base image tags (and ideally digests) rather than floating tags like `latest`, for reproducible builds.

### Git

- Follow Conventional Commits format: `<type>(<optional scope>): <description>` (e.g., `fix(airflow): correct idempotent execution date handling`), using imperative mood ("add", not "added"/"adds").
- Keep the commit subject line short (~50 characters) and use the body to explain *why*, not just *what*, when the change isn't self-evident.
- Keep commits atomic — one logical change per commit — rather than bundling unrelated changes together.
- Branch naming should signal intent and match the type prefix convention already used for commits (e.g., `feature/scd2-backfill`, `fix/dag-parse-time`, `chore/bump-delta-version`).

### CI/CD (GitHub Actions)

- Keep workflows small and focused by purpose (`ci.yml` for lint+test, a separate workflow for deploy/release) rather than one large monolithic YAML file.
- Pin third-party Actions to a specific version tag (or commit SHA for maximum supply-chain safety) — never rely on a floating `@main`/`@latest` reference.
- Cache dependencies (`actions/cache` or the built-in cache option in `setup-python`/`setup-node`) keyed on a hash of the lockfile, so cache invalidates automatically when dependencies change.
- Set explicit, minimal `permissions:` at the workflow or job level rather than relying on default broad tokens.
- Prefer OIDC-based temporary credentials for any AWS deployment step over long-lived static AWS access keys stored as secrets.
- Add a `concurrency` group with `cancel-in-progress: true` so redundant runs on the same branch don't queue up and waste CI minutes.

### AWS

- Grant IAM permissions scoped to specific resources and specific actions (e.g., `s3:GetObject` on one named bucket) rather than wildcard actions (`s3:*`) or account-wide access — this is the Well-Architected Framework's least-privilege guidance.
- Prefer IAM roles with temporary credentials (including OIDC from CI/CD) over long-lived IAM user access keys wherever possible.
- Periodically review for unused permissions/roles — AWS IAM Access Analyzer can surface permissions that have never actually been exercised.
- Require MFA for any human access to the AWS console, especially for the root account, which should not be used for day-to-day operations at all.
- (See Section 3's Cost Minimization guidance for the cost side of AWS usage in this personal-project context.)

---

## 7. Self-Learning, Error Correction, and Style Adaptation

To ensure this workspace evolves seamlessly around the owner's problem-solving preferences and coding style, the AI assistant must operate on an active feedback loop:

### Post-Mortem Rule Codification

- **Identify Root Cause**: When an error, bug, or design misstep is encountered and resolved (e.g., syntax errors, environment mismatches, unexpected tool updates, or architectural failures), analyze why the issue happened.
- **Document the Rule**: If the failure points to a systemic knowledge gap, missing edge case, or lack of tool-specific specification in this guide, actively suggest or apply an adjustment to this Markdown file. Codify the corrected behavior or a prevention rule inside the appropriate section to guarantee the mistake is never repeated.

### Developer Style Synchronization

- **Preference Tracking**: Observe the code patterns, architecture strategies, and logic choices the repository owner chooses, accepts, or requests.
- **Incorporate Constraints**: Keep this document synchronized with those habits (e.g., specific library preferences, data validation styles, logging formats, or folder structures). Update sections accordingly so the AI behaves as an extension of the developer's specific technical mind.