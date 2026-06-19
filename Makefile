# Docker Environment Variables
COMPOSE_FILE = docker/docker-compose.yml
export DOCKER_CLI_HINTS=false

.PHONY: up down build shell airflow_up airflow_down lint lint_fix format test test_cov run run_prices run_metadata _prices_flow _metadata_flow run_landing_prices run_landing_metadata \
		run_bronze_prices run_bronze_metadata run_silver_prices \
		run_silver_metadata run_gold run_gold_prices run_gold_metadata run_maintenance clean clean_data reset

# --- Infrastructure ---
up: ## Start the Docker environment
	docker compose --env-file .env -f $(COMPOSE_FILE) up -d

build: ## Build and start the Docker environment
	docker compose --env-file .env -f $(COMPOSE_FILE) up -d --build

down: ## Stop and remove the Docker environment
	docker compose --env-file .env -f $(COMPOSE_FILE) down

shell: ## Access the Python container bash shell
	docker exec -it python_finance bash

airflow_up: ## Start only the Airflow orchestration services
	docker compose --env-file .env -f $(COMPOSE_FILE) up -d airflow_postgres airflow_init airflow_apiserver airflow_scheduler airflow_dag_processor

airflow_down: ## Stop only the Airflow orchestration services
	docker compose --env-file .env -f $(COMPOSE_FILE) stop airflow_postgres airflow_apiserver airflow_scheduler airflow_dag_processor

# --- Quality & Testing ---
lint: ## Run Ruff linter checks inside the container
	docker exec python_finance ruff check .

lint_fix: ## Run Ruff linter checks and automatically apply fixes inside the container
	docker exec python_finance ruff check --fix .

format: ## Run Ruff formatter inside the container
	docker exec python_finance ruff format .

test: ## Run pytest suite inside the container
	docker exec python_finance pytest

test_cov: ## Run pytest suite with coverage inside the container
	docker exec python_finance pytest --cov=src --cov-report=term-missing --cov-fail-under=80

# --- Environment Management ---
clean: ## Stop containers and remove docker volumes (Clickhouse data)
	@echo "Stopping Docker containers and removing database volumes..."
	@echo "Note: Local data directory (data/) is preserved."
	docker compose --env-file .env -f $(COMPOSE_FILE) down --volumes

clean_data: ## Remove the local data directory (landing/, bronze/, silver/)
	@echo "WARNING: This will permanently delete the local 'data/' directory (landing files, bronze/silver Delta tables, transaction logs, and history)."
	@printf "Are you sure you want to continue? [y/N]: " && read ans && [ "$$ans" = "y" ] | [ "$$ans" = "Y" ] || (echo "Aborted."; exit 1)
	rm -rf data/

reset: clean clean_data ## Full reset: remove containers, volumes, and local data	

# --- Pipeline Execution ---
_prices_flow:
	@echo "--- Ingestion & Processing: Prices ---"
	@$(MAKE) run_landing_prices
	@$(MAKE) run_bronze_prices
	@$(MAKE) run_silver_prices

_metadata_flow:
	@echo "--- Ingestion & Processing: Metadata ---"
	@$(MAKE) run_landing_metadata
	@$(MAKE) run_bronze_metadata
	@$(MAKE) run_silver_metadata

run_prices: _prices_flow run_gold_prices ## Run only Prices pipeline

run_metadata: _metadata_flow run_gold_metadata ## Run only Metadata pipeline

run: _prices_flow _metadata_flow run_gold ## Run the full Medallion pipeline
	@echo "\nFull pipeline completed successfully."

run_landing_prices: ## Run only Landing layer for prices
	@docker exec -it python_finance python -m src.producer.generator

run_landing_metadata: ## Run only Landing layer for metadata
	@docker exec -it python_finance python -m src.producer.metadata_generator

run_bronze_prices: ## Run only Bronze layer for prices
	@docker exec -it python_finance python -m src.streaming.bronze

run_bronze_metadata: ## Run only Bronze layer for metadata
	@docker exec -it python_finance python -m src.streaming.bronze_metadata

run_silver_prices: ## Run only Silver layer for prices
	@docker exec -it python_finance python -m src.streaming.silver

run_silver_metadata: ## Run only Silver layer for metadata
	@docker exec -it python_finance python -m src.streaming.silver_metadata

run_gold: ## Run only Gold layer (both prices and metadata)
	@docker exec -it python_finance python -m src.streaming.gold

run_gold_prices: ## Run only Gold layer for prices
	@docker exec -it python_finance python -m src.streaming.gold --table prices

run_gold_metadata: ## Run only Gold layer for metadata
	@docker exec -it python_finance python -m src.streaming.gold --table metadata

run_maintenance: # Run Delta Lake table maintenance (Compaction + vacuum)
	@docker exec -it python_finance python -m src.streaming.maintenance
