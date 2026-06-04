# Docker Environment Variables
COMPOSE_FILE = docker/docker-compose.yml
export DOCKER_CLI_HINTS=false

.PHONY: up down build shell lint lint_fix format test test_cov run run_landing_prices run_landing_metadata \
		run_bronze_prices run_bronze_metadata run_silver_prices \
		run_silver_metadata run_gold clean clean_data reset

# --- Infrastructure ---
up: ## Start the Docker environment
	docker compose --env-file .env -f $(COMPOSE_FILE) up -d

build: ## Build and start the Docker environment
	docker compose --env-file .env -f $(COMPOSE_FILE) up -d --build

down: ## Stop and remove the Docker environment
	docker compose --env-file .env -f $(COMPOSE_FILE) down

shell: ## Access the Python container bash shell
	docker exec -it python_finance bash

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
	docker compose --env-file .env -f $(COMPOSE_FILE) down --volumes

clean_data: ## Remove the local data directory (landing/, bronze/, silver/)
	rm -rf data/

reset: clean clean_data ## Full reset: remove containers, volumes, and local data	

# --- Pipeline Execution ---
run: ## Run the full Medallion pipeline (Landing -> Bronze -> Silver -> Gold)
	@echo "--- 1. Landing: Extraction ---"
	@$(MAKE) run_landing_prices
	@$(MAKE) run_landing_metadata
	@echo "\n--- 2. Bronze: Raw Ingestion ---"
	@$(MAKE) run_bronze_prices
	@$(MAKE) run_bronze_metadata
	@echo "\n--- 3. Silver: Cleaning and Deduplication ---"
	@$(MAKE) run_silver_prices
	@$(MAKE) run_silver_metadata
	@echo "\n--- 4. Gold: Loading into ClickHouse ---"
	@$(MAKE) run_gold
	@echo "\nPipeline completed successfully."

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

run_gold: ## Run only Gold layer
	@docker exec -it python_finance python -m src.streaming.gold
