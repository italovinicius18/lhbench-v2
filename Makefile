.PHONY: help setup build up down logs clean benchmark status

.DEFAULT_GOAL := help

# Colors for output
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
BLUE   := $(shell tput -Txterm setaf 4)
RESET  := $(shell tput -Txterm sgr0)

# Configuration
DATA_DIR := /mnt/c/Users/italo/WSL_DATA/lakehouse-data

help: ## Show this help message
	@echo '$(BLUE)Lakehouse Benchmark - Available Commands$(RESET)'
	@echo ''
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(RESET) %s\n", $$1, $$2}'
	@echo ''

setup: ## Initial setup - create directories and .env
	@echo '$(YELLOW)🔧 Setting up environment...$(RESET)'
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo '$(GREEN)✓ Created .env file from .env.example$(RESET)'; \
		echo '$(YELLOW)⚠ Please edit .env with your configuration$(RESET)'; \
	else \
		echo '$(GREEN)✓ .env already exists$(RESET)'; \
	fi
	@mkdir -p $(DATA_DIR)/{bronze,silver,gold}
	@mkdir -p $(DATA_DIR)/bronze/tpch/{_metadata,sf1,sf10,sf100}
	@mkdir -p $(DATA_DIR)/silver/{iceberg,delta,hudi}
	@mkdir -p $(DATA_DIR)/gold/{metrics,reports}
	@echo '$(GREEN)✓ Data directories created$(RESET)'
	@echo '$(GREEN)✅ Setup complete!$(RESET)'

build: ## Build all Docker images
	@echo '$(YELLOW)🐳 Building Docker images...$(RESET)'
	docker compose build
	@echo '$(GREEN)✅ Build complete!$(RESET)'

up: ## Start all services
	@echo '$(YELLOW)🚀 Starting services...$(RESET)'
	docker compose up -d spark-master spark-worker-1 spark-worker-2
	@echo '$(GREEN)✓ Waiting for Spark cluster to be ready...$(RESET)'
	@sleep 10
	@echo '$(GREEN)✅ Services started!$(RESET)'
	@echo ''
	@echo '$(BLUE)Spark Master UI:$(RESET) http://localhost:8080'
	@echo '$(BLUE)Worker 1 UI:$(RESET)     http://localhost:8081'
	@echo '$(BLUE)Worker 2 UI:$(RESET)     http://localhost:8082'

down: ## Stop all services
	@echo '$(YELLOW)🛑 Stopping services...$(RESET)'
	docker compose down
	@echo '$(GREEN)✅ Services stopped$(RESET)'

logs: ## Show logs from all services
	docker compose logs -f

status: ## Show status of all services
	@echo '$(BLUE)📊 Service Status:$(RESET)'
	@docker compose ps

# ============================================================================
# BENCHMARK EXECUTION
# ============================================================================

# Default scale factor
SF ?= 1

benchmark: ## Run complete benchmark (all phases). Usage: make benchmark SF=1
	@echo '$(YELLOW)🏁 Starting complete benchmark (SF=$(SF))...$(RESET)'
	@echo '$(BLUE)Phase 1/3: Bronze (data generation)...$(RESET)'
	@$(MAKE) benchmark-bronze SF=$(SF)
	@echo '$(BLUE)Phase 2/3: Silver (lakehouse conversion)...$(RESET)'
	@$(MAKE) benchmark-silver SF=$(SF)
	@echo '$(BLUE)Phase 3/3: Gold (query execution)...$(RESET)'
	@$(MAKE) benchmark-gold SF=$(SF)
	@echo '$(GREEN)✅ Benchmark complete!$(RESET)'

benchmark-bronze: ## Run Bronze phase only (data generation). Usage: make benchmark-bronze SF=1
	@echo '$(YELLOW)📦 Running Bronze phase (SF=$(SF))...$(RESET)'
	docker compose --profile datagen run --rm -e TPCH_SCALE_FACTOR=$(SF) tpchgen
	@echo '$(GREEN)✅ Bronze phase complete!$(RESET)'

benchmark-silver: ## Run Silver phase only (all frameworks). Usage: make benchmark-silver SF=1
	@echo '$(YELLOW)🔄 Running Silver phase (SF=$(SF))...$(RESET)'
	@echo '$(BLUE)Converting to Delta Lake...$(RESET)'
	docker compose exec spark-master bash -c "TPCH_SCALE_FACTOR=$(SF) python3 /opt/spark/jobs/silver/delta/convert_tables.py"
	@echo '$(BLUE)Converting to Iceberg...$(RESET)'
	docker compose exec spark-master bash -c "TPCH_SCALE_FACTOR=$(SF) python3 /opt/spark/jobs/silver/iceberg/convert_tables.py"
	@echo '$(BLUE)Converting to Hudi...$(RESET)'
	docker compose exec spark-master bash -c "TPCH_SCALE_FACTOR=$(SF) python3 /opt/spark/jobs/silver/hudi/convert_tables.py"
	@echo '$(GREEN)✅ Silver phase complete!$(RESET)'

benchmark-gold: ## Run Gold phase only (queries). Usage: make benchmark-gold SF=1
	@echo '$(YELLOW)⚡ Running Gold phase (SF=$(SF))...$(RESET)'
	@echo '$(BLUE)Executing Delta queries...$(RESET)'
	docker compose exec spark-master bash -c "FRAMEWORK=delta TPCH_SCALE_FACTOR=$(SF) python3 /opt/spark/jobs/gold/query_executor.py"
	@echo '$(BLUE)Executing Iceberg queries...$(RESET)'
	docker compose exec spark-master bash -c "FRAMEWORK=iceberg TPCH_SCALE_FACTOR=$(SF) python3 /opt/spark/jobs/gold/query_executor.py"
	@echo '$(BLUE)Executing Hudi queries...$(RESET)'
	docker compose exec spark-master bash -c "FRAMEWORK=hudi TPCH_SCALE_FACTOR=$(SF) python3 /opt/spark/jobs/gold/query_executor.py"
	@echo '$(GREEN)✅ Gold phase complete!$(RESET)'

# ============================================================================
# UTILITIES
# ============================================================================

spark-ui: ## Open Spark Master UI in browser
	@echo '$(BLUE)🌐 Opening Spark UI...$(RESET)'
	@python -m webbrowser http://localhost:8080 2>/dev/null || \
		xdg-open http://localhost:8080 2>/dev/null || \
		open http://localhost:8080 2>/dev/null || \
		echo 'Please open http://localhost:8080 in your browser'

shell-spark: ## Open shell in Spark master container
	docker compose exec spark-master /bin/bash

clean-cache: ## Clean Spark cache and temp files
	@echo '$(YELLOW)🧹 Cleaning cache...$(RESET)'
	docker compose exec spark-master rm -rf /tmp/spark-* 2>/dev/null || true
	docker run --rm -v $(shell pwd):/workspace alpine sh -c "find /workspace -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true"
	@echo '$(GREEN)✅ Cache cleaned$(RESET)'

clean: ## Clean all data and containers
	@echo '$(YELLOW)🧹 Cleaning up...$(RESET)'
	@read -p "This will delete all data and containers. Continue? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker compose down -v; \
		rm -rf $(DATA_DIR)/*; \
		echo '$(GREEN)✅ Cleanup complete$(RESET)'; \
	else \
		echo '$(YELLOW)Cancelled$(RESET)'; \
	fi

clean-results: ## Clean only results directory
	@echo '$(YELLOW)🧹 Cleaning results...$(RESET)'
	rm -rf results/*
	@echo '$(GREEN)✅ Results cleaned$(RESET)'

reset: ## Reset benchmark state
	@echo '$(YELLOW)🔄 Resetting benchmark state...$(RESET)'
	rm -f $(DATA_DIR)/bronze/tpch/_metadata/state.json
	@echo '$(GREEN)✅ State reset$(RESET)'

# ============================================================================
# DEVELOPMENT
# ============================================================================

test: ## Run tests
	@echo '$(YELLOW)🧪 Running tests...$(RESET)'
	pytest tests/ -v
	@echo '$(GREEN)✅ Tests complete$(RESET)'

lint: ## Run linters
	@echo '$(YELLOW)🔍 Running linters...$(RESET)'
	flake8 scripts/ spark_jobs/
	black --check scripts/ spark_jobs/
	@echo '$(GREEN)✅ Linting complete$(RESET)'

format: ## Format code
	@echo '$(YELLOW)✨ Formatting code...$(RESET)'
	black scripts/ spark_jobs/
	@echo '$(GREEN)✅ Formatting complete$(RESET)'

# ============================================================================
# INFORMATION
# ============================================================================

info: ## Show configuration info
	@echo '$(BLUE)ℹ Configuration Information:$(RESET)'
	@echo ''
	@if [ -f .env ]; then \
		echo '$(GREEN)TPCH_SCALE_FACTOR$(RESET):' $$(grep TPCH_SCALE_FACTOR .env | cut -d '=' -f2); \
		echo '$(GREEN)FRAMEWORKS$(RESET):' $$(grep '^FRAMEWORKS=' .env | cut -d '=' -f2); \
		echo '$(GREEN)SPARK_WORKER_CORES$(RESET):' $$(grep SPARK_WORKER_CORES .env | cut -d '=' -f2); \
		echo '$(GREEN)SPARK_WORKER_MEMORY$(RESET):' $$(grep SPARK_WORKER_MEMORY .env | cut -d '=' -f2); \
	else \
		echo '$(YELLOW)No .env file found. Run "make setup" first.$(RESET)'; \
	fi
	@echo ''
	@echo '$(BLUE)Data Directory:$(RESET) $(DATA_DIR)'

version: ## Show versions
	@echo '$(BLUE)📦 Version Information:$(RESET)'
	@docker --version
	@docker compose --version
	@echo ''
