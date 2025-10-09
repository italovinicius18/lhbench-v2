.PHONY: help all build up down clean benchmark results

.DEFAULT_GOAL := all

# Colors for output
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
BLUE   := $(shell tput -Txterm setaf 4)
RESET  := $(shell tput -Txterm sgr0)

# Configuration from .env
-include .env
export

# Default values
SF ?= $(TPCDS_SCALE_FACTOR)
TIER ?= $(TPCDS_QUERY_TIER)
DATA_DIR := /mnt/c/Users/italo/WSL_DATA/lakehouse-data

help: ## Show this help message
	@echo '$(BLUE)TPC-DS Lakehouse Benchmark$(RESET)'
	@echo ''
	@echo '$(YELLOW)Quick Start:$(RESET)'
	@echo '  make          - Build and run complete benchmark'
	@echo '  make SF=10    - Run with scale factor 10'
	@echo ''
	@echo '$(YELLOW)Available Commands:$(RESET)'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-15s$(RESET) %s\n", $$1, $$2}'

# ============================================================================
# MAIN AUTOMATION - Run everything with one command
# ============================================================================

all: setup build benchmark results ## Complete automated benchmark (default)

benchmark: up bronze silver gold ## Run complete TPC-DS benchmark (all phases)
	@echo '$(GREEN)✅ Benchmark complete! Check results/metrics/ for results$(RESET)'

# ============================================================================
# SETUP
# ============================================================================

setup: ## Setup environment (.env and directories)
	@echo '$(YELLOW)🔧 Setting up environment...$(RESET)'
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo '$(GREEN)✓ Created .env file$(RESET)'; \
	fi
	@mkdir -p $(DATA_DIR)/{bronze/tpcds,silver/{delta,iceberg,hudi},gold/metrics}
	@mkdir -p results/metrics
	@echo '$(GREEN)✓ Directories created$(RESET)'

# ============================================================================
# DOCKER MANAGEMENT
# ============================================================================

build: ## Build Docker images
	@echo '$(YELLOW)🐳 Building Docker images...$(RESET)'
	@docker compose build
	@echo '$(GREEN)✅ Images built$(RESET)'

up: ## Start Spark cluster
	@echo '$(YELLOW)🚀 Starting Spark cluster...$(RESET)'
	@docker compose up -d
	@echo '$(YELLOW)⏳ Waiting for cluster to be ready...$(RESET)'
	@sleep 10
	@echo '$(GREEN)✅ Cluster ready$(RESET)'
	@echo '$(BLUE)Spark UI: http://localhost:8080$(RESET)'

down: ## Stop all services
	@docker compose down
	@echo '$(GREEN)✅ Services stopped$(RESET)'

restart: down up ## Restart cluster

# ============================================================================
# BENCHMARK PHASES
# ============================================================================

bronze: ## Phase 1: Generate TPC-DS data
	@echo '$(BLUE)📦 Phase 1/3: Generating TPC-DS data (SF=$(SF))...$(RESET)'
	@docker compose exec spark-master bash -c "TPCDS_SCALE_FACTOR=$(SF) python3 /opt/spark/jobs/bronze/generate_tpcds_data_duckdb.py"
	@echo '$(GREEN)✓ Bronze phase complete$(RESET)'

silver: ## Phase 2: Convert to lakehouse formats (Delta, Iceberg, Hudi)
	@echo '$(BLUE)🔄 Phase 2/3: Converting to lakehouse formats (SF=$(SF))...$(RESET)'
	@echo '$(YELLOW)Converting to Delta Lake...$(RESET)'
	@docker compose exec spark-master bash -c "TPCDS_SCALE_FACTOR=$(SF) python3 /opt/spark/jobs/silver/delta/convert_tables_tpcds.py"
	@echo '$(YELLOW)Converting to Iceberg...$(RESET)'
	@docker compose exec spark-master bash -c "TPCDS_SCALE_FACTOR=$(SF) python3 /opt/spark/jobs/silver/iceberg/convert_tables_tpcds.py"
	@echo '$(YELLOW)Converting to Hudi...$(RESET)'
	@docker compose exec spark-master bash -c "TPCDS_SCALE_FACTOR=$(SF) python3 /opt/spark/jobs/silver/hudi/convert_tables_tpcds.py"
	@echo '$(GREEN)✓ Silver phase complete$(RESET)'

gold: ## Phase 3: Run TPC-DS queries on all formats
	@echo '$(BLUE)⚡ Phase 3/3: Running TPC-DS queries (SF=$(SF), Tier=$(TIER))...$(RESET)'
	@echo '$(YELLOW)Executing Delta queries...$(RESET)'
	@docker compose exec spark-master bash -c "FRAMEWORK=delta TPCDS_SCALE_FACTOR=$(SF) TIER=$(TIER) python3 /opt/spark/jobs/gold/tpcds_query_executor.py"
	@echo '$(YELLOW)Executing Iceberg queries...$(RESET)'
	@docker compose exec spark-master bash -c "FRAMEWORK=iceberg TPCDS_SCALE_FACTOR=$(SF) TIER=$(TIER) python3 /opt/spark/jobs/gold/tpcds_query_executor.py"
	@echo '$(YELLOW)Executing Hudi queries...$(RESET)'
	@docker compose exec spark-master bash -c "FRAMEWORK=hudi TPCDS_SCALE_FACTOR=$(SF) TIER=$(TIER) python3 /opt/spark/jobs/gold/tpcds_query_executor.py"
	@echo '$(GREEN)✓ Gold phase complete$(RESET)'

# ============================================================================
# TIER-SPECIFIC QUERIES
# ============================================================================

tier1: ## Run tier1 queries (10 essential)
	@$(MAKE) gold TIER=tier1

tier2: ## Run tier2 queries (15 advanced)
	@$(MAKE) gold TIER=tier2

tier3: ## Run tier3 queries (10 stress)
	@$(MAKE) gold TIER=tier3

lhbench: ## Run LHBench queries (5 refresh test)
	@$(MAKE) gold TIER=lhbench

# ============================================================================
# RESULTS & UTILITIES
# ============================================================================

results: ## Copy results from container to local
	@echo '$(YELLOW)📊 Copying results...$(RESET)'
	@docker compose cp spark-master:/data/gold/metrics/. results/metrics/ 2>/dev/null || true
	@echo '$(GREEN)✓ Results copied to results/metrics/$(RESET)'

logs: ## Show Spark cluster logs
	@docker compose logs -f

status: ## Show cluster status
	@docker compose ps

shell: ## Open shell in Spark master
	@docker compose exec spark-master /bin/bash

ui: ## Open Spark UI in browser
	@python3 -m webbrowser http://localhost:8080 2>/dev/null || \
		xdg-open http://localhost:8080 2>/dev/null || \
		open http://localhost:8080 2>/dev/null || \
		echo '$(BLUE)Spark UI: http://localhost:8080$(RESET)'

# ============================================================================
# CLEANUP
# ============================================================================

clean: ## Stop services and remove containers
	@docker compose down -v
	@echo '$(GREEN)✅ Cleanup complete$(RESET)'

clean-data: ## Remove all generated data (WARNING: destructive)
	@read -p "Delete all data in $(DATA_DIR)? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		rm -rf $(DATA_DIR)/{bronze,silver,gold}/*; \
		echo '$(GREEN)✅ Data deleted$(RESET)'; \
	fi

clean-results: ## Remove local results
	@rm -rf results/metrics/*
	@echo '$(GREEN)✅ Results cleaned$(RESET)'

clean-all: clean clean-data clean-results ## Complete cleanup
	@echo '$(GREEN)✅ Complete cleanup done$(RESET)'
