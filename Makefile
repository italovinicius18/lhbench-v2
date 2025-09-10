# LHBench v2 - Lakehouse Performance Benchmark
# Makefile for orchestrating the complete benchmark workflow

# Environment defaults
SCALE_FACTOR ?= 1
DATA_PATH ?= /mnt/c/Users/italo/WSL_DATA/tpcds
MINIO_ENDPOINT ?= http://localhost:9000
MINIO_ACCESS_KEY ?= minioadmin
MINIO_SECRET_KEY ?= minioadmin
BUCKET_NAME ?= lakehouse
ENGINES ?= delta iceberg hudi
BENCHMARK_ITERATIONS ?= 3

# Internal variables
MINIO_CONTAINER_NAME = lhbench-minio
PYTHONPATH := $(shell pwd)
export PYTHONPATH

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[1;33m
BLUE := \033[0;34m
NC := \033[0m # No Color

.PHONY: help setup clean status validate
.PHONY: minio-start minio-stop minio-restart minio-status minio-bucket
.PHONY: data-generate data-regenerate data-clean data-status
.PHONY: benchmark-run benchmark-engines benchmark-queries
.PHONY: view-results results-summary
.PHONY: spark-deps spark-jars spark-clean-jars
.PHONY: all validate test

# Default target
all: help

help: ## Show this help message
	@echo "$(BLUE)LHBench v2 - Lakehouse Performance Benchmark$(NC)"
	@echo "=============================================="
	@echo ""
	@echo "$(YELLOW)Environment Variables:$(NC)"
	@echo "  SCALE_FACTOR     TPC-DS scale factor (default: $(SCALE_FACTOR))"
	@echo "  DATA_PATH        Path for TPC-DS data (default: $(DATA_PATH))"
	@echo "  MINIO_ENDPOINT   MinIO endpoint (default: $(MINIO_ENDPOINT))"
	@echo "  ENGINES          Engines to test (default: $(ENGINES))"
	@echo ""
	@echo "$(YELLOW)Available targets:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

# =============================================================================
# Setup and Validation
# =============================================================================

setup: validate spark-jars minio-start minio-bucket ## Complete environment setup
	@echo "$(GREEN)✓ Environment setup complete!$(NC)"
	@echo "$(YELLOW)Next steps:$(NC)"
	@echo "  make data-generate    # Generate TPC-DS data"
	@echo "  make benchmark-run    # Run complete benchmark"

validate: ## Validate implementation and dependencies
	@echo "$(BLUE)Validating implementation...$(NC)"
	@python scripts/validate_implementation.py
	@echo "$(GREEN)✓ Validation complete$(NC)"

clean: data-clean minio-stop ## Clean all resources
	@echo "$(GREEN)✓ Cleanup complete$(NC)"

status: minio-status data-status ## Show status of all components
	@echo "$(GREEN)✓ Status check complete$(NC)"

# =============================================================================
# MinIO Management
# =============================================================================

minio-start: ## Start MinIO using Docker
	@echo "$(BLUE)Starting MinIO...$(NC)"
	@if [ $$(docker ps -q -f name=$(MINIO_CONTAINER_NAME)) ]; then \
		echo "$(YELLOW)MinIO container already running$(NC)"; \
	elif [ $$(docker ps -aq -f name=$(MINIO_CONTAINER_NAME)) ]; then \
		echo "$(YELLOW)Starting existing MinIO container...$(NC)"; \
		docker start $(MINIO_CONTAINER_NAME); \
		echo "$(GREEN)✓ MinIO container started$(NC)"; \
		echo "$(YELLOW)Waiting for MinIO to be ready...$(NC)"; \
		sleep 5; \
	else \
		docker run -d \
			--name $(MINIO_CONTAINER_NAME) \
			-p 9000:9000 \
			-p 9001:9001 \
			-e MINIO_ROOT_USER=$(MINIO_ACCESS_KEY) \
			-e MINIO_ROOT_PASSWORD=$(MINIO_SECRET_KEY) \
			-v $(DATA_PATH):/data \
			quay.io/minio/minio:latest \
			server /data --console-address ":9001"; \
		echo "$(GREEN)✓ MinIO started$(NC)"; \
		echo "$(YELLOW)Waiting for MinIO to be ready...$(NC)"; \
		sleep 10; \
	fi

minio-stop: ## Stop and remove MinIO container
	@echo "$(BLUE)Stopping MinIO...$(NC)"
	@if [ $$(docker ps -q -f name=$(MINIO_CONTAINER_NAME)) ]; then \
		docker stop $(MINIO_CONTAINER_NAME); \
		docker rm $(MINIO_CONTAINER_NAME); \
		echo "$(GREEN)✓ MinIO stopped and removed$(NC)"; \
	elif [ $$(docker ps -aq -f name=$(MINIO_CONTAINER_NAME)) ]; then \
		docker rm $(MINIO_CONTAINER_NAME); \
		echo "$(GREEN)✓ MinIO container removed$(NC)"; \
	else \
		echo "$(YELLOW)No MinIO container found$(NC)"; \
	fi

minio-restart: minio-stop minio-start ## Restart MinIO container
	@echo "$(GREEN)✓ MinIO restarted$(NC)"

minio-status: ## Check MinIO status
	@echo "$(BLUE)MinIO Status:$(NC)"
	@if [ $$(docker ps -q -f name=$(MINIO_CONTAINER_NAME)) ]; then \
		echo "$(GREEN)✓ MinIO container running$(NC)"; \
		echo "  Web UI: http://localhost:9001 ($(MINIO_ACCESS_KEY)/$(MINIO_SECRET_KEY))"; \
		echo "  API: $(MINIO_ENDPOINT)"; \
	else \
		echo "$(RED)✗ MinIO container not running$(NC)"; \
	fi

minio-bucket: ## Create MinIO bucket
	@echo "$(BLUE)Creating MinIO bucket...$(NC)"
	@python scripts/create_bucket.py

# =============================================================================
# Data Management
# =============================================================================

data-generate: minio-start minio-bucket ## Generate TPC-DS data directly to MinIO (skip if exists)
	@echo "$(BLUE)Generating TPC-DS data directly to MinIO (scale factor: $(SCALE_FACTOR))...$(NC)"
	@python scripts/generate_data.py $(SCALE_FACTOR)
	@echo "$(GREEN)✓ Data generation complete$(NC)"
	@make data-status

data-regenerate: minio-start minio-bucket ## Force regenerate TPC-DS data directly to MinIO
	@echo "$(BLUE)Force regenerating TPC-DS data directly to MinIO (scale factor: $(SCALE_FACTOR))...$(NC)"
	@FORCE_REGENERATE=true python scripts/generate_data.py $(SCALE_FACTOR)
	@echo "$(GREEN)✓ Data regeneration complete$(NC)"
	@make data-status

data-clean: ## Clean generated data from MinIO
	@echo "$(BLUE)Cleaning TPC-DS data from MinIO...$(NC)"
	@python -c "import boto3, os; s3 = boto3.client('s3', endpoint_url='$(MINIO_ENDPOINT)', aws_access_key_id='$(MINIO_ACCESS_KEY)', aws_secret_access_key='$(MINIO_SECRET_KEY)'); bucket = 'lakehouse'; resp = s3.list_objects_v2(Bucket=bucket, Prefix='tpcds-data/'); objs = [{'Key': obj['Key']} for obj in resp.get('Contents', [])]; s3.delete_objects(Bucket=bucket, Delete={'Objects': objs}) if objs else None; print(f'✓ Deleted {len(objs)} objects from MinIO')" || echo "$(YELLOW)⚠ MinIO cleanup failed$(NC)"
	@echo "$(GREEN)✓ MinIO data cleanup complete$(NC)"

data-status: ## Check data status
	@echo "$(BLUE)Data Status:$(NC)"
	@echo "$(YELLOW)Local Data:$(NC)"
	@if [ -d "$(DATA_PATH)" ]; then \
		file_count=$$(ls -1 $(DATA_PATH)/*.parquet 2>/dev/null | wc -l); \
		total_size=$$(du -sh $(DATA_PATH) 2>/dev/null | cut -f1); \
		echo "$(GREEN)✓ Data directory: $(DATA_PATH)$(NC)"; \
		echo "  Files: $$file_count parquet files"; \
		echo "  Size: $$total_size"; \
		if [ $$file_count -eq 24 ]; then \
			echo "$(GREEN)✓ All 24 TPC-DS tables present$(NC)"; \
		else \
			echo "$(YELLOW)⚠ Expected 24 tables, found $$file_count$(NC)"; \
		fi; \
	else \
		echo "$(RED)✗ Data directory not found$(NC)"; \
	fi
	@echo ""
	@echo "$(YELLOW)MinIO Data:$(NC)"
	@python scripts/check_minio_data.py || echo "$(YELLOW)⚠ MinIO check failed$(NC)"

# =============================================================================
# Spark Dependencies Management
# =============================================================================

spark-deps: spark-jars ## Download all Spark dependencies (alias for spark-jars)

spark-jars: ## Download Spark JAR dependencies for lakehouse engines
	@echo "$(BLUE)Downloading Spark JAR dependencies...$(NC)"
	@export SPARK_JARS_DIR=./jars && ./scripts/download_spark_jars.sh ./jars
	@echo "$(GREEN)✓ Spark JARs download complete$(NC)"
	@echo "$(YELLOW)Setting environment variable for this session:$(NC)"
	@echo "  export SPARK_JARS_DIR=./jars"

spark-clean-jars: ## Remove downloaded JAR files
	@echo "$(BLUE)Cleaning Spark JAR dependencies...$(NC)"
	@if [ -d "./jars" ]; then \
		rm -rf ./jars/*.jar; \
		echo "$(GREEN)✓ JAR files removed$(NC)"; \
	else \
		echo "$(YELLOW)No JAR directory found$(NC)"; \
	fi

# =============================================================================
# =============================================================================
# Benchmark Execution
# =============================================================================

benchmark-run: benchmark-engines benchmark-queries ## Run complete benchmark suite
	@echo "$(GREEN)✓ Complete benchmark finished!$(NC)"
	@echo "$(YELLOW)Check results in: results/$(NC)"

benchmark-engines: ## Test engines implementation
	@echo "$(BLUE)Running engines benchmark...$(NC)"
	@mkdir -p results
	@python examples/engines_example.py \
		--scale-factor $(SCALE_FACTOR) \
		--data-path $(DATA_PATH) \
		--engines $(ENGINES) \
		--minio-endpoint $(MINIO_ENDPOINT) \
		--skip-data-generation \
		--test-queries \
		2>&1 | tee results/engines_benchmark_$$(date +%Y%m%d_%H%M%S).log
	@echo "$(GREEN)✓ Engines benchmark complete$(NC)"

benchmark-queries: ## Run query performance tests
	@echo "$(BLUE)Running query benchmark...$(NC)"
	@mkdir -p results
	@python scripts/run_queries.py \
		--scale-factor $(SCALE_FACTOR) \
		--data-path $(DATA_PATH) \
		--engines $(ENGINES) \
		--minio-endpoint $(MINIO_ENDPOINT) \
		--iterations $(BENCHMARK_ITERATIONS) \
		--output results/query_results_$$(date +%Y%m%d_%H%M%S).json \
		2>&1 | tee results/query_benchmark_$$(date +%Y%m%d_%H%M%S).log
	@echo "$(GREEN)✓ Query benchmark complete$(NC)"

# =============================================================================
# Testing and Development
# =============================================================================

test: ## Run all tests
	@echo "$(BLUE)Running tests...$(NC)"
	@python scripts/test_engines.py
	@echo "$(GREEN)✓ Tests complete$(NC)"

test-integration: ## Run integration tests with real data
	@echo "$(BLUE)Running integration tests...$(NC)"
	@python scripts/test_integration.py \
		--data-path $(DATA_PATH) \
		--minio-endpoint $(MINIO_ENDPOINT)
	@echo "$(GREEN)✓ Integration tests complete$(NC)"

# =============================================================================
# Results and Analysis
# =============================================================================

view-results: ## View latest benchmark results
	@echo "$(BLUE)Viewing latest benchmark results...$(NC)"
	@latest_file=$$(find results -name "query_results_*.json" 2>/dev/null | sort | tail -1); \
	if [ -n "$$latest_file" ]; then \
		echo "Latest results file: $$latest_file"; \
		python scripts/view_results.py "$$latest_file" --format table; \
	else \
		echo "$(RED)No results found. Run 'make benchmark-queries' first.$(NC)"; \
	fi

view-results-plot: ## Create plots from latest results
	@echo "$(BLUE)Creating plots from latest results...$(NC)"
	@mkdir -p results/plots
	@latest_file=$$(find results -name "query_results_*.json" 2>/dev/null | sort | tail -1); \
	if [ -n "$$latest_file" ]; then \
		echo "Latest results file: $$latest_file"; \
		output_plot="results/plots/benchmark_$$(date +%Y%m%d_%H%M%S).png"; \
		python scripts/view_results.py "$$latest_file" --format plot --output "$$output_plot"; \
		echo "Plot saved to: $$output_plot"; \
	else \
		echo "$(RED)No results found. Run 'make benchmark-queries' first.$(NC)"; \
	fi

clean-results: ## Clean benchmark results
	@echo "$(BLUE)Cleaning benchmark results...$(NC)"
	@rm -rf results/
	@echo "$(GREEN)✓ Results cleaned$(NC)"

# =============================================================================
# Convenience Targets
# =============================================================================

quick-start: minio-start data-generate benchmark-engines ## Quick start: MinIO + data + engines test
	@echo "$(GREEN)✓ Quick start complete!$(NC)"

dev-setup: minio-start minio-bucket ## Minimal setup for development
	@echo "$(GREEN)✓ Development setup complete!$(NC)"

logs: ## Show recent logs
	@echo "$(BLUE)Recent benchmark logs:$(NC)"
	@if [ -d "results" ]; then \
		ls -lt results/*.log | head -5; \
	else \
		echo "$(YELLOW)No logs found$(NC)"; \
	fi

# =============================================================================
# Environment Info
# =============================================================================

env: ## Show environment information
	@echo "$(BLUE)Environment Information:$(NC)"
	@echo "  SCALE_FACTOR:     $(SCALE_FACTOR)"
	@echo "  DATA_PATH:        $(DATA_PATH)"
	@echo "  MINIO_ENDPOINT:   $(MINIO_ENDPOINT)"
	@echo "  BUCKET_NAME:      $(BUCKET_NAME)"
	@echo "  ENGINES:          $(ENGINES)"
	@echo "  SPARK_NAMESPACE:  $(SPARK_NAMESPACE)"
	@echo "  PYTHONPATH:       $(PYTHONPATH)"

# =============================================================================
# Advanced Targets
# =============================================================================

scale-test: ## Run benchmark with multiple scale factors
	@echo "$(BLUE)Running multi-scale benchmark...$(NC)"
	@for scale in 1 2 5; do \
		echo "$(YELLOW)Testing scale factor: $$scale$(NC)"; \
		$(MAKE) data-clean data-generate SCALE_FACTOR=$$scale; \
		$(MAKE) benchmark-engines SCALE_FACTOR=$$scale; \
	done
	@echo "$(GREEN)✓ Multi-scale benchmark complete$(NC)"

perf-profile: ## Run performance profiling
	@echo "$(BLUE)Running performance profiling...$(NC)"
	@mkdir -p results/profiles
	@python scripts/profile_performance.py \
		--data-path $(DATA_PATH) \
		--engines $(ENGINES) \
		--output results/profiles/profile_$$(date +%Y%m%d_%H%M%S).json
	@echo "$(GREEN)✓ Performance profiling complete$(NC)"

# =============================================================================
# Documentation
# =============================================================================

docs: ## Generate documentation
	@echo "$(BLUE)Generating documentation...$(NC)"
	@mkdir -p docs/generated
	@python scripts/generate_docs.py
	@echo "$(GREEN)✓ Documentation generated$(NC)"

# Error handling
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules
