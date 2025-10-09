# TPC-DS Lakehouse Benchmark

Automated benchmark comparing Apache Spark with three lakehouse formats: **Delta Lake**, **Apache Iceberg**, and **Apache Hudi** using the TPC-DS benchmark (99 queries, 24 tables).

## Quick Start

```bash
# Run complete benchmark with one command
make

# Or run with custom scale factor
make SF=10
```

That's it! The Makefile will automatically:
1. Setup environment and directories
2. Build Docker images
3. Start Spark cluster
4. Generate TPC-DS data
5. Convert to all lakehouse formats
6. Run all 99 queries on each format
7. Copy results to `results/metrics/`

## Requirements

- Docker & Docker Compose
- 16GB+ RAM recommended
- 50GB+ disk space

## Configuration

Edit `.env` to customize:

```bash
# Scale factor (1=3GB, 10=30GB, 100=300GB)
TPCDS_SCALE_FACTOR=1

# Query tier (tier1, tier2, tier3, lhbench, all)
TPCDS_QUERY_TIER=all

# Spark cluster resources
SPARK_WORKER_CORES=5
SPARK_WORKER_MEMORY=3g
```

## Available Commands

```bash
make              # Run complete automated benchmark
make help         # Show all available commands

# Individual phases
make bronze       # Generate TPC-DS data
make silver       # Convert to lakehouse formats
make gold         # Run queries

# Query tiers
make tier1        # Run 10 essential queries
make tier2        # Run 15 advanced queries
make tier3        # Run 10 stress queries
make lhbench      # Run 5 refresh test queries

# Utilities
make status       # Show cluster status
make logs         # Show logs
make results      # Copy results from container
make ui           # Open Spark UI (http://localhost:8080)
make shell        # Open shell in Spark master

# Cleanup
make clean        # Stop services
make clean-all    # Complete cleanup (data + containers)
```

## Project Structure

```
lhbench-v2/
├── Makefile                    # One-command automation
├── docker-compose.yml          # Spark cluster (1 master + 2 workers)
├── .env.example               # Configuration template
├── configs/
│   └── queries/tpcds/         # 99 TPC-DS queries (DuckDB format)
├── spark_jobs/
│   ├── bronze/                # Data generation
│   ├── silver/                # Format conversion (Delta/Iceberg/Hudi)
│   └── gold/                  # Query execution
└── results/metrics/           # Benchmark results (JSON)
```

## Benchmark Phases

### Phase 1: Bronze (Data Generation)
Generates TPC-DS data using DuckDB at specified scale factor:
- 24 tables (7 fact + 17 dimension)
- Parquet format with Snappy compression

### Phase 2: Silver (Lakehouse Conversion)
Converts bronze Parquet to lakehouse formats:
- **Delta Lake 3.0.0** - ACID transactions, time travel
- **Apache Iceberg 1.4.3** - Hidden partitioning, schema evolution
- **Apache Hudi 1.0.2** - Upserts, incremental processing

### Phase 3: Gold (Query Execution)
Runs TPC-DS queries on each format and collects metrics:
- Query execution time
- Success/failure status
- Total queries executed
- Framework-specific performance

## Results

Results are saved to `results/metrics/` in JSON format:

```json
{
  "framework": "delta",
  "scale_factor": 1,
  "total_queries": 99,
  "successful_queries": 99,
  "total_duration_seconds": 447.34,
  "average_query_time_seconds": 4.52,
  "queries": { ... }
}
```

## Scale Factors

| SF | Data Size | Tables Size | Recommended RAM |
|----|-----------|-------------|-----------------|
| 1  | ~3 GB     | 24 tables   | 8 GB            |
| 10 | ~30 GB    | 24 tables   | 16 GB           |
| 100| ~300 GB   | 24 tables   | 32 GB+          |

## Query Tiers

- **tier1**: 10 essential queries (Q1,Q3,Q7,Q11,Q19,Q25,Q42,Q52,Q55,Q98)
- **tier2**: 15 advanced queries
- **tier3**: 10 stress queries
- **lhbench**: 5 refresh test queries (Q3,Q9,Q34,Q42,Q59)
- **all**: All 99 queries

## Architecture

- **Spark 3.5.0** with Python 3.10
- **1 Master + 2 Workers** (5 cores, 3GB each)
- **DuckDB** for TPC-DS data generation
- **Delta Lake 3.0.0**, **Iceberg 1.4.3**, **Hudi 1.0.2**

## License

MIT
