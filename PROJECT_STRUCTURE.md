# TPC-DS Lakehouse Benchmark - Project Structure

## Quick Reference

```
lhbench-v2/
├── Makefile                           # Main automation (run `make` to start)
├── docker-compose.yml                 # Spark cluster definition
├── .env                              # Configuration (edit this!)
├── .env.example                      # Configuration template
├── README.md                         # Documentation
│
├── configs/
│   ├── queries/tpcds/                # 99 TPC-DS queries (q01.sql - q99.sql)
│   │   └── README.md                 # Query documentation
│   ├── tpcds_schemas.py              # Table schemas
│   └── tpcds_tables.json             # Table metadata
│
├── docker/
│   ├── Dockerfile.spark-master       # Master node image
│   ├── Dockerfile.spark-worker       # Worker node image
│   └── worker-entrypoint.sh          # Worker startup script
│
├── spark_jobs/
│   ├── bronze/
│   │   └── generate_tpcds_data_duckdb.py    # Phase 1: Data generation
│   │
│   ├── silver/
│   │   ├── delta/convert_tables_tpcds.py    # Phase 2: Delta conversion
│   │   ├── iceberg/convert_tables_tpcds.py  # Phase 2: Iceberg conversion
│   │   └── hudi/convert_tables_tpcds.py     # Phase 2: Hudi conversion
│   │
│   ├── gold/
│   │   └── tpcds_query_executor.py          # Phase 3: Query execution
│   │
│   └── common/
│       ├── spark_session_builder.py         # Spark session factory
│       └── cache_manager.py                 # Cache utilities
│
├── scripts/utils/
│   ├── logger.py                            # Logging utilities
│   ├── metrics_collector.py                 # Metrics helpers
│   └── tpcds_query_processor.py             # Query processor
│
└── results/metrics/                         # Benchmark results (JSON)
```

## Key Files

### Configuration
- **`.env`** - Main configuration file (TPCDS_SCALE_FACTOR, TPCDS_QUERY_TIER)
- **`Makefile`** - Automation commands

### Benchmark Phases
1. **Bronze** - `spark_jobs/bronze/generate_tpcds_data_duckdb.py`
2. **Silver** - `spark_jobs/silver/{delta,iceberg,hudi}/convert_tables_tpcds.py`
3. **Gold** - `spark_jobs/gold/tpcds_query_executor.py`

### Data Flow
```
DuckDB TPC-DS Generator
         ↓
Bronze (Parquet)
         ↓
Silver (Delta/Iceberg/Hudi)
         ↓
Gold (Query Results)
```

## File Count
- Python files: 12
- Configuration files: 5
- Docker files: 3
- Query files: 99
- Total LOC: ~3000

## Removed Files
All TPC-H references have been removed:
- ❌ spark_jobs/gold/tpch_queries.py
- ❌ spark_jobs/bronze/generate_tpch_data_duckdb.py
- ❌ spark_jobs/silver/{delta,iceberg,hudi}/convert_tables.py
- ❌ docker/Dockerfile.tpchgen
- ❌ scripts/phases/* (all phase scripts)
- ❌ scripts/generate_refresh_data.py
- ❌ scripts/utils/config_loader.py
- ❌ docs/* (old documentation)
