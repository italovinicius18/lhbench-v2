# Getting Started with Lakehouse Benchmark

## 🚀 Quick Start (3 Commands)

```bash
# 1. Setup
make setup

# 2. Edit configuration (use SF1 for testing)
nano .env

# 3. Run first benchmark
make build && make up && make benchmark-bronze
```

## 📋 What You Get

### Bronze Phase (Fully Working)
✅ High-performance TPC-H data generation using tpchgen-cli
✅ 10x faster than DuckDB
✅ Idempotent execution (smart caching)
✅ Metadata tracking

### Infrastructure (Ready)
✅ Spark 3.5 cluster (1 master + 2 workers)
✅ Apache Iceberg JARs loaded
✅ Delta Lake JARs loaded
✅ Apache Hudi JARs loaded
✅ Docker Compose orchestration
✅ Health checks & monitoring

### Developer Experience
✅ Makefile with 30+ commands
✅ Interactive shell script (./run_benchmark.sh)
✅ Comprehensive documentation
✅ Structured logging
✅ Metrics collection

## 🎯 Next: Implement Silver Phase

The Bronze phase is complete. To finish the benchmark, implement:

1. **Silver Phase** (spark_jobs/silver/)
   - Iceberg conversion
   - Delta conversion  
   - Hudi conversion

2. **Gold Phase** (spark_jobs/gold/)
   - TPC-H query execution
   - Performance metrics

3. **Reports** (scripts/phases/report_phase.py)
   - Comparative analysis

See PROJECT_SUMMARY.md for details.

## 📚 Documentation

- [README.md](README.md) - Full documentation
- [QUICKSTART.md](docs/QUICKSTART.md) - Step-by-step guide
- [PROJECT_SUMMARY.md](PROJECT_SUMMARY.md) - Implementation status

## 🏁 Test It Now

```bash
# Setup
make setup

# Edit .env - set TPCH_SCALE_FACTOR=1
nano .env

# Build and start
make build
make up

# Generate test data (should take ~2 seconds)
make benchmark-bronze

# Check results
ls -lh /mnt/c/Users/italo/WSL_DATA/lakehouse-data/bronze/tpch/sf1/

# View Spark UI
make spark-ui
```

Happy benchmarking! 🚀
