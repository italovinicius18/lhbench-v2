# Lakehouse Benchmark

High-performance benchmark system comparing **Apache Iceberg**, **Delta Lake**, and **Apache Hudi** using TPC-H data.

## 🎯 Features

- **Simple Architecture**: Docker Compose orchestration (no Airflow complexity)
- **High-Performance Data Generation**: Using [tpchgen-cli](https://github.com/clflushopt/tpchgen-rs) (10x faster than alternatives)
- **Configurable via .env**: All settings in one centralized configuration file
- **Idempotent Execution**: Smart caching prevents unnecessary regeneration
- **Medallion Architecture**: Bronze → Silver → Gold data flow
- **Comprehensive Metrics**: JSON-based performance tracking
- **Local Storage**: Direct filesystem access (no S3/MinIO overhead)

## 📋 Prerequisites

- **Docker** & **Docker Compose** installed
- **WSL2** (Windows) or native Linux/macOS
- **Minimum 16GB RAM** recommended
- **Disk Space**:
  - SF1: ~3.6GB
  - SF10: ~36GB
  - SF100: ~360GB

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Clone repository
git clone <repository-url>
cd lhbench-v2

# Create configuration
make setup

# Edit .env with your settings
nano .env
```

### 2. Configure Benchmark

Edit `.env` file:

```bash
# Scale Factor (1=1GB, 10=10GB, 100=100GB)
TPCH_SCALE_FACTOR=10

# Frameworks to test
FRAMEWORKS=iceberg,delta,hudi

# Spark cluster resources
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=4g
```

### 3. Build & Start

```bash
# Build Docker images
make build

# Start Spark cluster
make up

# Verify services are running
make status
```

### 4. Run Benchmark

```bash
# Run complete benchmark (all phases)
make benchmark

# Or run phases individually:
make benchmark-bronze  # Generate TPC-H data
make benchmark-silver  # Convert to frameworks
make benchmark-gold    # Run queries
```

## 📊 Architecture

### Data Flow

```
Bronze (Parquet)
    ↓
Silver (Iceberg, Delta, Hudi)
    ↓
Gold (Metrics & Reports)
```

### Directory Structure

```
/mnt/c/Users/italo/WSL_DATA/lakehouse-data/
├── bronze/
│   └── tpch/
│       ├── _metadata/          # Generation metadata & state
│       ├── sf1/                # Scale Factor 1 (1GB)
│       ├── sf10/               # Scale Factor 10 (10GB)
│       └── sf100/              # Scale Factor 100 (100GB)
├── silver/
│   ├── iceberg/                # Iceberg tables
│   ├── delta/                  # Delta Lake tables
│   └── hudi/                   # Hudi tables
└── gold/
    ├── metrics/                # Performance metrics
    ├── reports/                # Comparative reports
    └── logs/                   # Execution logs
```

## 🛠️ Commands Reference

### Essential Commands

```bash
make help              # Show all commands
make setup             # Initial setup
make build             # Build Docker images
make up                # Start services
make down              # Stop services
make benchmark         # Run full benchmark
```

### Phase-Specific Commands

```bash
make benchmark-bronze  # Generate TPC-H data only
make benchmark-silver  # Convert to frameworks only
make benchmark-gold    # Run queries only
```

### Data Generation

```bash
make generate-sf1      # Generate 1GB dataset
make generate-sf10     # Generate 10GB dataset
make generate-sf100    # Generate 100GB dataset
```

### Utilities

```bash
make logs              # View all logs
make status            # Show service status
make spark-ui          # Open Spark UI (http://localhost:8080)
make clean-cache       # Clean Spark cache
make reset             # Reset benchmark state
make clean             # Clean all data & containers
```

## 📈 TPC-H Data Generation Performance

Using `tpchgen-cli` (Rust-based, high-performance):

| Scale Factor | Size | Generation Time* |
|-------------|------|------------------|
| SF1         | 3.6GB | ~2 seconds      |
| SF10        | 36GB  | ~10 seconds     |
| SF100       | 360GB | ~1.5 minutes    |
| SF1000      | 3.6TB | ~10 minutes     |

*On modern laptop (M3 Max or equivalent)

## 🔧 Configuration Guide

### Key Environment Variables

#### Benchmark Settings
```bash
BENCHMARK_NAME=lakehouse-comparison
TPCH_SCALE_FACTOR=10
FRAMEWORKS=iceberg,delta,hudi
FORCE_REGENERATE=false  # Skip regeneration if data exists
```

#### Spark Cluster
```bash
SPARK_WORKER_COUNT=2
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=4g
SPARK_EXECUTOR_CORES=2
SPARK_EXECUTOR_MEMORY=4g
```

#### Framework-Specific
```bash
# Iceberg
ICEBERG_ENABLED=true
ICEBERG_FILE_FORMAT=parquet
ICEBERG_COMPRESSION=snappy

# Delta Lake
DELTA_ENABLED=true
DELTA_AUTO_OPTIMIZE=true
DELTA_AUTO_COMPACT=true

# Hudi
HUDI_ENABLED=true
HUDI_TABLE_TYPE=COPY_ON_WRITE
HUDI_CLUSTERING_ENABLED=true
```

## 📊 Monitoring

### Spark Web UIs

- **Spark Master**: http://localhost:8080
- **Worker 1**: http://localhost:8081
- **Worker 2**: http://localhost:8082

### Logs

```bash
# All logs
make logs

# Orchestrator only
make logs-orchestrator

# Spark master
docker-compose logs -f spark-master
```

### Metrics

Metrics are saved to:
- `results/metrics/` - Execution metrics (JSON)
- `results/logs/` - Detailed logs
- `/data/gold/metrics/` - Benchmark metrics

## 🐛 Troubleshooting

### Services won't start

```bash
# Check status
make status

# View logs
make logs

# Restart services
make down && make up
```

### Out of memory

```bash
# Increase worker memory in .env
SPARK_WORKER_MEMORY=8g
SPARK_EXECUTOR_MEMORY=8g

# Rebuild and restart
make down && make up
```

### Data generation fails

```bash
# Check tpchgen container
docker-compose --profile datagen run --rm tpchgen tpchgen-cli --version

# Regenerate manually
docker-compose --profile datagen run --rm tpchgen \
  tpchgen-cli -s 10 --format=parquet --output-dir=/data/bronze/tpch/sf10
```

### Reset everything

```bash
# Reset state (keeps data)
make reset

# Clean all data
make clean

# Fresh start
make setup && make build && make up
```

## 📚 Project Structure

```
lakehouse-benchmark/
├── docker/                 # Dockerfiles
│   ├── Dockerfile.tpchgen
│   ├── Dockerfile.spark-master
│   ├── Dockerfile.spark-worker
│   └── Dockerfile.orchestrator
├── scripts/                # Python orchestration scripts
│   ├── phases/            # Benchmark phases
│   ├── orchestrator/      # State & task management
│   └── utils/             # Utilities
├── spark_jobs/            # Spark jobs
│   ├── bronze/            # Data generation
│   ├── silver/            # Framework conversions
│   ├── gold/              # Query execution
│   └── common/            # Shared utilities
├── configs/               # Configuration files
├── tests/                 # Test suite
├── docker-compose.yml     # Service orchestration
├── .env.example           # Configuration template
├── Makefile              # Command shortcuts
└── README.md             # This file
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `make test`
5. Submit a pull request

## 📝 License

[Your License]

## 🙏 Acknowledgments

- **tpchgen-cli**: High-performance TPC-H generator by [@clflushopt](https://github.com/clflushopt/tpchgen-rs)
- **Apache Iceberg, Delta Lake, Apache Hudi**: Lakehouse frameworks
- **Apache Spark**: Processing engine

## 📧 Support

- Issues: [GitHub Issues](your-repo-url/issues)
- Discussions: [GitHub Discussions](your-repo-url/discussions)

---

**Built with ❤️ for the lakehouse community**
