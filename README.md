# LHBench v2 - Lakehouse Performance Benchmark

A comprehensive benchmark suite for evaluating Apache Spark lakehouse engines (Delta Lake, Apache Iceberg, and Apache Hudi) using TPC-DS queries with DuckDB-generated data.

## 🚀 Quick Start

```bash
# 1. Clone and setup
git clone <repository-url>
cd lhbench-v2

# 2. Install dependencies and download JARs
pip install -r requirements.txt
make spark-jars

# 3. Start MinIO and generate TPC-DS data
make minio-start
SCALE_FACTOR=10 make data-generate

# 4. Run complete benchmark with all 99 TPC-DS queries
SCALE_FACTOR=10 make benchmark-run

# 5. View results
make view-results
```

## 📋 Prerequisites

- **Python 3.8+** with pip
- **Java 8 or 11** (for Apache Spark)
- **Docker** (for MinIO storage)
- **Make** (for workflow automation)
- **8GB+ RAM** recommended

### Installation Commands

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install python3 python3-pip python3-venv openjdk-11-jdk docker.io make

# Add user to docker group
sudo usermod -aG docker $USER
# Logout and login again
```

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   DuckDB        │    │   Apache Spark  │    │     MinIO       │
│  TPC-DS Data    │───▶│   + Lakehouse   │───▶│   S3 Storage    │
│   Generator     │    │    Engines      │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │  Delta Lake     │
                       │  Apache Iceberg │
                       │  Apache Hudi    │
                       └─────────────────┘
```

### Data Flow

1. **DuckDB** generates TPC-DS data (24 tables) directly to MinIO in Parquet format
2. **Apache Spark** reads Parquet data and converts to engine-specific formats
3. **Lakehouse Engines** process queries using their native table formats
4. **Performance metrics** are collected and stored in JSON results

### Supported Engines

- **Delta Lake**: ACID transactions, time travel, auto-optimization
- **Apache Iceberg**: Schema evolution, hidden partitioning, metadata management
- **Apache Hudi**: Incremental processing, record-level updates, compaction

## 📁 Project Structure

```
lhbench-v2/
├── README.md                    # Project documentation  
├── requirements.txt             # Python dependencies
├── setup.py                    # Package installation
├── setup.sh                   # Initial setup script
├── Makefile                    # Automation commands
├── .gitignore                  # Git exclusions
├── .env.example               # Environment template
│
├── configs/                    # Configuration files
│   ├── engines.yaml           # Engine configurations
│   └── spark.yaml             # Spark settings
│
├── lhbench/                   # Core package
│   ├── core/                  # Core functionality
│   ├── data/                  # Data generation (TPC-DS)
│   │   ├── data_manager.py    # Data lifecycle management
│   │   └── tpcds_generator.py # DuckDB TPC-DS generation
│   ├── engines/               # Engine implementations
│   │   ├── base_engine.py     # Abstract base class
│   │   ├── delta_engine.py    # Delta Lake implementation
│   │   ├── iceberg_engine.py  # Apache Iceberg implementation
│   │   ├── hudi_engine.py     # Apache Hudi implementation
│   │   ├── engine_factory.py  # Multi-engine manager
│   │   ├── spark_manager.py   # Spark session management
│   │   └── config.py          # Engine configurations
│   ├── storage/               # Storage management (MinIO)
│   └── utils/                 # Utility functions
│
├── scripts/                   # Benchmark scripts
│   ├── run_queries.py         # Query execution runner
│   ├── tpcds_queries.py       # TPC-DS 99 queries (5929 lines)
│   ├── generate_data.py       # TPC-DS data generation
│   └── test_engines.py        # Engine integration tests
│
├── tests/                     # Unit tests
├── docs/                      # Documentation
├── examples/                  # Usage examples
├── jars/                     # JAR dependencies (315MB)
├── data/                     # Generated data storage
└── results/                  # Benchmark results
```

## ⚙️ Configuration

### Environment Variables

Key settings for customization:

```bash
# TPC-DS data scale (1 = ~1GB, 10 = ~10GB, 100 = ~100GB)
SCALE_FACTOR=10

# MinIO configuration  
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
BUCKET_NAME=lakehouse

# Engines to test
ENGINES="delta iceberg hudi"

# Benchmark settings
BENCHMARK_ITERATIONS=3
```

### Spark Configuration

Automatically configured based on scale factor:
- **JAR Dependencies**: Auto-downloaded to `./jars/`
- **Memory Settings**: Adaptive based on data size
- **Engine Extensions**: Delta, Iceberg, Hudi SQL extensions
- **S3 Configuration**: MinIO-compatible settings

## 🔧 Workflow Commands

### Environment Setup

```bash
make help                    # Show all available commands
make setup                   # Complete environment setup (MinIO + data)
make validate                # Validate implementation
make spark-jars             # Download Spark JAR dependencies
```

### MinIO Storage Management

```bash
make minio-start            # Start MinIO container
make minio-stop             # Stop MinIO container
make minio-status           # Check MinIO status
make minio-bucket           # Create lakehouse bucket
```

### TPC-DS Data Generation

```bash
# Generate data (skip if exists)
SCALE_FACTOR=10 make data-generate

# Force regenerate data
SCALE_FACTOR=10 make data-regenerate

# Check data status
make data-status

# Clean generated data from MinIO
make data-clean
```

### Benchmarking

```bash
# Run complete benchmark (engines + queries)
SCALE_FACTOR=10 make benchmark-run

# Test engine implementations only
make benchmark-engines

# Run query performance tests only
SCALE_FACTOR=10 make benchmark-queries

# Run specific queries
python scripts/run_queries.py --queries q01 q02 q03 --scale-factor 10
```

### Results Analysis

```bash
make view-results           # View latest results in table format
python scripts/view_results.py results/query_results_*.json
```

### Development & Testing

```bash
make test                   # Run integration tests
python scripts/test_engines.py  # Test all engines
```

## 📊 TPC-DS Query Suite

LHBench v2 uses the complete **TPC-DS 99 query suite** for comprehensive performance evaluation:

### Query Categories

- **Reporting Queries**: Complex aggregations and window functions (q01-q10)
- **Ad-hoc Queries**: Interactive analytics patterns (q11-q30) 
- **Data Mining Queries**: Statistical analysis and patterns (q31-q50)
- **Complex Queries**: Multi-table joins and subqueries (q51-q70)
- **Iterative Queries**: Recursive and iterative patterns (q71-q99)

### Example Results

```
TPC-DS Scale Factor 10 Benchmark Results
============================================
Query: q01 (Customer Segmentation)
------------------------------------------------------------
Engine          Avg Time (s) Min Time (s) Max Time (s) Success Rate
------------------------------------------------------------
delta           2.156        2.089        2.245        100.0%      
iceberg         2.834        2.756        2.923        100.0%      
hudi            3.127        3.045        3.198        100.0%      

Query: q42 (Sales Analysis)
------------------------------------------------------------
Engine          Avg Time (s) Min Time (s) Max Time (s) Success Rate
------------------------------------------------------------
delta           1.432        1.398        1.467        100.0%      
iceberg         1.789        1.756        1.821        100.0%      
hudi            2.034        1.998        2.071        100.0%      
```

### Custom Query Support

You can run specific queries or subsets:

```bash
# Run first 10 queries
python scripts/run_queries.py --queries q01 q02 q03 q04 q05 q06 q07 q08 q09 q10

# Run complex analytical queries
python scripts/run_queries.py --queries q13 q23 q34 q67 q78

# Run all queries with higher iterations
python scripts/run_queries.py --iterations 5 --scale-factor 10
```

## 🧪 Usage Examples

### Basic Engine Testing

```python
from lhbench.engines import SparkManager, EngineFactory
from lhbench.engines.config import get_spark_config, get_engine_config

# Create Spark session with all engines
spark_config = get_spark_config(scale_factor=10)
spark_manager = SparkManager(spark_config)
spark = spark_manager.create_spark_session(['delta', 'iceberg', 'hudi'])

# Create Delta engine
delta_config = get_engine_config('delta', scale_factor=10)
delta_engine = EngineFactory.create_engine('delta', spark, delta_config)

# Create table from Parquet data
success = delta_engine.create_table(
    'store_sales', 
    's3a://lakehouse/tpcds-data/sf_10/store_sales.parquet',
    partitions=['ss_sold_date_sk']
)

# Run query
results = delta_engine.run_query(
    "SELECT COUNT(*) FROM store_sales WHERE ss_sold_date_sk > 2450000", 
    collect_results=True
)
```

### Multi-Engine Comparison

```python
from lhbench.engines import MultiEngineManager
from lhbench.engines.config import get_all_engines_config

# Initialize all engines
engines_config = get_all_engines_config(scale_factor=10)
engine_manager = MultiEngineManager(spark, engines_config)

# Create tables in all engines
results = engine_manager.create_table_all_engines(
    'customer', 
    's3a://lakehouse/tpcds-data/sf_10/customer.parquet'
)

# Run query across all engines
query_results = engine_manager.run_query_all_engines(
    "SELECT c_customer_sk, c_first_name, c_last_name FROM customer LIMIT 10",
    collect_results=True
)

# Compare performance
perf_results = engine_manager.compare_query_performance(
    "SELECT COUNT(*) FROM customer", 
    iterations=3
)
```

## 🔍 Troubleshooting

### Common Issues

1. **Java not found**
   ```bash
   sudo apt-get install openjdk-11-jdk
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
   ```

2. **Docker permission denied**
   ```bash
   sudo usermod -aG docker $USER
   # Logout and login again
   ```

3. **MinIO connection failed**
   ```bash
   make minio-status          # Check if running
   make minio-start           # Start if needed
   docker logs lhbench-minio  # Check logs
   ```

4. **Out of memory errors**
   ```bash
   # Reduce scale factor
   SCALE_FACTOR=1 make data-generate
   
   # Or increase Spark memory in lhbench/engines/config.py
   ```

5. **Missing Spark JARs**
   ```bash
   make spark-jars           # Download all dependencies
   ls -la jars/              # Verify JARs exist
   ```

6. **Iceberg S3FileIO errors**
   ```bash
   # This is expected - system auto-falls back to HadoopFileIO
   # See logs for "Failed to set SQL configurations" warnings
   ```

### Debug Mode

Enable detailed logging:

```bash
# Set environment variable
export LOG_LEVEL=DEBUG

# Run with verbose output
SCALE_FACTOR=10 make benchmark-run 2>&1 | tee debug.log
```

### Validate Setup

```bash
# Test engines integration
python scripts/test_engines.py

# Validate implementation
make validate

# Check MinIO data
python scripts/check_minio_data.py
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

### Development Setup

```bash
git clone <repository-url>
cd lhbench-v2

# Install in development mode
pip install -r requirements.txt
pip install -e .

# Download dependencies
make spark-jars

# Test setup
python scripts/test_engines.py
```

### Adding New Engines

1. Create new engine class in `lhbench/engines/`
2. Inherit from `BaseLakehouseEngine`
3. Implement required methods: `create_table`, `run_query`, etc.
4. Add configuration in `lhbench/engines/config.py`
5. Update `EngineFactory.create_engine()`

### Adding New Queries

1. Add queries to `scripts/tpcds_queries.py`
2. Follow the structure: `{'name': '...', 'sql': '...'}`
3. Test with `python scripts/run_queries.py --queries your_query`

## 📊 Performance Characteristics

### Observed Performance Patterns

Based on testing with scale factor 10 (8.6GB TPC-DS data):

**Delta Lake**: 
- ✅ Fastest for analytical queries
- ✅ Excellent for time travel queries
- ⚠️ Larger storage footprint

**Apache Iceberg**:
- ✅ Best metadata management
- ✅ Schema evolution capabilities  
- ⚠️ Moderate query performance

**Apache Hudi**:
- ✅ Best for incremental processing
- ✅ Record-level updates
- ⚠️ Complex configuration requirements

### Scale Factor Recommendations

- **SF 1 (1GB)**: Development and testing
- **SF 10 (10GB)**: Performance evaluation  
- **SF 100 (100GB)**: Production simulation
- **SF 1000 (1TB)**: Large-scale benchmarking

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Apache Spark](https://spark.apache.org/) Community
- [Delta Lake](https://delta.io/) Project  
- [Apache Iceberg](https://iceberg.apache.org/) Community
- [Apache Hudi](https://hudi.apache.org/) Community
- [DuckDB](https://duckdb.org/) for excellent TPC-DS implementation
- [TPC-DS](http://www.tpc.org/tpcds/) Benchmark Specification

---

**LHBench v2** - Making lakehouse performance benchmarking simple and reliable with modern tooling and complete TPC-DS query coverage.