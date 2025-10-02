"""Configuration loader from .env file."""

import os
from typing import Dict, Any, List
from pathlib import Path
from dotenv import load_dotenv


class ConfigLoader:
    """Loads and manages configuration from .env file."""

    _instance = None
    _config: Dict[str, Any] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._config:
            self._load_config()

    def _load_config(self):
        """Load configuration from .env file."""
        # Load .env file
        env_path = Path('.env')
        if env_path.exists():
            load_dotenv(env_path)
        else:
            load_dotenv(Path('.env.example'))

        # Parse all environment variables
        self._config = {
            # Benchmark settings
            'BENCHMARK_NAME': os.getenv('BENCHMARK_NAME', 'lakehouse-comparison'),
            'BENCHMARK_VERSION': os.getenv('BENCHMARK_VERSION', '1.0.0'),
            'BENCHMARK_ITERATIONS': int(os.getenv('BENCHMARK_ITERATIONS', '3')),
            'FRAMEWORKS': self._parse_list(os.getenv('FRAMEWORKS', 'iceberg,delta,hudi')),

            # TPC-H settings
            'TPCH_SCALE_FACTOR': int(os.getenv('TPCH_SCALE_FACTOR', '10')),
            'TPCH_TABLES': self._parse_list(os.getenv('TPCH_TABLES',
                'customer,orders,lineitem,supplier,part,partsupp,nation,region')),
            'TPCHGEN_FORMAT': os.getenv('TPCHGEN_FORMAT', 'parquet'),
            'TPCHGEN_COMPRESSION': os.getenv('TPCHGEN_COMPRESSION', 'snappy'),
            'TPCHGEN_PARTS': int(os.getenv('TPCHGEN_PARTS', '4')),
            'TPCHGEN_PARQUET_ROW_GROUP_BYTES': int(os.getenv('TPCHGEN_PARQUET_ROW_GROUP_BYTES', '134217728')),
            'FORCE_REGENERATE': os.getenv('FORCE_REGENERATE', 'false').lower() == 'true',
            'BASE_SEED': int(os.getenv('BASE_SEED', '42')),

            # Paths
            'DATA_ROOT': os.getenv('DATA_ROOT', '/data'),
            'BRONZE_PATH': os.getenv('BRONZE_PATH', '/data/bronze/tpch'),
            'SILVER_PATH': os.getenv('SILVER_PATH', '/data/silver'),
            'GOLD_PATH': os.getenv('GOLD_PATH', '/data/gold'),
            'METADATA_PATH': os.getenv('METADATA_PATH', '/data/bronze/tpch/_metadata'),
            'MANIFEST_PATH': os.getenv('MANIFEST_PATH', '/data/bronze/updates'),

            # Spark settings
            'SPARK_MASTER_URL': os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077'),
            'SPARK_DRIVER_MEMORY': os.getenv('SPARK_DRIVER_MEMORY', '4g'),
            'SPARK_DRIVER_CORES': int(os.getenv('SPARK_DRIVER_CORES', '2')),
            'SPARK_EXECUTOR_MEMORY': os.getenv('SPARK_EXECUTOR_MEMORY', '4g'),
            'SPARK_EXECUTOR_CORES': int(os.getenv('SPARK_EXECUTOR_CORES', '2')),
            'SPARK_EXECUTOR_INSTANCES': int(os.getenv('SPARK_EXECUTOR_INSTANCES', '2')),
            'SPARK_SQL_SHUFFLE_PARTITIONS': int(os.getenv('SPARK_SQL_SHUFFLE_PARTITIONS', '200')),

            # Iceberg settings
            'ICEBERG_ENABLED': os.getenv('ICEBERG_ENABLED', 'true').lower() == 'true',
            'ICEBERG_WAREHOUSE_PATH': os.getenv('ICEBERG_WAREHOUSE_PATH', '/data/silver/iceberg'),
            'ICEBERG_CATALOG_NAME': os.getenv('ICEBERG_CATALOG_NAME', 'lakehouse_catalog'),

            # Delta settings
            'DELTA_ENABLED': os.getenv('DELTA_ENABLED', 'true').lower() == 'true',
            'DELTA_WAREHOUSE_PATH': os.getenv('DELTA_WAREHOUSE_PATH', '/data/silver/delta'),

            # Hudi settings
            'HUDI_ENABLED': os.getenv('HUDI_ENABLED', 'true').lower() == 'true',
            'HUDI_WAREHOUSE_PATH': os.getenv('HUDI_WAREHOUSE_PATH', '/data/silver/hudi'),
            'HUDI_TABLE_TYPE': os.getenv('HUDI_TABLE_TYPE', 'COPY_ON_WRITE'),

            # Query settings
            'TPCH_QUERIES': self._parse_queries(os.getenv('TPCH_QUERIES', 'all')),
            'QUERY_TIMEOUT_SECONDS': int(os.getenv('QUERY_TIMEOUT_SECONDS', '3600')),
            'QUERY_WARMUP_RUNS': int(os.getenv('QUERY_WARMUP_RUNS', '1')),
            'QUERY_BENCHMARK_RUNS': int(os.getenv('QUERY_BENCHMARK_RUNS', '3')),

            # Monitoring
            'LOG_LEVEL': os.getenv('LOG_LEVEL', 'INFO'),
            'METRICS_OUTPUT_PATH': os.getenv('METRICS_OUTPUT_PATH', '/data/gold/metrics'),
            'ENABLE_DETAILED_METRICS': os.getenv('ENABLE_DETAILED_METRICS', 'true').lower() == 'true',

            # State management
            'STATE_FILE_PATH': os.getenv('STATE_FILE_PATH', '/data/bronze/tpch/_metadata/state.json'),
            'ENABLE_STATE_RECOVERY': os.getenv('ENABLE_STATE_RECOVERY', 'true').lower() == 'true',
        }

    @staticmethod
    def _parse_list(value: str) -> List[str]:
        """Parse comma-separated string to list."""
        return [item.strip() for item in value.split(',') if item.strip()]

    @staticmethod
    def _parse_queries(value: str) -> List[int]:
        """Parse query list (e.g., '1,3,5' or 'all')."""
        if value.lower() == 'all':
            return list(range(1, 23))  # TPC-H has 22 queries
        return [int(q.strip()) for q in value.split(',') if q.strip()]

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self._config.get(key, default)

    def get_all(self) -> Dict[str, Any]:
        """Get all configuration."""
        return self._config.copy()

    def __getitem__(self, key: str) -> Any:
        """Dict-like access."""
        return self._config[key]


# Singleton instance
_config_loader = ConfigLoader()


def load_config() -> Dict[str, Any]:
    """Load configuration from .env file."""
    return _config_loader.get_all()


def get_config(key: str, default: Any = None) -> Any:
    """Get specific configuration value."""
    return _config_loader.get(key, default)
