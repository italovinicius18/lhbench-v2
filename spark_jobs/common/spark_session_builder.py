"""Spark Session Builder configured from .env."""

import os
from typing import Optional
from pyspark.sql import SparkSession


class SparkSessionBuilder:
    """Builds SparkSession with configuration from environment variables."""

    @staticmethod
    def build_session(
        app_name: str,
        framework: Optional[str] = None,
        additional_configs: Optional[dict] = None
    ) -> SparkSession:
        """
        Build Spark session with framework-specific configurations.

        Args:
            app_name: Name of the Spark application
            framework: Framework name ('iceberg', 'delta', 'hudi', or None)
            additional_configs: Additional Spark configurations

        Returns:
            Configured SparkSession
        """
        builder = SparkSession.builder.appName(app_name)

        # Master URL
        master_url = os.getenv('SPARK_MASTER_URL', 'local[*]')
        builder = builder.master(master_url)

        # Driver configurations
        builder = builder \
            .config("spark.driver.memory", os.getenv('SPARK_DRIVER_MEMORY', '4g')) \
            .config("spark.driver.cores", os.getenv('SPARK_DRIVER_CORES', '2')) \
            .config("spark.driver.maxResultSize", os.getenv('SPARK_DRIVER_MAX_RESULT_SIZE', '2g'))

        # Executor configurations
        builder = builder \
            .config("spark.executor.memory", os.getenv('SPARK_EXECUTOR_MEMORY', '4g')) \
            .config("spark.executor.cores", os.getenv('SPARK_EXECUTOR_CORES', '2')) \
            .config("spark.executor.instances", os.getenv('SPARK_EXECUTOR_INSTANCES', '2'))

        # SQL configurations
        builder = builder \
            .config("spark.sql.adaptive.enabled", os.getenv('SPARK_SQL_ADAPTIVE_ENABLED', 'true')) \
            .config("spark.sql.adaptive.coalescePartitions.enabled",
                    os.getenv('SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS', 'true')) \
            .config("spark.sql.shuffle.partitions", os.getenv('SPARK_SQL_SHUFFLE_PARTITIONS', '200')) \
            .config("spark.sql.autoBroadcastJoinThreshold",
                    os.getenv('SPARK_SQL_AUTOBROADCASTJOIN_THRESHOLD', '10485760'))

        # Memory management
        memory_fraction = os.getenv('SPARK_MEMORY_FRACTION', '0.8')
        storage_fraction = os.getenv('SPARK_MEMORY_STORAGE_FRACTION', '0.3')
        builder = builder \
            .config("spark.memory.fraction", memory_fraction) \
            .config("spark.memory.storageFraction", storage_fraction)

        # Framework-specific configurations
        if framework == 'iceberg':
            builder = SparkSessionBuilder._configure_iceberg(builder)
        elif framework == 'delta':
            builder = SparkSessionBuilder._configure_delta(builder)
        elif framework == 'hudi':
            builder = SparkSessionBuilder._configure_hudi(builder)

        # Additional custom configurations
        if additional_configs:
            for key, value in additional_configs.items():
                builder = builder.config(key, value)

        # Create session
        spark = builder.getOrCreate()

        # Set log level
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        spark.sparkContext.setLogLevel(log_level)

        return spark

    @staticmethod
    def _configure_iceberg(builder: SparkSession.Builder) -> SparkSession.Builder:
        """Configure Spark for Apache Iceberg."""
        catalog_name = os.getenv('ICEBERG_CATALOG_NAME', 'lakehouse_catalog')
        warehouse_path = os.getenv('ICEBERG_WAREHOUSE_PATH', '/data/silver/iceberg')

        return builder \
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config(f"spark.sql.catalog.{catalog_name}",
                    "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop") \
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path) \
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hive") \
            .config("spark.sql.defaultCatalog", catalog_name)

    @staticmethod
    def _configure_delta(builder: SparkSession.Builder) -> SparkSession.Builder:
        """Configure Spark for Delta Lake."""
        return builder \
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.optimizeWrite.enabled",
                    os.getenv('DELTA_OPTIMIZE_WRITE', 'true')) \
            .config("spark.databricks.delta.autoCompact.enabled",
                    os.getenv('DELTA_AUTO_COMPACT', 'true'))

    @staticmethod
    def _configure_hudi(builder: SparkSession.Builder) -> SparkSession.Builder:
        """Configure Spark for Apache Hudi."""
        return builder \
            .config("spark.serializer",
                    "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.hive.convertMetastoreParquet", "false") \
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
            .config("spark.sql.extensions",
                    "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")


def build_session(
    app_name: str,
    framework: Optional[str] = None,
    **configs
) -> SparkSession:
    """Convenience function to build Spark session."""
    return SparkSessionBuilder.build_session(app_name, framework, configs)
