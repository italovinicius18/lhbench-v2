"""
Configurações para execução standalone do LHBench TPC-H
"""
import os
from dataclasses import dataclass
from typing import List, Dict, Any

@dataclass
class LHBenchConfig:
    """Configuração central do LHBench"""
    
    # TPC-H Settings
    scale_factor: int = 10
    
    # MinIO/S3 Settings
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin" 
    minio_secret_key: str = "minioadmin"
    
    # Buckets
    bronze_bucket: str = "bronze"
    silver_bucket: str = "silver"
    gold_bucket: str = "gold"
    
    # Spark Settings
    spark_executor_memory: str = "4g"
    spark_driver_memory: str = "2g"
    spark_executor_cores: int = 2
    spark_executor_instances: int = 2
    
    # Processing Options
    input_format: str = "parquet"  # csv ou parquet
    force_regenerate_bronze: bool = False
    force_recreate_silver: bool = False
    
    # Benchmark Settings
    benchmark_formats: List[str] = None
    benchmark_iterations: int = 3
    timeout_minutes: int = 30
    
    # Output Settings
    results_path: str = "./results"
    log_level: str = "INFO"
    save_detailed_results: bool = True
    
    def __post_init__(self):
        if self.benchmark_formats is None:
            self.benchmark_formats = ["delta", "iceberg", "hudi"]
        
        # Criar diretório de resultados
        os.makedirs(self.results_path, exist_ok=True)
    
    @classmethod
    def from_env(cls) -> 'LHBenchConfig':
        """Cria configuração a partir de variáveis de ambiente"""
        return cls(
            scale_factor=int(os.getenv("SCALE_FACTOR", "10")),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            bronze_bucket=os.getenv("BRONZE_BUCKET", "bronze"),
            silver_bucket=os.getenv("SILVER_BUCKET", "silver"),
            gold_bucket=os.getenv("GOLD_BUCKET", "gold"),
            spark_executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
            spark_driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
            spark_executor_cores=int(os.getenv("SPARK_EXECUTOR_CORES", "2")),
            spark_executor_instances=int(os.getenv("SPARK_EXECUTOR_INSTANCES", "2")),
            input_format=os.getenv("INPUT_FORMAT", "parquet"),
            force_regenerate_bronze=os.getenv("FORCE_REGENERATE_BRONZE", "false").lower() == "true",
            force_recreate_silver=os.getenv("FORCE_RECREATE_SILVER", "false").lower() == "true",
            benchmark_iterations=int(os.getenv("BENCHMARK_ITERATIONS", "3")),
            timeout_minutes=int(os.getenv("TIMEOUT_MINUTES", "30")),
            results_path=os.getenv("RESULTS_PATH", "./results"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            save_detailed_results=os.getenv("SAVE_DETAILED_RESULTS", "true").lower() == "true"
        )
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Retorna configuração do Spark"""
        return {
            # Jars para todos os formatos
            "spark.jars.packages": ",".join([
                "io.delta:delta-spark_2.12:3.2.0",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1", 
                "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0",
            ]),
            
            # Extensions
            "spark.sql.extensions": ",".join([
                "io.delta.sql.DeltaSparkSessionExtension",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
            ]),
            
            # Catálogos
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.type": "hadoop",
            "spark.sql.catalog.iceberg.warehouse": f"s3a://{self.silver_bucket}/iceberg_warehouse",
            
            # Hudi
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            
            # S3A / MinIO
            "spark.hadoop.fs.s3a.endpoint": self.minio_endpoint.replace('http://', '').replace('https://', ''),
            "spark.hadoop.fs.s3a.access.key": self.minio_access_key,
            "spark.hadoop.fs.s3a.secret.key": self.minio_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            
            # Performance
            "spark.executor.memory": self.spark_executor_memory,
            "spark.driver.memory": self.spark_driver_memory,
            "spark.executor.cores": str(self.spark_executor_cores),
            "spark.executor.instances": str(self.spark_executor_instances),
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            
            # Delta Lake
            "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        }

# Instância global de configuração
def get_default_config():
    """Retorna configuração padrão"""
    try:
        return LHBenchConfig.from_env()
    except Exception:
        # Fallback para configuração padrão se houver erro
        return LHBenchConfig()

# Instância global de configuração
config = get_default_config()