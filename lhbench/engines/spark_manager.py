"""
Spark session manager with lakehouse extensions
"""
import os
import logging
from typing import Dict, Optional, List
import structlog
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

logger = structlog.get_logger(__name__)


class SparkManager:
    """
    Manager for Spark sessions with lakehouse storage format support
    """
    
    def __init__(self, config: Dict):
        """
        Initialize Spark manager
        
        Args:
            config: Spark and storage configuration
        """
        self.config = config
        self.spark_session = None
        
    def create_spark_session(self, engines: List[str] = None) -> Optional[SparkSession]:
        """
        Create Spark session with lakehouse engines configured
        
        Args:
            engines: List of engines to enable ('delta', 'iceberg', 'hudi')
            
        Returns:
            Configured Spark session or None if failed
        """
        try:
            engines = engines or ['delta', 'iceberg', 'hudi']
            
            # Build Spark configuration
            spark_conf = self._build_spark_config(engines)
            
            # Create Spark session
            builder = SparkSession.builder.config(conf=spark_conf)
            
            # Set application name
            app_name = self.config.get('app_name', 'LakehouseBenchmark')
            builder = builder.appName(app_name)
            
            # Enable Hive support if requested
            if self.config.get('enable_hive_support', True):
                builder = builder.enableHiveSupport()
                
            # Create session
            self.spark_session = builder.getOrCreate()
            
            # Configure catalogs
            self._configure_catalogs(engines)
            
            # Set additional SQL configurations
            self._set_sql_configurations()
            
            logger.info("Created Spark session",
                       app_name=app_name,
                       engines=engines,
                       spark_version=self.spark_session.version)
                       
            return self.spark_session
            
        except Exception as e:
            logger.error("Failed to create Spark session", error=str(e))
            return None
            
    def get_spark_session(self) -> Optional[SparkSession]:
        """
        Get current Spark session
        
        Returns:
            Current Spark session or None
        """
        return self.spark_session
        
    def stop_spark_session(self):
        """Stop current Spark session"""
        if self.spark_session:
            try:
                self.spark_session.stop()
                self.spark_session = None
                logger.info("Stopped Spark session")
            except Exception as e:
                logger.error("Failed to stop Spark session", error=str(e))
                
    def _build_spark_config(self, engines: List[str]) -> SparkConf:
        """
        Build Spark configuration with lakehouse extensions
        
        Args:
            engines: List of engines to configure
            
        Returns:
            Spark configuration object
        """
        conf = SparkConf()
        
        # Basic Spark settings
        basic_configs = {
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.adaptive.skewJoin.enabled': 'true',
            'spark.sql.extensions': self._get_sql_extensions(engines),
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false'
        }
        
        # Add JAR dependencies
        jars_config = self._get_jar_dependencies(engines)
        if jars_config:
            basic_configs.update(jars_config)
        
        # S3/MinIO configuration
        s3_config = self.config.get('s3', {})
        if s3_config:
            s3_configs = {
                'spark.hadoop.fs.s3a.endpoint': s3_config.get('endpoint', 'http://localhost:9000'),
                'spark.hadoop.fs.s3a.access.key': s3_config.get('access_key', 'minioadmin'),
                'spark.hadoop.fs.s3a.secret.key': s3_config.get('secret_key', 'minioadmin'),
                'spark.hadoop.fs.s3a.bucket.probe': '0',
                'spark.hadoop.fs.s3a.change.detection.enabled': 'false',
                'spark.hadoop.fs.s3a.change.detection.version.required': 'false'
            }
            basic_configs.update(s3_configs)
            
        # Engine-specific configurations
        if 'delta' in engines:
            delta_configs = self._get_delta_configs()
            basic_configs.update(delta_configs)
            
        if 'iceberg' in engines:
            iceberg_configs = self._get_iceberg_configs()
            basic_configs.update(iceberg_configs)
            
        if 'hudi' in engines:
            hudi_configs = self._get_hudi_configs()
            basic_configs.update(hudi_configs)
            
        # Memory and executor settings
        memory_configs = {
            'spark.executor.memory': self.config.get('executor_memory', '2g'),
            'spark.driver.memory': self.config.get('driver_memory', '2g'),
            'spark.executor.cores': str(self.config.get('executor_cores', 2)),
            'spark.executor.instances': str(self.config.get('executor_instances', 2)),
            'spark.driver.maxResultSize': self.config.get('driver_max_result_size', '1g')
        }
        basic_configs.update(memory_configs)
        
        # Add custom configurations
        custom_configs = self.config.get('spark_configs', {})
        basic_configs.update(custom_configs)
        
        # Set all configurations
        for key, value in basic_configs.items():
            conf.set(key, value)
            
        logger.info("Built Spark configuration",
                   engines=engines,
                   config_count=len(basic_configs))
                   
        return conf
        
    def _get_sql_extensions(self, engines: List[str]) -> str:
        """
        Get SQL extensions for enabled engines
        
        Args:
            engines: List of enabled engines
            
        Returns:
            Comma-separated list of SQL extensions
        """
        extensions = []
        
        if 'delta' in engines:
            extensions.append('io.delta.sql.DeltaSparkSessionExtension')
            
        if 'iceberg' in engines:
            extensions.append('org.apache.iceberg.spark.extensions.IcebergSparkSessionExtension')
            
        # Hudi doesn't require SQL extensions in the same way
        
        return ','.join(extensions)
        
    def _get_delta_configs(self) -> Dict[str, str]:
        """Get Delta Lake specific configurations"""
        return {
            'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
        }
        
    def _get_iceberg_configs(self) -> Dict[str, str]:
        """Get Iceberg specific configurations"""
        warehouse_path = self.config.get('iceberg', {}).get('warehouse_path', 's3a://lakehouse/iceberg')
        catalog_name = self.config.get('iceberg', {}).get('catalog_name', 'lakehouse_catalog')
        
        return {
            f'spark.sql.catalog.{catalog_name}': 'org.apache.iceberg.spark.SparkCatalog',
            f'spark.sql.catalog.{catalog_name}.type': 'hadoop',
            f'spark.sql.catalog.{catalog_name}.warehouse': warehouse_path,
            # Use HadoopFileIO which works better with S3A
            f'spark.sql.catalog.{catalog_name}.io-impl': 'org.apache.iceberg.hadoop.HadoopFileIO',
            'spark.sql.defaultCatalog': catalog_name
        }
        
    def _get_hudi_configs(self) -> Dict[str, str]:
        """Get Hudi specific configurations"""
        return {
            'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.hudi.catalog.HoodieCatalog',
            'spark.kryo.registrator': 'org.apache.spark.HoodieSparkKryoRegistrar',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer'
        }
        
    def _configure_catalogs(self, engines: List[str]):
        """Configure additional catalogs if needed"""
        if not self.spark_session:
            return
            
        try:
            # Additional catalog configuration can be done here
            # For now, catalogs are configured via Spark conf
            pass
            
        except Exception as e:
            logger.warning("Failed to configure additional catalogs", error=str(e))
            
    def _set_sql_configurations(self):
        """Set additional SQL configurations"""
        if not self.spark_session:
            return
            
        try:
            # Set timezone
            timezone = self.config.get('timezone', 'UTC')
            self.spark_session.sql(f"SET TIME ZONE '{timezone}'")
            
            # Set additional SQL configurations
            sql_configs = self.config.get('sql_configs', {})
            for key, value in sql_configs.items():
                self.spark_session.conf.set(key, value)
                
        except Exception as e:
            logger.warning("Failed to set SQL configurations", error=str(e))
            
    def validate_engines(self, engines: List[str]) -> Dict[str, bool]:
        """
        Validate that engines are properly configured
        
        Args:
            engines: List of engines to validate
            
        Returns:
            Dictionary with validation results
        """
        results = {}
        
        if not self.spark_session:
            return {engine: False for engine in engines}
            
        for engine in engines:
            try:
                if engine == 'delta':
                    # Test Delta functionality
                    test_df = self.spark_session.range(1)
                    test_df.write.format("delta").mode("overwrite").option("path", "/tmp/delta_test").save()
                    results['delta'] = True
                    
                elif engine == 'iceberg':
                    # Test Iceberg functionality
                    catalogs = self.spark_session.sql("SHOW CATALOGS").collect()
                    iceberg_catalog = self.config.get('iceberg', {}).get('catalog_name', 'lakehouse_catalog')
                    results['iceberg'] = any(row.catalog == iceberg_catalog for row in catalogs)
                    
                elif engine == 'hudi':
                    # Test Hudi functionality
                    test_df = self.spark_session.range(1).withColumn("ts", self.spark_session.sql("SELECT current_timestamp()").collect()[0][0])
                    hudi_options = {
                        'hoodie.table.name': '_test_validation',
                        'hoodie.datasource.write.recordkey.field': 'id',
                        'hoodie.datasource.write.precombine.field': 'ts'
                    }
                    test_df.write.format("hudi").options(**hudi_options).mode("overwrite").save("/tmp/hudi_test")
                    results['hudi'] = True
                    
            except Exception as e:
                logger.error("Engine validation failed",
                           engine=engine,
                           error=str(e))
                results[engine] = False
                
        return results
        
    def get_spark_ui_url(self) -> Optional[str]:
        """
        Get Spark UI URL
        
        Returns:
            Spark UI URL or None
        """
        if self.spark_session:
            try:
                return self.spark_session.sparkContext.uiWebUrl
            except:
                pass
        return None
        
    def get_session_info(self) -> Dict:
        """
        Get information about current Spark session
        
        Returns:
            Dictionary with session information
        """
        if not self.spark_session:
            return {"session_active": False}
            
        try:
            sc = self.spark_session.sparkContext
            return {
                "session_active": True,
                "app_name": sc.appName,
                "app_id": sc.applicationId,
                "spark_version": self.spark_session.version,
                "master": sc.master,
                "executor_memory": sc.getConf().get("spark.executor.memory", "unknown"),
                "driver_memory": sc.getConf().get("spark.driver.memory", "unknown"),
                "ui_web_url": sc.uiWebUrl,
                "default_parallelism": sc.defaultParallelism
            }
            
        except Exception as e:
            logger.error("Failed to get session info", error=str(e))
            return {"session_active": False, "error": str(e)}
            
    def _get_jar_dependencies(self, engines: List[str]) -> Dict[str, str]:
        """
        Get JAR dependencies for enabled engines
        
        Args:
            engines: List of enabled engines
            
        Returns:
            Dictionary with JAR configuration
        """
        jars_dir = os.environ.get('SPARK_JARS_DIR', './jars')
        
        # Check if jars directory exists
        if not os.path.exists(jars_dir):
            logger.warning("JARs directory not found, trying to use packages", jars_dir=jars_dir)
            return self._get_maven_packages(engines)
            
        jars = []
        
        # Common AWS S3 JARs
        aws_jars = [
            'hadoop-aws-3.3.4.jar',
            'aws-java-sdk-bundle-1.12.262.jar'
        ]
        
        for jar in aws_jars:
            jar_path = os.path.join(jars_dir, jar)
            if os.path.exists(jar_path):
                jars.append(jar_path)
                
        # Engine-specific JARs
        if 'delta' in engines:
            delta_jars = [
                'delta-core_2.12-2.4.0.jar',
                'delta-storage-2.4.0.jar'
            ]
            for jar in delta_jars:
                jar_path = os.path.join(jars_dir, jar)
                if os.path.exists(jar_path):
                    jars.append(jar_path)
                    
        if 'iceberg' in engines:
            iceberg_jars = [
                'iceberg-spark-runtime-3.5_2.12-1.4.2.jar',
                'iceberg-aws-1.4.2.jar'
            ]
            for jar in iceberg_jars:
                jar_path = os.path.join(jars_dir, jar)
                if os.path.exists(jar_path):
                    jars.append(jar_path)
                    
        if 'hudi' in engines:
            hudi_jars = [
                'hudi-spark3.4-bundle_2.12-0.14.0.jar'
            ]
            for jar in hudi_jars:
                jar_path = os.path.join(jars_dir, jar)
                if os.path.exists(jar_path):
                    jars.append(jar_path)
        
        if jars:
            logger.info("Found JAR dependencies", jars_count=len(jars), jars_dir=jars_dir)
            return {'spark.jars': ','.join(jars)}
        else:
            logger.warning("No JAR dependencies found, falling back to packages")
            return self._get_maven_packages(engines)
            
    def _get_maven_packages(self, engines: List[str]) -> Dict[str, str]:
        """
        Get Maven packages for engines (fallback when JARs not available)
        
        Args:
            engines: List of enabled engines
            
        Returns:
            Dictionary with packages configuration
        """
        packages = [
            'org.apache.hadoop:hadoop-aws:3.3.4',
            'com.amazonaws:aws-java-sdk-bundle:1.12.262'
        ]
        
        if 'delta' in engines:
            packages.extend([
                'io.delta:delta-core_2.12:2.4.0',
                'io.delta:delta-storage:2.4.0'
            ])
            
        if 'iceberg' in engines:
            packages.append('org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2')
            
        if 'hudi' in engines:
            packages.append('org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0')
            
        logger.info("Using Maven packages", packages_count=len(packages))
        return {'spark.jars.packages': ','.join(packages)}