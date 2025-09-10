#!/usr/bin/env python3
"""
Integration test for Spark lakehouse engines
"""

import os
import sys
import time
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_engines_integration():
    """Test integration of all lakehouse engines"""
    
    logger.info("Starting engines integration test")
    
    try:
        # Import components
        from lhbench.engines import SparkManager, MultiEngineManager
        from lhbench.engines.config import get_spark_config, get_all_engines_config
        
        # Step 1: Create Spark session
        logger.info("Creating Spark session...")
        spark_config = get_spark_config(scale_factor=1)
        
        # Override S3 endpoint for local testing
        spark_config['s3']['endpoint'] = 'http://localhost:9000'
        
        spark_manager = SparkManager(spark_config)
        spark_session = spark_manager.create_spark_session(['delta', 'iceberg', 'hudi'])
        
        if not spark_session:
            logger.error("Failed to create Spark session")
            return False
            
        logger.info(f"Spark session created successfully - Version: {spark_session.version}")
        
        # Step 2: Test Spark basic functionality
        logger.info("Testing basic Spark functionality...")
        test_df = spark_session.range(1000).toDF("id")
        test_df = test_df.withColumn("value", test_df.id * 2)
        row_count = test_df.count()
        logger.info(f"Basic Spark test passed - Generated {row_count} rows")
        
        # Step 3: Test engines validation
        logger.info("Validating engines...")
        validation_results = spark_manager.validate_engines(['delta', 'iceberg', 'hudi'])
        
        for engine, is_valid in validation_results.items():
            status = "PASS" if is_valid else "FAIL"
            logger.info(f"Engine {engine} validation: {status}")
            
        # Step 4: Initialize engine manager
        logger.info("Initializing engine manager...")
        engines_config = get_all_engines_config(scale_factor=1)
        engine_manager = MultiEngineManager(spark_session, engines_config)
        
        available_engines = engine_manager.get_available_engines()
        logger.info(f"Available engines: {available_engines}")
        
        # Step 5: Test table creation
        logger.info("Testing table creation...")
        test_data_path = "/tmp/test_data"
        create_test_data(spark_session, test_data_path)
        
        # Create tables in all engines
        results = engine_manager.create_table_all_engines(
            table_name='test_table',
            data_path=test_data_path,
            partitions=['category']
        )
        
        for engine, success in results.items():
            status = "SUCCESS" if success else "FAILED"
            logger.info(f"Table creation in {engine}: {status}")
            
        # Step 6: Test queries
        logger.info("Testing queries...")
        test_query = "SELECT category, COUNT(*) as count FROM test_table GROUP BY category"
        
        query_results = engine_manager.run_query_all_engines(
            sql=test_query,
            collect_results=True
        )
        
        for engine, result in query_results.items():
            if result is not None:
                logger.info(f"Query in {engine}: {len(result)} result rows")
            else:
                logger.warning(f"Query in {engine}: FAILED")
                
        # Step 7: Test performance comparison
        logger.info("Testing performance comparison...")
        perf_results = engine_manager.compare_query_performance(
            sql="SELECT COUNT(*) FROM test_table",
            iterations=2
        )
        
        logger.info("Performance results:")
        for engine, metrics in perf_results.items():
            if metrics['successful_runs'] > 0:
                logger.info(f"  {engine}: {metrics['avg_time']:.3f}s avg")
            else:
                logger.warning(f"  {engine}: All runs failed")
                
        # Step 8: Test table information
        logger.info("Testing table information retrieval...")
        table_info = engine_manager.get_table_info_all_engines('test_table')
        
        for engine, info in table_info.items():
            if info:
                format_type = info.get('format', 'unknown')
                logger.info(f"Table info for {engine}: format={format_type}")
            else:
                logger.warning(f"Table info for {engine}: No info available")
                
        logger.info("All integration tests completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        if 'spark_manager' in locals():
            logger.info("Stopping Spark session...")
            spark_manager.stop_spark_session()


def create_test_data(spark_session, output_path: str):
    """Create test data for engines testing"""
    
    logger.info(f"Creating test data at {output_path}")
    
    # Create sample data
    import random
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
    from pyspark.sql.functions import lit, current_timestamp
    
    # Define schema
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("category", StringType(), True),
        StructField("value", IntegerType(), True),
        StructField("description", StringType(), True)
    ])
    
    # Generate sample data
    categories = ['A', 'B', 'C', 'D', 'E']
    data = []
    
    for i in range(10000):
        data.append((
            i,
            random.choice(categories),
            random.randint(1, 1000),
            f"Description for item {i}"
        ))
    
    # Create DataFrame
    df = spark_session.createDataFrame(data, schema)
    df = df.withColumn("created_at", current_timestamp())
    
    # Write as Parquet
    df.write.mode("overwrite").parquet(output_path)
    
    logger.info(f"Created test data with {df.count()} rows")


def test_specific_engine_features():
    """Test specific features of each engine"""
    
    logger.info("Testing specific engine features...")
    
    try:
        from lhbench.engines import SparkManager, EngineFactory
        from lhbench.engines.config import get_spark_config, get_engine_config
        
        # Create Spark session
        spark_config = get_spark_config()
        spark_config['s3']['endpoint'] = 'http://localhost:9000'
        
        spark_manager = SparkManager(spark_config)
        spark_session = spark_manager.create_spark_session(['delta', 'iceberg', 'hudi'])
        
        if not spark_session:
            logger.error("Failed to create Spark session")
            return False
            
        # Test each engine individually
        engines_to_test = ['delta', 'iceberg', 'hudi']
        
        for engine_type in engines_to_test:
            logger.info(f"Testing {engine_type} specific features...")
            
            try:
                # Create engine instance
                engine_config = get_engine_config(engine_type)
                engine = EngineFactory.create_engine(engine_type, spark_session, engine_config)
                
                if not engine:
                    logger.error(f"Failed to create {engine_type} engine")
                    continue
                    
                # Test engine validation
                validation = engine.validate_setup()
                logger.info(f"{engine_type} validation: {validation}")
                
                # Test basic operations
                test_table = f"test_{engine_type}_table"
                test_data_path = f"/tmp/test_data_{engine_type}"
                
                # Create test data
                test_df = spark_session.range(1000).toDF("id")
                test_df = test_df.withColumn("category", (test_df.id % 5).cast("string"))
                test_df.write.mode("overwrite").parquet(test_data_path)
                
                # Test table creation
                success = engine.create_table(
                    table_name=test_table,
                    data_path=test_data_path,
                    partitions=['category']
                )
                
                if success:
                    logger.info(f"{engine_type} table creation: SUCCESS")
                    
                    # Test query
                    result = engine.run_query(
                        f"SELECT COUNT(*) as count FROM {test_table}",
                        collect_results=True
                    )
                    
                    if result:
                        logger.info(f"{engine_type} query: SUCCESS - {result[0]['count']} rows")
                    else:
                        logger.warning(f"{engine_type} query: FAILED")
                        
                    # Test table info
                    info = engine.get_table_info(test_table)
                    if info:
                        logger.info(f"{engine_type} table info: SUCCESS")
                    else:
                        logger.warning(f"{engine_type} table info: FAILED")
                        
                else:
                    logger.error(f"{engine_type} table creation: FAILED")
                    
            except Exception as e:
                logger.error(f"Error testing {engine_type}: {e}")
                
        logger.info("Specific engine features testing completed")
        return True
        
    except Exception as e:
        logger.error(f"Engine features test failed: {e}")
        return False
        
    finally:
        if 'spark_manager' in locals():
            spark_manager.stop_spark_session()


def main():
    """Main test function"""
    
    logger.info("=== Lakehouse Engines Integration Test ===")
    
    # Test 1: Full integration test
    logger.info("\n--- Test 1: Full Integration Test ---")
    integration_success = test_engines_integration()
    
    # Test 2: Specific engine features
    logger.info("\n--- Test 2: Specific Engine Features ---")
    features_success = test_specific_engine_features()
    
    # Summary
    logger.info("\n=== Test Summary ===")
    logger.info(f"Integration Test: {'PASS' if integration_success else 'FAIL'}")
    logger.info(f"Features Test: {'PASS' if features_success else 'FAIL'}")
    
    overall_success = integration_success and features_success
    logger.info(f"Overall Result: {'PASS' if overall_success else 'FAIL'}")
    
    return 0 if overall_success else 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
