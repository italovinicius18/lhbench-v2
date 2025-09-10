#!/usr/bin/env python3
"""
Example usage of lakehouse engines with TPC-DS data
"""

import os
import sys
import logging
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from lhbench.engines import SparkManager, MultiEngineManager
from lhbench.engines.config import get_spark_config, get_all_engines_config
from lhbench.storage import MinIOClient
from lhbench.data import TPCDSGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main function demonstrating lakehouse engines usage"""
    parser = argparse.ArgumentParser(description="Lakehouse Engines Example")
    parser.add_argument('--scale-factor', type=int, default=1, help='TPC-DS scale factor')
    parser.add_argument('--engines', nargs='+', default=['delta', 'iceberg', 'hudi'], 
                       help='Engines to test')
    parser.add_argument('--data-path', type=str, default='/mnt/c/Users/italo/WSL_DATA/tpcds',
                       help='Path to TPC-DS data')
    parser.add_argument('--minio-endpoint', type=str, default='http://localhost:9000',
                       help='MinIO endpoint')
    parser.add_argument('--minio-access-key', type=str, default='minioadmin',
                       help='MinIO access key')
    parser.add_argument('--minio-secret-key', type=str, default='minioadmin',
                       help='MinIO secret key')
    parser.add_argument('--bucket-name', type=str, default='lakehouse',
                       help='S3/MinIO bucket name')
    parser.add_argument('--skip-data-generation', action='store_true',
                       help='Skip TPC-DS data generation')
    parser.add_argument('--skip-table-creation', action='store_true',
                       help='Skip table creation')
    parser.add_argument('--test-queries', action='store_true',
                       help='Run test queries')
    parser.add_argument('--output-dir', type=str, default='./results',
                       help='Output directory for results')
    
    args = parser.parse_args()
    
    logger.info("Starting lakehouse engines example")
    logger.info(f"Scale factor: {args.scale_factor}")
    logger.info(f"Engines: {args.engines}")
    logger.info(f"Data path: {args.data_path}")
    
    try:
        # Step 1: Generate TPC-DS data if needed
        if not args.skip_data_generation:
            logger.info("Generating TPC-DS data...")
            generate_tpcds_data(args.scale_factor, args.data_path)
        
        # Step 2: Initialize MinIO client
        logger.info("Initializing MinIO client...")
        minio_client = MinIOClient({
            'endpoint_url': args.minio_endpoint,
            'access_key': args.minio_access_key,
            'secret_key': args.minio_secret_key,
            'bucket_name': args.bucket_name
        })
        
        # Step 3: Create Spark session
        logger.info("Creating Spark session...")
        spark_config = get_spark_config(args.scale_factor)
        spark_config['s3']['endpoint'] = args.minio_endpoint
        spark_config['s3']['access_key'] = args.minio_access_key
        spark_config['s3']['secret_key'] = args.minio_secret_key
        
        spark_manager = SparkManager(spark_config)
        spark_session = spark_manager.create_spark_session(args.engines)
        
        if not spark_session:
            logger.error("Failed to create Spark session")
            return 1
            
        # Step 4: Initialize engines
        logger.info("Initializing lakehouse engines...")
        engines_config = get_all_engines_config(args.scale_factor)
        
        # Filter to requested engines
        engines_config = {
            engine: config for engine, config in engines_config.items()
            if engine in args.engines
        }
        
        engine_manager = MultiEngineManager(spark_session, engines_config)
        
        # Step 5: Validate engine setup
        logger.info("Validating engine setup...")
        validation_results = engine_manager.validate_all_engines()
        
        for engine, results in validation_results.items():
            logger.info(f"Engine {engine} validation: {results}")
            
        # Step 6: Create tables if needed
        if not args.skip_table_creation:
            logger.info("Creating tables in all engines...")
            create_tables_example(engine_manager, args.data_path)
        
        # Step 7: Run test queries if requested
        if args.test_queries:
            logger.info("Running test queries...")
            run_test_queries_example(engine_manager)
            
        # Step 8: Performance comparison
        logger.info("Running performance comparison...")
        run_performance_comparison(engine_manager)
        
        logger.info("Example completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"Example failed: {e}")
        return 1
        
    finally:
        # Cleanup
        if 'spark_manager' in locals():
            spark_manager.stop_spark_session()


def generate_tpcds_data(scale_factor: int, data_path: str):
    """Generate TPC-DS data using DuckDB"""
    try:
        with TPCDSGenerator(data_path) as generator:
            generator.generate_data(scale_factor=scale_factor)
        logger.info(f"Generated TPC-DS data (scale factor {scale_factor}) in {data_path}")
        
    except Exception as e:
        logger.error(f"Failed to generate TPC-DS data: {e}")
        raise


def create_tables_example(engine_manager: MultiEngineManager, data_path: str):
    """Example of creating tables in all engines"""
    
    # Define sample tables to create
    tables_to_create = [
        {
            'name': 'store_sales',
            'data_path': f'{data_path}/store_sales.parquet',
            'partitions': ['ss_sold_date_sk']
        },
        {
            'name': 'item',
            'data_path': f'{data_path}/item.parquet',
            'partitions': None
        }
    ]
    
    for table_info in tables_to_create:
        logger.info(f"Creating table {table_info['name']}...")
        
        # Check if data file exists
        if not os.path.exists(table_info['data_path']):
            logger.warning(f"Data file not found: {table_info['data_path']}")
            continue
            
        # Create table in all engines
        results = engine_manager.create_table_all_engines(
            table_name=table_info['name'],
            data_path=table_info['data_path'],
            partitions=table_info['partitions']
        )
        
        for engine, success in results.items():
            status = "SUCCESS" if success else "FAILED"
            logger.info(f"  {engine}: {status}")


def run_test_queries_example(engine_manager: MultiEngineManager):
    """Example of running test queries"""
    
    test_queries = [
        {
            'name': 'Simple Count',
            'sql': 'SELECT COUNT(*) as total_sales FROM store_sales'
        },
        {
            'name': 'Aggregation',
            'sql': '''
                SELECT i_category, COUNT(*) as sales_count, SUM(ss_sales_price) as total_sales
                FROM store_sales ss
                JOIN item i ON ss.ss_item_sk = i.i_item_sk
                GROUP BY i_category
                ORDER BY total_sales DESC
                LIMIT 10
            '''
        }
    ]
    
    for query_info in test_queries:
        logger.info(f"Running query: {query_info['name']}")
        
        try:
            results = engine_manager.run_query_all_engines(
                sql=query_info['sql'],
                collect_results=True
            )
            
            for engine, result in results.items():
                if result is not None:
                    logger.info(f"  {engine}: {len(result)} rows returned")
                else:
                    logger.warning(f"  {engine}: Query failed")
                    
        except Exception as e:
            logger.error(f"Query failed: {e}")


def run_performance_comparison(engine_manager: MultiEngineManager):
    """Example of performance comparison between engines"""
    
    # Simple performance test query
    test_query = """
        SELECT ss_sold_date_sk, COUNT(*) as sales_count, SUM(ss_sales_price) as total_sales
        FROM store_sales
        WHERE ss_sold_date_sk IS NOT NULL
        GROUP BY ss_sold_date_sk
        ORDER BY ss_sold_date_sk
        LIMIT 100
    """
    
    logger.info("Running performance comparison...")
    
    try:
        results = engine_manager.compare_query_performance(
            sql=test_query,
            iterations=3
        )
        
        logger.info("Performance Results:")
        for engine, metrics in results.items():
            if metrics['successful_runs'] > 0:
                logger.info(f"  {engine}:")
                logger.info(f"    Average time: {metrics['avg_time']:.3f}s")
                logger.info(f"    Min time: {metrics['min_time']:.3f}s")
                logger.info(f"    Max time: {metrics['max_time']:.3f}s")
                logger.info(f"    Successful runs: {metrics['successful_runs']}/{metrics['total_runs']}")
            else:
                logger.warning(f"  {engine}: All runs failed")
                
    except Exception as e:
        logger.error(f"Performance comparison failed: {e}")


def show_table_info_example(engine_manager: MultiEngineManager):
    """Example of getting table information from all engines"""
    
    table_name = 'store_sales'
    logger.info(f"Getting table info for {table_name}...")
    
    try:
        results = engine_manager.get_table_info_all_engines(table_name)
        
        for engine, info in results.items():
            if info:
                logger.info(f"  {engine}:")
                logger.info(f"    Format: {info.get('format', 'unknown')}")
                logger.info(f"    Location: {info.get('location', 'unknown')}")
                if 'row_count' in info:
                    logger.info(f"    Rows: {info['row_count']:,}")
            else:
                logger.warning(f"  {engine}: No table info available")
                
    except Exception as e:
        logger.error(f"Failed to get table info: {e}")


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
