#!/usr/bin/env python3
"""
Query benchmark runner for lakehouse engines
"""

import os
import sys
import json
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from lhbench.engines import SparkManager, MultiEngineManager
from lhbench.engines.config import get_spark_config, get_all_engines_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# TPC-DS Official Queries (99 queries)
# Complete TPC-DS query suite imported from official DuckDB repository
from tpcds_queries import SAMPLE_QUERIES

def main():
    """Main function for running query benchmarks"""
    parser = argparse.ArgumentParser(description="Query Benchmark Runner")
    parser.add_argument('--scale-factor', type=int, default=1, help='TPC-DS scale factor')
    parser.add_argument('--engines', nargs='+', default=['delta', 'iceberg', 'hudi'],
                       help='Engines to test')
    parser.add_argument('--data-path', type=str, default='/mnt/c/Users/italo/WSL_DATA/tpcds',
                       help='Path to TPC-DS data')
    parser.add_argument('--minio-endpoint', type=str, default='http://localhost:9000',
                       help='MinIO endpoint')
    parser.add_argument('--iterations', type=int, default=3,
                       help='Number of iterations per query')
    parser.add_argument('--output', type=str, required=True,
                       help='Output file for results (JSON)')
    parser.add_argument('--queries', nargs='+', default=list(SAMPLE_QUERIES.keys()),
                       help='Queries to run')
    
    args = parser.parse_args()
    
    logger.info("Starting query benchmark")
    logger.info(f"Scale factor: {args.scale_factor}")
    logger.info(f"Engines: {args.engines}")
    logger.info(f"Iterations: {args.iterations}")
    logger.info(f"Queries: {args.queries}")
    
    try:
        # Initialize Spark and engines
        spark_config = get_spark_config(args.scale_factor)
        spark_config['s3']['endpoint'] = args.minio_endpoint
        
        spark_manager = SparkManager(spark_config)
        spark_session = spark_manager.create_spark_session(args.engines)
        
        if not spark_session:
            logger.error("Failed to create Spark session")
            return 1
            
        engines_config = get_all_engines_config(args.scale_factor)
        engines_config = {
            engine: config for engine, config in engines_config.items()
            if engine in args.engines
        }
        
        engine_manager = MultiEngineManager(spark_session, engines_config)
        
        # Run benchmarks
        results = run_query_benchmarks(
            engine_manager, 
            args.queries, 
            args.iterations
        )
        
        # Save results
        save_results(results, args.output)
        
        # Print summary
        print_summary(results)
        
        logger.info("Query benchmark completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        return 1
        
    finally:
        if 'spark_manager' in locals():
            spark_manager.stop_spark_session()


def run_query_benchmarks(engine_manager: MultiEngineManager, 
                        query_names: List[str], 
                        iterations: int) -> Dict[str, Any]:
    """Run query benchmarks on all engines"""
    
    results = {
        'metadata': {
            'timestamp': datetime.now().isoformat(),
            'engines': engine_manager.get_available_engines(),
            'iterations': iterations,
            'queries': query_names
        },
        'queries': {}
    }
    
    for query_name in query_names:
        if query_name not in SAMPLE_QUERIES:
            logger.warning(f"Query {query_name} not found, skipping")
            continue
            
        query_info = SAMPLE_QUERIES[query_name]
        logger.info(f"Running query: {query_info['name']}")
        
        query_results = run_single_query_benchmark(
            engine_manager, 
            query_info, 
            iterations
        )
        
        results['queries'][query_name] = query_results
        
    return results


def run_single_query_benchmark(engine_manager: MultiEngineManager,
                              query_info: Dict[str, str],
                              iterations: int) -> Dict[str, Any]:
    """Run benchmark for a single query across all engines"""
    
    results = {
        'query': query_info,
        'engines': {}
    }
    
    for engine_name in engine_manager.get_available_engines():
        logger.info(f"  Testing engine: {engine_name}")
        
        engine = engine_manager.get_engine(engine_name)
        if not engine:
            logger.warning(f"Engine {engine_name} not available")
            continue
            
        engine_results = {
            'times': [],
            'success_count': 0,
            'error_count': 0,
            'errors': []
        }
        
        for iteration in range(iterations):
            logger.info(f"    Iteration {iteration + 1}/{iterations}")
            
            try:
                start_time = time.time()
                result = engine.run_query(query_info['sql'], collect_results=False)
                end_time = time.time()
                
                execution_time = end_time - start_time
                engine_results['times'].append(execution_time)
                engine_results['success_count'] += 1
                
                logger.info(f"    ✓ Completed in {execution_time:.3f}s")
                
            except Exception as e:
                engine_results['error_count'] += 1
                engine_results['errors'].append(str(e))
                logger.error(f"    ✗ Failed: {e}")
                
        # Calculate statistics
        if engine_results['times']:
            times = engine_results['times']
            engine_results['statistics'] = {
                'avg_time': sum(times) / len(times),
                'min_time': min(times),
                'max_time': max(times),
                'success_rate': engine_results['success_count'] / iterations
            }
        else:
            engine_results['statistics'] = {
                'avg_time': None,
                'min_time': None,
                'max_time': None,
                'success_rate': 0.0
            }
            
        results['engines'][engine_name] = engine_results
        
    return results


def save_results(results: Dict[str, Any], output_file: str):
    """Save benchmark results to file"""
    
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
        
    logger.info(f"Results saved to: {output_path}")


def print_summary(results: Dict[str, Any]):
    """Print benchmark summary"""
    
    print("\n" + "="*80)
    print("QUERY BENCHMARK SUMMARY")
    print("="*80)
    
    engines = results['metadata']['engines']
    
    for query_name, query_results in results['queries'].items():
        query_info = query_results['query']
        print(f"\nQuery: {query_info['name']} ({query_name})")
        print("-" * 60)
        
        # Header
        print(f"{'Engine':<15} {'Avg Time (s)':<12} {'Min Time (s)':<12} {'Max Time (s)':<12} {'Success Rate':<12}")
        print("-" * 60)
        
        # Results for each engine
        for engine_name in engines:
            engine_results = query_results['engines'].get(engine_name, {})
            stats = engine_results.get('statistics', {})
            
            avg_time = stats.get('avg_time')
            min_time = stats.get('min_time')
            max_time = stats.get('max_time')
            success_rate = stats.get('success_rate', 0.0)
            
            avg_str = f"{avg_time:.3f}" if avg_time is not None else "FAILED"
            min_str = f"{min_time:.3f}" if min_time is not None else "FAILED"
            max_str = f"{max_time:.3f}" if max_time is not None else "FAILED"
            success_str = f"{success_rate:.1%}"
            
            print(f"{engine_name:<15} {avg_str:<12} {min_str:<12} {max_str:<12} {success_str:<12}")
    
    print("\n" + "="*80)


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
