#!/usr/bin/env python3
"""
Gold Phase - TPC-H Query Executor

Executes TPC-H queries against lakehouse frameworks and collects performance metrics.
"""

import sys
import os
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import SparkSession

# Add common modules to path
sys.path.insert(0, '/opt/spark/jobs/common')
sys.path.insert(0, '/opt/spark/jobs/gold')

from spark_session_builder import SparkSessionBuilder
from tpch_queries import get_all_queries


class QueryExecutor:
    """Executes TPC-H queries and measures performance"""

    def __init__(self, framework: str, scale_factor: int = 1):
        self.framework = framework
        self.scale_factor = scale_factor
        self.silver_path = f"/data/silver/{framework}/sf{scale_factor}"
        self.warehouse_path = f"/data/silver/{framework}/warehouse"

        # Build Spark session
        self.spark = SparkSessionBuilder.build_session(
            app_name=f"Gold-{framework.upper()}-SF{scale_factor}",
            framework=framework
        )

        # Configure framework-specific settings
        self._configure_framework()

        self.metrics = {
            'framework': framework,
            'scale_factor': scale_factor,
            'start_time': datetime.utcnow().isoformat(),
            'queries': {}
        }

    def _configure_framework(self):
        """Configure framework-specific Spark settings"""
        if self.framework == 'iceberg':
            self.spark.conf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            self.spark.conf.set("spark.sql.catalog.iceberg.type", "hadoop")
            self.spark.conf.set("spark.sql.catalog.iceberg.warehouse", self.warehouse_path)
            # Use iceberg catalog
            self.spark.sql("USE iceberg.tpch")

        elif self.framework == 'delta':
            # Delta uses path-based access
            pass

        elif self.framework == 'hudi':
            # Hudi uses path-based access
            pass

    def register_tables(self):
        """Register all TPC-H tables as temporary views"""
        print(f"\n📋 Registering {self.framework.upper()} tables...")

        tables = ['customer', 'orders', 'lineitem', 'supplier',
                  'part', 'partsupp', 'nation', 'region']

        for table in tables:
            try:
                if self.framework == 'iceberg':
                    # Iceberg tables already registered via catalog
                    iceberg_table = f"iceberg.tpch.{table}"
                    df = self.spark.table(iceberg_table)
                    df.createOrReplaceTempView(table)

                elif self.framework == 'delta':
                    table_path = f"{self.silver_path}/{table}"
                    df = self.spark.read.format("delta").load(table_path)
                    df.createOrReplaceTempView(table)

                elif self.framework == 'hudi':
                    table_path = f"{self.silver_path}/{table}"
                    df = self.spark.read.format("hudi").load(table_path)
                    df.createOrReplaceTempView(table)

                print(f"   ✅ Registered: {table}")

            except Exception as e:
                print(f"   ❌ Failed to register {table}: {e}")
                raise

    def execute_query(self, query_id: str, query_info: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single TPC-H query and measure performance"""
        query_name = query_info['name']
        query_sql = query_info['sql']

        print(f"\n📊 Executing {query_id.upper()}: {query_name}")

        result = {
            'query_id': query_id,
            'query_name': query_name,
            'framework': self.framework
        }

        try:
            # Warm-up run (optional, commented out for faster execution)
            # self.spark.sql(query_sql).count()

            # Actual timed run
            start_time = time.time()
            df = self.spark.sql(query_sql)

            # Trigger execution and collect results
            result_count = df.count()
            result_rows = df.limit(10).collect()  # Get first 10 rows for verification

            end_time = time.time()
            duration = end_time - start_time

            result.update({
                'status': 'success',
                'duration_seconds': round(duration, 3),
                'row_count': result_count,
                'sample_rows': len(result_rows)
            })

            print(f"   ✅ {query_id.upper()}: {duration:.3f}s ({result_count} rows)")

        except Exception as e:
            print(f"   ❌ {query_id.upper()} failed: {e}")
            result.update({
                'status': 'failed',
                'error': str(e)
            })

        return result

    def execute_all_queries(self) -> Dict[str, Any]:
        """Execute all TPC-H queries"""
        print(f"\n🚀 Starting Gold Phase - {self.framework.upper()} Query Execution")
        print(f"   Scale Factor: SF{self.scale_factor}")

        # Register tables
        self.register_tables()

        # Get all queries
        queries = get_all_queries()

        print(f"\n📝 Executing {len(queries)} TPC-H queries...\n")

        # Execute each query
        for query_id, query_info in queries.items():
            query_result = self.execute_query(query_id, query_info)
            self.metrics['queries'][query_id] = query_result

        # Calculate summary statistics
        self.metrics['end_time'] = datetime.utcnow().isoformat()
        self.metrics['total_duration'] = (
            datetime.fromisoformat(self.metrics['end_time']) -
            datetime.fromisoformat(self.metrics['start_time'])
        ).total_seconds()

        successful_queries = [q for q in self.metrics['queries'].values() if q['status'] == 'success']
        if successful_queries:
            total_query_time = sum(q['duration_seconds'] for q in successful_queries)
            self.metrics['summary'] = {
                'total_queries': len(queries),
                'successful_queries': len(successful_queries),
                'failed_queries': len(queries) - len(successful_queries),
                'total_query_time': round(total_query_time, 3),
                'average_query_time': round(total_query_time / len(successful_queries), 3),
            }

        # Save metrics to both locations
        metrics_paths = [
            Path("/data/gold/metrics"),  # WSL data directory
            Path("/opt/spark/results/metrics")  # Project results directory
        ]

        for metrics_dir in metrics_paths:
            metrics_dir.mkdir(parents=True, exist_ok=True)
            metrics_file = metrics_dir / f"gold_{self.framework}_sf{self.scale_factor}.json"
            with open(metrics_file, 'w') as f:
                json.dump(self.metrics, f, indent=2)

        print(f"\n{'='*60}")
        print(f"GOLD PHASE COMPLETED - {self.framework.upper()}")
        print(f"{'='*60}")
        if 'summary' in self.metrics:
            print(f"Successful Queries: {self.metrics['summary']['successful_queries']}/{self.metrics['summary']['total_queries']}")
            print(f"Total Query Time: {self.metrics['summary']['total_query_time']:.3f}s")
            print(f"Average Query Time: {self.metrics['summary']['average_query_time']:.3f}s")
        print(f"📊 Metrics saved to: /data/gold/metrics/ and /opt/spark/results/metrics/")

        return self.metrics

    def close(self):
        """Stop Spark session"""
        self.spark.stop()


if __name__ == "__main__":
    framework = os.getenv('FRAMEWORK', 'delta')
    scale_factor = int(os.getenv('TPCH_SCALE_FACTOR', '1'))

    executor = QueryExecutor(framework=framework, scale_factor=scale_factor)

    try:
        metrics = executor.execute_all_queries()
        print("\n" + json.dumps(metrics, indent=2))
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Gold phase failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        executor.close()
