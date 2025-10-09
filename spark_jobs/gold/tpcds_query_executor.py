#!/usr/bin/env python3
"""
TPC-DS Query Executor
Executes TPC-DS queries (99 queries) and collects performance metrics
Supports tier-based execution (tier1/tier2/tier3/all)
"""

import os
import sys
import time
import json
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession

# Add scripts to path
sys.path.append('/opt/spark/jobs/../scripts')
from utils.tpcds_query_processor import TPCDSQueryProcessor


class TPCDSQueryExecutor:
    def __init__(self, framework: str = "delta", scale_factor: int = 1, tier: str = "all"):
        self.framework = framework.lower()
        self.scale_factor = scale_factor
        self.tier = tier.lower()

        self.queries_dir = Path("/opt/spark/configs/queries/tpcds")
        self.table_path = f"/data/silver/{self.framework}/tpcds_sf{scale_factor}"

        # Initialize query processor
        self.processor = TPCDSQueryProcessor(scale_factor)

        # Get queries to execute based on tier
        self.queries_to_run = self._get_queries_for_tier()

        print(f"🎯 Executing {len(self.queries_to_run)} queries (tier: {tier})")

        # Create Spark session based on framework
        self.spark = self._create_spark_session()

        # Register tables
        self._register_tables()

        self.metrics = {
            "framework": self.framework,
            "benchmark": "tpcds",
            "scale_factor": scale_factor,
            "tier": tier,
            "start_time": datetime.now().isoformat(),
            "queries": {}
        }

    def _get_queries_for_tier(self):
        """Get list of queries to run based on tier."""
        metadata_file = Path("/opt/spark/configs/queries/tpcds/queries_metadata.json")

        with open(metadata_file) as f:
            metadata = json.load(f)

        if self.tier == "all":
            # Run all 99 queries
            return list(range(1, 100))

        elif self.tier == "tier1":
            # Run 10 essential queries
            return metadata["tiers"]["tier1_essential"]["queries"]

        elif self.tier == "tier2":
            # Run 15 advanced queries
            return metadata["tiers"]["tier2_advanced"]["queries"]

        elif self.tier == "tier3":
            # Run 10 stress queries
            return metadata["tiers"]["tier3_stress"]["queries"]

        elif self.tier == "lhbench":
            # Run LHBench refresh test queries
            return metadata["lhbench_comparison"]["refresh_test_queries"]["queries"]

        else:
            print(f"⚠️  Unknown tier: {self.tier}, defaulting to tier1")
            return metadata["tiers"]["tier1_essential"]["queries"]

    def _create_spark_session(self):
        """Create Spark session with framework-specific configurations."""
        builder = SparkSession.builder \
            .appName(f"TPC-DS {self.framework.upper()} SF{self.scale_factor}") \
            .master("spark://spark-master:7077") \
            .config("spark.executor.memory", "2500m") \
            .config("spark.executor.cores", "5") \
            .config("spark.cores.max", "10") \
            .config("spark.default.parallelism", "20") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        if self.framework == "delta":
            builder = builder \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        elif self.framework == "iceberg":
            builder = builder \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.local.type", "hadoop") \
                .config("spark.sql.catalog.local.warehouse", self.table_path)

        elif self.framework == "hudi":
            builder = builder \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")

        return builder.getOrCreate()

    def _register_tables(self):
        """Register all TPC-DS tables as views."""
        print(f"📋 Registering TPC-DS tables...")

        tables = [
            # Fact tables
            "store_sales", "store_returns", "catalog_sales", "catalog_returns",
            "web_sales", "web_returns", "inventory",
            # Dimension tables
            "store", "call_center", "catalog_page", "web_site", "web_page",
            "warehouse", "customer", "customer_address", "customer_demographics",
            "date_dim", "time_dim", "item", "promotion",
            "household_demographics", "income_band", "reason", "ship_mode"
        ]

        for table in tables:
            try:
                self._register_table(table)
            except Exception as e:
                print(f"   ⚠️  Warning: Could not register {table}: {e}")

        print(f"   ✓ Tables registered\n")

    def _register_table(self, table_name: str):
        """Register a single table as a view."""
        if self.framework == "delta":
            table_path = f"{self.table_path}/{table_name}"
            self.spark.read.format("delta").load(table_path).createOrReplaceTempView(table_name)

        elif self.framework == "iceberg":
            # Iceberg tables are accessed via catalog
            iceberg_table_name = f"tpcds_{table_name}"
            self.spark.sql(f"CREATE OR REPLACE TEMP VIEW {table_name} AS SELECT * FROM local.{iceberg_table_name}")

        elif self.framework == "hudi":
            table_path = f"{self.table_path}/{table_name}"
            self.spark.read.format("hudi").load(table_path).createOrReplaceTempView(table_name)

    def execute_all_queries(self):
        """Execute all queries in the tier."""
        print(f"\n{'='*60}")
        print(f"TPC-DS Query Execution - {self.framework.upper()}")
        print(f"Scale Factor: {self.scale_factor}")
        print(f"Tier: {self.tier} ({len(self.queries_to_run)} queries)")
        print(f"{'='*60}\n")

        total_start = time.time()
        successful = 0
        failed = 0

        for query_num in self.queries_to_run:
            try:
                self.execute_query(query_num)
                successful += 1
            except Exception as e:
                print(f"   ❌ Failed: {e}")
                failed += 1
                self.metrics["queries"][f"q{query_num:02d}"] = {
                    "query_id": f"q{query_num:02d}",
                    "status": "failed",
                    "error": str(e)
                }

        total_duration = time.time() - total_start

        # Calculate summary statistics
        query_times = [
            m["duration_seconds"]
            for m in self.metrics["queries"].values()
            if m.get("status") == "success"
        ]

        self.metrics["end_time"] = datetime.now().isoformat()
        self.metrics["summary"] = {
            "total_queries": len(self.queries_to_run),
            "successful_queries": successful,
            "failed_queries": failed,
            "total_query_time": round(sum(query_times), 2) if query_times else 0,
            "average_query_time": round(sum(query_times) / len(query_times), 2) if query_times else 0,
            "min_query_time": round(min(query_times), 2) if query_times else 0,
            "max_query_time": round(max(query_times), 2) if query_times else 0,
            "total_execution_time": round(total_duration, 2)
        }

        self.save_metrics()

        print(f"\n{'='*60}")
        print(f"✅ Execution Complete!")
        print(f"   Total Queries: {len(self.queries_to_run)}")
        print(f"   Successful: {successful}")
        print(f"   Failed: {failed}")
        print(f"   Total Time: {total_duration:.2f}s")
        print(f"   Avg Query Time: {self.metrics['summary']['average_query_time']:.2f}s")
        print(f"{'='*60}\n")

        return self.metrics

    def execute_query(self, query_num: int):
        """Execute a single TPC-DS query."""
        query_id = f"q{query_num:02d}"
        query_file = self.queries_dir / f"{query_id}.sql"

        if not query_file.exists():
            raise FileNotFoundError(f"Query file not found: {query_file}")

        print(f"⚡ Executing {query_id}...")

        # Process query (substitute parameters)
        query_text = self.processor.process_query_file(query_file)

        # Validate query
        is_valid, remaining = self.processor.validate_query(query_text)
        if not is_valid:
            print(f"   ⚠️  Warning: {len(remaining)} unsubstituted parameters: {remaining}")

        # Execute query
        start_time = time.time()

        try:
            result = self.spark.sql(query_text)
            row_count = result.count()
            duration = time.time() - start_time

            print(f"   ✓ Completed in {duration:.2f}s ({row_count:,} rows)")

            self.metrics["queries"][query_id] = {
                "query_id": query_id,
                "query_number": query_num,
                "framework": self.framework,
                "status": "success",
                "duration_seconds": round(duration, 2),
                "row_count": row_count
            }

        except Exception as e:
            duration = time.time() - start_time
            print(f"   ❌ Failed in {duration:.2f}s: {str(e)[:100]}")

            self.metrics["queries"][query_id] = {
                "query_id": query_id,
                "query_number": query_num,
                "framework": self.framework,
                "status": "failed",
                "duration_seconds": round(duration, 2),
                "error": str(e)[:500]  # Truncate error
            }
            raise

    def save_metrics(self):
        """Save metrics to JSON."""
        metrics_paths = [
            Path("/data/gold/metrics"),
            Path("/opt/spark/results/metrics")
        ]

        for metrics_dir in metrics_paths:
            metrics_dir.mkdir(parents=True, exist_ok=True)
            metrics_file = metrics_dir / f"gold_{self.framework}_tpcds_sf{self.scale_factor}_{self.tier}.json"

            with open(metrics_file, 'w') as f:
                json.dump(self.metrics, f, indent=2)

            print(f"💾 Metrics saved to {metrics_file}")


def main():
    framework = os.environ.get("FRAMEWORK", "delta").lower()
    scale_factor = int(os.environ.get("TPCDS_SCALE_FACTOR", os.environ.get("TPCH_SCALE_FACTOR", "1")))
    tier = os.environ.get("TIER", "all").lower()

    print(f"\n{'='*60}")
    print(f"  TPC-DS Query Executor")
    print(f"  Framework: {framework.upper()}")
    print(f"  Scale Factor: {scale_factor}")
    print(f"  Tier: {tier}")
    print(f"{'='*60}\n")

    try:
        executor = TPCDSQueryExecutor(framework, scale_factor, tier)
        result = executor.execute_all_queries()

        print("\n✅ All queries executed successfully!")
        sys.exit(0)

    except Exception as e:
        print(f"\n❌ Execution failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
