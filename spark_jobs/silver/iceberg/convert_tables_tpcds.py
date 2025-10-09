#!/usr/bin/env python3
"""
TPC-DS Silver Layer Converter - Apache Iceberg
Converts Bronze Parquet tables to Iceberg format (24 tables)
"""

import os
import sys
import time
import json
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession


class IcebergTPCDSConverter:
    def __init__(self, scale_factor: int = 1):
        self.scale_factor = scale_factor
        self.bronze_path = f"/data/bronze/tpcds/sf{scale_factor}"
        self.iceberg_path = f"/data/silver/iceberg/tpcds_sf{scale_factor}"

        # TPC-DS tables (24 total)
        self.fact_tables = [
            "store_sales", "store_returns",
            "catalog_sales", "catalog_returns",
            "web_sales", "web_returns",
            "inventory"
        ]

        self.dimension_tables = [
            "store", "call_center", "catalog_page", "web_site", "web_page",
            "warehouse", "customer", "customer_address", "customer_demographics",
            "date_dim", "time_dim", "item", "promotion",
            "household_demographics", "income_band", "reason", "ship_mode"
        ]

        self.all_tables = self.fact_tables + self.dimension_tables

        # Disable partitioning for SF=1 to avoid too many small files and OOM errors
        # Partitioning configuration (disabled for SF < 10)
        self.partition_columns = {} if scale_factor < 10 else {
            "store_sales": "ss_sold_date_sk",
            "store_returns": "sr_returned_date_sk",
            "catalog_sales": "cs_sold_date_sk",
            "catalog_returns": "cr_returned_date_sk",
            "web_sales": "ws_sold_date_sk",
            "web_returns": "wr_returned_date_sk",
            "inventory": "inv_date_sk",
        }

        self.spark = SparkSession.builder \
            .appName(f"TPC-DS Iceberg Converter SF{scale_factor}") \
            .master("spark://spark-master:7077") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", self.iceberg_path) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.executor.memory", "2500m") \
            .config("spark.executor.cores", "5") \
            .config("spark.cores.max", "10") \
            .config("spark.default.parallelism", "20") \
            .getOrCreate()

        self.metrics = {
            "framework": "iceberg",
            "benchmark": "tpcds",
            "scale_factor": scale_factor,
            "start_time": datetime.now().isoformat(),
            "tables": {},
            "summary": {}
        }

    def convert_all_tables(self):
        """Convert all TPC-DS tables from Bronze to Iceberg."""
        print(f"\n{'='*60}")
        print(f"TPC-DS Apache Iceberg Converter - SF{self.scale_factor}")
        print(f"{'='*60}\n")

        total_start = time.time()
        successful = 0
        failed = 0

        # Convert fact tables first
        print("📊 Converting Fact Tables (7 tables)...")
        for table in self.fact_tables:
            try:
                self.convert_table(table, is_fact=True)
                successful += 1
            except Exception as e:
                print(f"   ❌ Failed: {e}")
                failed += 1
                self.metrics["tables"][table] = {
                    "status": "failed",
                    "error": str(e)
                }

        # Convert dimension tables
        print(f"\n📋 Converting Dimension Tables ({len(self.dimension_tables)} tables)...")
        for table in self.dimension_tables:
            try:
                self.convert_table(table, is_fact=False)
                successful += 1
            except Exception as e:
                print(f"   ❌ Failed: {e}")
                failed += 1
                self.metrics["tables"][table] = {
                    "status": "failed",
                    "error": str(e)
                }

        total_duration = time.time() - total_start

        # Summary
        self.metrics["end_time"] = datetime.now().isoformat()
        self.metrics["summary"] = {
            "total_tables": len(self.all_tables),
            "successful": successful,
            "failed": failed,
            "total_duration_seconds": round(total_duration, 2),
            "average_per_table_seconds": round(total_duration / len(self.all_tables), 2)
        }

        self.save_metrics()

        print(f"\n{'='*60}")
        print(f"✅ Conversion Complete!")
        print(f"   Total Tables: {len(self.all_tables)}")
        print(f"   Successful: {successful}")
        print(f"   Failed: {failed}")
        print(f"   Total Time: {total_duration:.2f}s")
        print(f"   Avg per Table: {total_duration/len(self.all_tables):.2f}s")
        print(f"{'='*60}\n")

        return self.metrics

    def convert_table(self, table_name: str, is_fact: bool = False):
        """Convert a single table from Bronze Parquet to Iceberg."""
        print(f"\n🔄 Converting {table_name}...")

        start_time = time.time()
        bronze_table_path = f"{self.bronze_path}/{table_name}"
        iceberg_table_name = f"tpcds_{table_name}"

        # Read from Bronze
        print(f"   📖 Reading from Bronze...")
        df = self.spark.read.parquet(bronze_table_path)

        row_count = df.count()
        print(f"   ✓ Read {row_count:,} rows")

        # Drop existing table if exists
        self.spark.sql(f"DROP TABLE IF EXISTS local.{iceberg_table_name}")

        # Get partition column if applicable
        partition_col = self.partition_columns.get(table_name)

        # Write to Iceberg
        print(f"   ✍️  Writing to Iceberg...")

        if partition_col and is_fact:
            # Partition fact tables by date
            print(f"   📂 Partitioning by {partition_col}")

            df.writeTo(f"local.{iceberg_table_name}") \
                .partitionedBy(partition_col) \
                .tableProperty("write.parquet.compression-codec", "snappy") \
                .tableProperty("write.metadata.compression-codec", "gzip") \
                .create()
        else:
            # No partitioning for dimension tables
            df.writeTo(f"local.{iceberg_table_name}") \
                .tableProperty("write.parquet.compression-codec", "snappy") \
                .tableProperty("write.metadata.compression-codec", "gzip") \
                .create()

        duration = time.time() - start_time

        # Get table size
        iceberg_path_real = Path(f"{self.iceberg_path}/{iceberg_table_name}".replace("/data", "/mnt/c/Users/italo/WSL_DATA/lakehouse-data"))
        if iceberg_path_real.exists():
            size_bytes = sum(f.stat().st_size for f in iceberg_path_real.rglob("*.parquet"))
            size_mb = size_bytes / (1024 * 1024)
        else:
            size_mb = 0

        # Store metrics
        self.metrics["tables"][table_name] = {
            "status": "success",
            "table_type": "fact" if is_fact else "dimension",
            "rows": row_count,
            "size_mb": round(size_mb, 2),
            "duration_seconds": round(duration, 2),
            "partitioned": partition_col is not None,
            "partition_column": partition_col,
            "iceberg_table_name": iceberg_table_name
        }

        print(f"   ✅ Completed in {duration:.2f}s ({size_mb:.2f} MB)")

    def save_metrics(self):
        """Save conversion metrics to JSON."""
        metrics_paths = [
            Path("/data/gold/metrics"),
            Path("/opt/spark/results/metrics")
        ]

        for metrics_dir in metrics_paths:
            metrics_dir.mkdir(parents=True, exist_ok=True)
            metrics_file = metrics_dir / f"silver_iceberg_tpcds_sf{self.scale_factor}.json"

            with open(metrics_file, 'w') as f:
                json.dump(self.metrics, f, indent=2)

            print(f"💾 Metrics saved to {metrics_file}")


def main():
    scale_factor = int(os.environ.get("TPCDS_SCALE_FACTOR", os.environ.get("TPCH_SCALE_FACTOR", "1")))

    print(f"\n{'='*60}")
    print(f"  TPC-DS to Apache Iceberg Converter")
    print(f"  Scale Factor: {scale_factor}")
    print(f"  Tables: 24 (7 fact + 17 dimension)")
    print(f"{'='*60}\n")

    try:
        converter = IcebergTPCDSConverter(scale_factor)
        result = converter.convert_all_tables()

        print("\n✅ All tables converted successfully!")
        sys.exit(0)

    except Exception as e:
        print(f"\n❌ Conversion failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
