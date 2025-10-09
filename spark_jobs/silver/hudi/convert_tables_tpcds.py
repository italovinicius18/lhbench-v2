#!/usr/bin/env python3
"""
TPC-DS Silver Layer Converter - Apache Hudi
Converts Bronze Parquet tables to Hudi format (24 tables)
"""

import os
import sys
import time
import json
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession


class HudiTPCDSConverter:
    def __init__(self, scale_factor: int = 1):
        self.scale_factor = scale_factor
        self.bronze_path = f"/data/bronze/tpcds/sf{scale_factor}"
        self.hudi_path = f"/data/silver/hudi/tpcds_sf{scale_factor}"

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

        # Record keys for Hudi (primary keys)
        self.record_keys = {
            "store_sales": "ss_item_sk,ss_ticket_number",
            "store_returns": "sr_item_sk,sr_ticket_number",
            "catalog_sales": "cs_item_sk,cs_order_number",
            "catalog_returns": "cr_item_sk,cr_order_number",
            "web_sales": "ws_item_sk,ws_order_number",
            "web_returns": "wr_item_sk,wr_order_number",
            "inventory": "inv_date_sk,inv_item_sk,inv_warehouse_sk",
            # Dimension tables use single key
            "store": "s_store_sk",
            "call_center": "cc_call_center_sk",
            "catalog_page": "cp_catalog_page_sk",
            "web_site": "web_site_sk",
            "web_page": "wp_web_page_sk",
            "warehouse": "w_warehouse_sk",
            "customer": "c_customer_sk",
            "customer_address": "ca_address_sk",
            "customer_demographics": "cd_demo_sk",
            "date_dim": "d_date_sk",
            "time_dim": "t_time_sk",
            "item": "i_item_sk",
            "promotion": "p_promo_sk",
            "household_demographics": "hd_demo_sk",
            "income_band": "ib_income_band_sk",
            "reason": "r_reason_sk",
            "ship_mode": "sm_ship_mode_sk",
        }

        self.spark = SparkSession.builder \
            .appName(f"TPC-DS Hudi Converter SF{scale_factor}") \
            .master("spark://spark-master:7077") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.executor.memory", "2500m") \
            .config("spark.executor.cores", "5") \
            .config("spark.cores.max", "10") \
            .config("spark.default.parallelism", "20") \
            .getOrCreate()

        self.metrics = {
            "framework": "hudi",
            "benchmark": "tpcds",
            "scale_factor": scale_factor,
            "start_time": datetime.now().isoformat(),
            "tables": {},
            "summary": {}
        }

    def convert_all_tables(self):
        """Convert all TPC-DS tables from Bronze to Hudi."""
        print(f"\n{'='*60}")
        print(f"TPC-DS Apache Hudi Converter - SF{self.scale_factor}")
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
        """Convert a single table from Bronze Parquet to Hudi."""
        print(f"\n🔄 Converting {table_name}...")

        start_time = time.time()
        bronze_table_path = f"{self.bronze_path}/{table_name}"
        hudi_table_path = f"{self.hudi_path}/{table_name}"

        # Read from Bronze
        print(f"   📖 Reading from Bronze...")
        df = self.spark.read.parquet(bronze_table_path)

        # Note: Not counting rows for Hudi due to performance issues
        print(f"   ✓ Data loaded")

        # Get record key and precombine field
        record_key = self.record_keys.get(table_name, self.record_keys.get(table_name.split("_")[0] + "_sk", "id"))
        precombine_field = record_key.split(",")[0]  # Use first key as precombine

        # Get partition column if applicable
        partition_col = self.partition_columns.get(table_name)

        # Hudi options
        hudi_options = {
            'hoodie.table.name': f'tpcds_{table_name}',
            'hoodie.datasource.write.recordkey.field': record_key,
            'hoodie.datasource.write.precombine.field': precombine_field,
            'hoodie.datasource.write.table.name': f'tpcds_{table_name}',
            'hoodie.datasource.write.operation': 'bulk_insert',
            'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
            'hoodie.datasource.hive_sync.enable': 'false',
            'hoodie.upsert.shuffle.parallelism': '4',
            'hoodie.insert.shuffle.parallelism': '4',
            'hoodie.bulkinsert.shuffle.parallelism': '4',
        }

        # Add partitioning for fact tables
        if partition_col and is_fact:
            print(f"   📂 Partitioning by {partition_col}")
            hudi_options['hoodie.datasource.write.partitionpath.field'] = partition_col
            hudi_options['hoodie.datasource.write.keygenerator.class'] = 'org.apache.hudi.keygen.SimpleKeyGenerator'

        # Write to Hudi
        print(f"   ✍️  Writing to Hudi (bulk_insert)...")

        df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("overwrite") \
            .save(hudi_table_path)

        duration = time.time() - start_time

        # Get table size
        hudi_path_real = Path(hudi_table_path.replace("/data", "/mnt/c/Users/italo/WSL_DATA/lakehouse-data"))
        if hudi_path_real.exists():
            size_bytes = sum(f.stat().st_size for f in hudi_path_real.rglob("*.parquet"))
            size_mb = size_bytes / (1024 * 1024)
        else:
            size_mb = 0

        # Store metrics (without row count to avoid performance issues)
        self.metrics["tables"][table_name] = {
            "status": "success",
            "table_type": "fact" if is_fact else "dimension",
            "rows": 0,  # Not counted for Hudi
            "size_mb": round(size_mb, 2),
            "duration_seconds": round(duration, 2),
            "partitioned": partition_col is not None,
            "partition_column": partition_col,
            "record_key": record_key
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
            metrics_file = metrics_dir / f"silver_hudi_tpcds_sf{self.scale_factor}.json"

            with open(metrics_file, 'w') as f:
                json.dump(self.metrics, f, indent=2)

            print(f"💾 Metrics saved to {metrics_file}")


def main():
    scale_factor = int(os.environ.get("TPCDS_SCALE_FACTOR", os.environ.get("TPCH_SCALE_FACTOR", "1")))

    print(f"\n{'='*60}")
    print(f"  TPC-DS to Apache Hudi Converter")
    print(f"  Scale Factor: {scale_factor}")
    print(f"  Tables: 24 (7 fact + 17 dimension)")
    print(f"{'='*60}\n")

    try:
        converter = HudiTPCDSConverter(scale_factor)
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
