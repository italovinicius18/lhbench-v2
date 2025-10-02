#!/usr/bin/env python3
"""
Iceberg Silver Phase - Convert Bronze Parquet to Iceberg Tables
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession

# Add common modules to path
sys.path.insert(0, '/opt/spark/jobs/common')
from spark_session_builder import SparkSessionBuilder


class IcebergConverter:
    """Converts Bronze Parquet data to Iceberg tables"""

    TPCH_TABLES = [
        'customer', 'orders', 'lineitem', 'supplier',
        'part', 'partsupp', 'nation', 'region'
    ]

    def __init__(self, scale_factor: int = 1):
        self.scale_factor = scale_factor
        self.bronze_path = f"/data/bronze/tpch/sf{scale_factor}"
        self.silver_path = f"/data/silver/iceberg/sf{scale_factor}"
        self.warehouse_path = f"/data/silver/iceberg/warehouse"

        # Build Spark session with Iceberg support
        self.spark = SparkSessionBuilder.build_session(
            app_name=f"Iceberg-Conversion-SF{scale_factor}",
            framework="iceberg"
        )

        # Configure Iceberg catalog
        self.spark.conf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        self.spark.conf.set("spark.sql.catalog.iceberg.type", "hadoop")
        self.spark.conf.set("spark.sql.catalog.iceberg.warehouse", self.warehouse_path)

        self.metrics = {
            'framework': 'iceberg',
            'scale_factor': scale_factor,
            'start_time': datetime.utcnow().isoformat(),
            'tables': {}
        }

    def convert_table(self, table_name: str):
        """Convert a single table from Parquet to Iceberg"""
        print(f"📦 Converting {table_name} to Iceberg...")

        start_time = datetime.utcnow()

        try:
            # Read Bronze Parquet
            bronze_table_path = f"{self.bronze_path}/{table_name}"
            df = self.spark.read.parquet(bronze_table_path)

            row_count = df.count()
            print(f"   Read {row_count:,} rows from Bronze")

            # Create Iceberg table
            iceberg_table = f"iceberg.tpch.{table_name}"

            # Drop table if exists
            self.spark.sql(f"DROP TABLE IF EXISTS {iceberg_table}")

            # Create database if not exists
            self.spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.tpch")

            # Write as Iceberg table with partitioning for large tables
            if table_name in ['lineitem', 'orders']:
                # Partition by date columns for better query performance
                if table_name == 'lineitem':
                    df.writeTo(iceberg_table).using("iceberg").createOrReplace()
                else:
                    df.writeTo(iceberg_table).using("iceberg").createOrReplace()
            else:
                df.writeTo(iceberg_table).using("iceberg").createOrReplace()

            # Get table stats
            iceberg_df = self.spark.table(iceberg_table)
            final_count = iceberg_df.count()

            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            # Get file size
            table_path = f"{self.warehouse_path}/tpch/{table_name}"
            try:
                files = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                    self.spark.sparkContext._jsc.hadoopConfiguration()
                ).listStatus(
                    self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(table_path)
                )
                total_size = sum([f.getLen() for f in files if f.isFile()])
                size_mb = total_size / (1024 * 1024)
            except:
                size_mb = 0

            self.metrics['tables'][table_name] = {
                'status': 'success',
                'row_count': final_count,
                'duration_seconds': duration,
                'size_mb': round(size_mb, 2),
                'table_location': iceberg_table
            }

            print(f"   ✅ {table_name}: {final_count:,} rows in {duration:.2f}s ({size_mb:.2f} MB)")

        except Exception as e:
            print(f"   ❌ Failed to convert {table_name}: {str(e)}")
            self.metrics['tables'][table_name] = {
                'status': 'failed',
                'error': str(e)
            }
            raise

    def convert_all_tables(self):
        """Convert all TPC-H tables"""
        print(f"\n🚀 Starting Iceberg conversion for SF{self.scale_factor}\n")

        for table in self.TPCH_TABLES:
            self.convert_table(table)

        self.metrics['end_time'] = datetime.utcnow().isoformat()
        self.metrics['total_duration'] = (
            datetime.fromisoformat(self.metrics['end_time']) -
            datetime.fromisoformat(self.metrics['start_time'])
        ).total_seconds()

        # Save metrics to both locations
        metrics_paths = [
            Path("/data/gold/metrics"),
            Path("/opt/spark/results/metrics")
        ]

        for metrics_dir in metrics_paths:
            metrics_dir.mkdir(parents=True, exist_ok=True)
            metrics_file = metrics_dir / f"silver_iceberg_sf{self.scale_factor}.json"
            with open(metrics_file, 'w') as f:
                json.dump(self.metrics, f, indent=2)

        print(f"\n✅ Iceberg conversion completed in {self.metrics['total_duration']:.2f}s")
        print(f"📊 Metrics saved to /data/gold/metrics/ and /opt/spark/results/metrics/")

        return self.metrics

    def close(self):
        """Stop Spark session"""
        self.spark.stop()


if __name__ == "__main__":
    scale_factor = int(os.getenv('TPCH_SCALE_FACTOR', '1'))

    converter = IcebergConverter(scale_factor=scale_factor)

    try:
        metrics = converter.convert_all_tables()
        print("\n" + "="*60)
        print("ICEBERG CONVERSION SUMMARY")
        print("="*60)
        print(json.dumps(metrics, indent=2))
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Iceberg conversion failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        converter.close()
