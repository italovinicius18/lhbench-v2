#!/usr/bin/env python3
"""
Delta Lake Silver Phase - Convert Bronze Parquet to Delta Tables
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


class DeltaConverter:
    """Converts Bronze Parquet data to Delta Lake tables"""

    TPCH_TABLES = [
        'customer', 'orders', 'lineitem', 'supplier',
        'part', 'partsupp', 'nation', 'region'
    ]

    def __init__(self, scale_factor: int = 1):
        self.scale_factor = scale_factor
        self.bronze_path = f"/data/bronze/tpch/sf{scale_factor}"
        self.silver_path = f"/data/silver/delta/sf{scale_factor}"

        # Build Spark session with Delta support
        self.spark = SparkSessionBuilder.build_session(
            app_name=f"Delta-Conversion-SF{scale_factor}",
            framework="delta"
        )

        self.metrics = {
            'framework': 'delta',
            'scale_factor': scale_factor,
            'start_time': datetime.utcnow().isoformat(),
            'tables': {}
        }

    def convert_table(self, table_name: str):
        """Convert a single table from Parquet to Delta Lake"""
        print(f"📦 Converting {table_name} to Delta Lake...")

        start_time = datetime.utcnow()

        try:
            # Read Bronze Parquet
            bronze_table_path = f"{self.bronze_path}/{table_name}"
            df = self.spark.read.parquet(bronze_table_path)

            row_count = df.count()
            print(f"   Read {row_count:,} rows from Bronze")

            # Create Delta table path
            delta_table_path = f"{self.silver_path}/{table_name}"

            # Write as Delta table with optimizations
            writer = df.write.format("delta").mode("overwrite")

            # Add partitioning for large tables
            if table_name in ['lineitem', 'orders']:
                # Optimize with Z-ordering on frequently queried columns
                writer = writer.option("overwriteSchema", "true")

            writer.save(delta_table_path)

            # Optimize Delta table
            from delta.tables import DeltaTable
            delta_table = DeltaTable.forPath(self.spark, delta_table_path)

            # Run optimize
            delta_table.optimize().executeCompaction()

            # Get table stats
            delta_df = self.spark.read.format("delta").load(delta_table_path)
            final_count = delta_df.count()

            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            # Get file size
            try:
                hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
                fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
                path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(delta_table_path)
                content = fs.listStatus(path)
                total_size = sum([
                    f.getLen() for f in content
                    if f.isFile() and f.getPath().getName().endswith('.parquet')
                ])
                size_mb = total_size / (1024 * 1024)
            except:
                size_mb = 0

            self.metrics['tables'][table_name] = {
                'status': 'success',
                'row_count': final_count,
                'duration_seconds': duration,
                'size_mb': round(size_mb, 2),
                'table_location': delta_table_path
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
        print(f"\n🚀 Starting Delta Lake conversion for SF{self.scale_factor}\n")

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
            metrics_file = metrics_dir / f"silver_delta_sf{self.scale_factor}.json"
            with open(metrics_file, 'w') as f:
                json.dump(self.metrics, f, indent=2)

        print(f"\n✅ Delta Lake conversion completed in {self.metrics['total_duration']:.2f}s")
        print(f"📊 Metrics saved to /data/gold/metrics/ and /opt/spark/results/metrics/")

        return self.metrics

    def close(self):
        """Stop Spark session"""
        self.spark.stop()


if __name__ == "__main__":
    scale_factor = int(os.getenv('TPCH_SCALE_FACTOR', '1'))

    converter = DeltaConverter(scale_factor=scale_factor)

    try:
        metrics = converter.convert_all_tables()
        print("\n" + "="*60)
        print("DELTA LAKE CONVERSION SUMMARY")
        print("="*60)
        print(json.dumps(metrics, indent=2))
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Delta Lake conversion failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        converter.close()
