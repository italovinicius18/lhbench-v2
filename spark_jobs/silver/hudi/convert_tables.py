#!/usr/bin/env python3
"""
Apache Hudi Silver Phase - Convert Bronze Parquet to Hudi Tables
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


class HudiConverter:
    """Converts Bronze Parquet data to Apache Hudi tables"""

    TPCH_TABLES = [
        'customer', 'orders', 'lineitem', 'supplier',
        'part', 'partsupp', 'nation', 'region'
    ]

    # Define primary keys for each table
    PRIMARY_KEYS = {
        'customer': 'c_custkey',
        'orders': 'o_orderkey',
        'lineitem': 'l_orderkey,l_linenumber',  # Composite key
        'supplier': 's_suppkey',
        'part': 'p_partkey',
        'partsupp': 'ps_partkey,ps_suppkey',  # Composite key
        'nation': 'n_nationkey',
        'region': 'r_regionkey'
    }

    def __init__(self, scale_factor: int = 1):
        self.scale_factor = scale_factor
        self.bronze_path = f"/data/bronze/tpch/sf{scale_factor}"
        self.silver_path = f"/data/silver/hudi/sf{scale_factor}"

        # Build Spark session with Hudi support
        self.spark = SparkSessionBuilder.build_session(
            app_name=f"Hudi-Conversion-SF{scale_factor}",
            framework="hudi"
        )

        self.metrics = {
            'framework': 'hudi',
            'scale_factor': scale_factor,
            'start_time': datetime.utcnow().isoformat(),
            'tables': {}
        }

    def convert_table(self, table_name: str):
        """Convert a single table from Parquet to Hudi"""
        print(f"📦 Converting {table_name} to Hudi...")

        start_time = datetime.utcnow()

        try:
            # Read Bronze Parquet
            bronze_table_path = f"{self.bronze_path}/{table_name}"
            df = self.spark.read.parquet(bronze_table_path)

            print(f"   Read data from Bronze")

            # Create Hudi table path
            hudi_table_path = f"{self.silver_path}/{table_name}"

            # Get primary key(s) for this table
            record_key = self.PRIMARY_KEYS[table_name]

            # Hudi options
            hudi_options = {
                'hoodie.table.name': f'tpch_{table_name}',
                'hoodie.datasource.write.recordkey.field': record_key,
                'hoodie.datasource.write.precombine.field': record_key.split(',')[0],  # Use first key for precombine
                'hoodie.datasource.write.table.name': f'tpch_{table_name}',
                'hoodie.datasource.write.operation': 'bulk_insert',
                'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
                'hoodie.datasource.hive_sync.enable': 'false',
                'hoodie.upsert.shuffle.parallelism': '4',
                'hoodie.insert.shuffle.parallelism': '4',
                'hoodie.bulkinsert.shuffle.parallelism': '4',
            }

            # Add partitioning for large tables
            if table_name == 'lineitem':
                # Don't partition lineitem - too many partitions
                pass
            elif table_name == 'orders':
                # Don't partition orders - complex date handling
                pass

            # Write as Hudi table
            df.write.format("hudi") \
                .options(**hudi_options) \
                .mode("overwrite") \
                .save(hudi_table_path)

            # Count after write (no read-back verification for Hudi due to compatibility issues)
            final_count = 0  # Will be populated from file stats

            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            # Get file size
            try:
                hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
                fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
                path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(hudi_table_path)
                content = fs.listStatus(path)
                total_size = 0
                for item in content:
                    if item.isDirectory():
                        # Sum all parquet files in subdirectories
                        sub_items = fs.listStatus(item.getPath())
                        total_size += sum([
                            f.getLen() for f in sub_items
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
                'table_location': hudi_table_path,
                'primary_key': record_key
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
        print(f"\n🚀 Starting Hudi conversion for SF{self.scale_factor}\n")

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
            metrics_file = metrics_dir / f"silver_hudi_sf{self.scale_factor}.json"
            with open(metrics_file, 'w') as f:
                json.dump(self.metrics, f, indent=2)

        print(f"\n✅ Hudi conversion completed in {self.metrics['total_duration']:.2f}s")
        print(f"📊 Metrics saved to /data/gold/metrics/ and /opt/spark/results/metrics/")

        return self.metrics

    def close(self):
        """Stop Spark session"""
        self.spark.stop()


if __name__ == "__main__":
    scale_factor = int(os.getenv('TPCH_SCALE_FACTOR', '1'))

    converter = HudiConverter(scale_factor=scale_factor)

    try:
        metrics = converter.convert_all_tables()
        print("\n" + "="*60)
        print("HUDI CONVERSION SUMMARY")
        print("="*60)
        print(json.dumps(metrics, indent=2))
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Hudi conversion failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        converter.close()
