#!/usr/bin/env python3
"""
TPC-DS Data Generation using DuckDB
Generates all 24 TPC-DS tables using DuckDB's native tpcds extension
"""

import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime

def main():
    scale_factor = int(os.environ.get("TPCDS_SCALE_FACTOR", "1"))
    output_path = f"/data/bronze/tpcds/sf{scale_factor}"

    print("=" * 60)
    print(f"  TPC-DS Data Generation (DuckDB)")
    print(f"  Scale Factor: {scale_factor}")
    print(f"  Output: {output_path}")
    print("=" * 60)
    print()

    try:
        import duckdb
    except ImportError:
        print("❌ DuckDB not installed. Installing...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb", "-q"])
        import duckdb

    # Create output directory
    Path(output_path).mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB
    print("📊 Initializing DuckDB...")
    con = duckdb.connect(':memory:')

    # Install and load tpcds extension
    print("📦 Loading TPC-DS extension...")
    con.execute("INSTALL tpcds;")
    con.execute("LOAD tpcds;")

    # Generate TPC-DS data
    print(f"⚡ Generating TPC-DS data at scale factor {scale_factor}...")
    start_time = time.time()

    con.execute(f"CALL dsdgen(sf = {scale_factor});")

    generation_time = time.time() - start_time
    print(f"   ✓ Data generated in {generation_time:.2f}s")

    # Get list of all TPC-DS tables
    tables = [
        # Fact tables
        "store_sales", "store_returns",
        "catalog_sales", "catalog_returns",
        "web_sales", "web_returns",
        "inventory",
        # Dimension tables
        "store", "call_center", "catalog_page", "web_site", "web_page",
        "warehouse", "customer", "customer_address", "customer_demographics",
        "date_dim", "time_dim", "item", "promotion",
        "household_demographics", "income_band", "reason", "ship_mode"
    ]

    print(f"\n📁 Exporting {len(tables)} tables to Parquet...")
    print()

    metrics = {
        "scale_factor": scale_factor,
        "start_time": datetime.now().isoformat(),
        "generation_time_seconds": round(generation_time, 2),
        "tables": {}
    }

    export_start = time.time()
    successful = 0
    failed = 0

    for table in tables:
        try:
            table_start = time.time()
            table_output = f"{output_path}/{table}"

            # Create directory for table
            Path(table_output).mkdir(parents=True, exist_ok=True)

            # Get row count
            row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

            # Export to Parquet (write to directory with data.parquet file)
            table_file = f"{table_output}/data.parquet"
            con.execute(f"""
                COPY {table} TO '{table_file}'
                (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000);
            """)

            table_duration = time.time() - table_start

            # Get file size
            parquet_files = list(Path(table_output).glob("*.parquet"))
            size_bytes = sum(f.stat().st_size for f in parquet_files) if parquet_files else 0
            size_mb = size_bytes / (1024 * 1024)

            print(f"   ✅ {table:25} {row_count:>12,} rows  {size_mb:>8.2f} MB  {table_duration:>6.2f}s")

            metrics["tables"][table] = {
                "status": "success",
                "rows": row_count,
                "size_mb": round(size_mb, 2),
                "duration_seconds": round(table_duration, 2)
            }

            successful += 1

        except Exception as e:
            print(f"   ❌ {table:25} FAILED: {e}")
            metrics["tables"][table] = {
                "status": "failed",
                "error": str(e)
            }
            failed += 1

    export_duration = time.time() - export_start
    total_duration = time.time() - start_time

    metrics["end_time"] = datetime.now().isoformat()
    metrics["export_time_seconds"] = round(export_duration, 2)
    metrics["total_duration_seconds"] = round(total_duration, 2)
    metrics["summary"] = {
        "total_tables": len(tables),
        "successful": successful,
        "failed": failed
    }

    # Save metrics
    metrics_paths = [
        Path("/data/gold/metrics"),
        Path("/opt/spark/results/metrics")
    ]

    for metrics_dir in metrics_paths:
        metrics_dir.mkdir(parents=True, exist_ok=True)
        metrics_file = metrics_dir / f"bronze_tpcds_sf{scale_factor}.json"

        with open(metrics_file, 'w') as f:
            json.dump(metrics, f, indent=2)

        print(f"\n💾 Metrics saved to {metrics_file}")

    print()
    print("=" * 60)
    print(f"✅ TPC-DS data generation complete!")
    print(f"   Total time: {total_duration:.2f}s")
    print(f"   Tables: {successful}/{len(tables)}")
    print(f"   Metrics: /data/gold/metrics/bronze_tpcds_sf{scale_factor}.json")
    print("=" * 60)
    print()

    if failed > 0:
        sys.exit(1)

    sys.exit(0)

if __name__ == "__main__":
    main()
