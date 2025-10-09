# TPC-DS Queries

This directory contains the 99 TPC-DS benchmark queries adapted for Spark SQL.

## Source

Queries were obtained from the DuckDB TPC-DS implementation:
https://github.com/duckdb/duckdb/tree/main/extension/tpcds/dsdgen/queries

## Modifications

The original DuckDB queries use double-quoted column aliases (e.g., `AS "order count"`), which are incompatible with Spark SQL. These have been converted to backtick-quoted aliases (e.g., `` AS `order count` ``) for Spark compatibility.

### Affected Queries
- q16, q32, q50, q62, q92, q94, q95, q99

## Structure

```
tpcds/
├── q01.sql through q99.sql  # 99 TPC-DS queries
└── queries_metadata.json     # Query metadata and tier definitions
```

## Tiers

Queries are organized into tiers for testing:

- **tier1_essential**: 10 core queries (q03, q07, q19, q34, q42, q52, q55, q59, q73, q79)
- **tier2_advanced**: 15 complex queries
- **tier3_stress**: 10 performance-intensive queries
- **all**: All 99 queries

## Usage

Queries are automatically loaded by the `tpcds_query_executor.py` script using the `TPCDSQueryProcessor` class.

Example:
```bash
# Run tier1 queries on Delta Lake
TPCDS_SCALE_FACTOR=1 FRAMEWORK=delta TIER=tier1 python3 /opt/spark/jobs/gold/tpcds_query_executor.py

# Run all queries on Iceberg
TPCDS_SCALE_FACTOR=1 FRAMEWORK=iceberg TIER=all python3 /opt/spark/jobs/gold/tpcds_query_executor.py
```
