# Quick Start Guide

## One-Command Execution

```bash
make
```

That's it! This will:
1. ✓ Setup environment and directories
2. ✓ Build Docker images
3. ✓ Start Spark cluster (1 master + 2 workers)
4. ✓ Generate TPC-DS data (24 tables, SF=1)
5. ✓ Convert to Delta Lake
6. ✓ Convert to Apache Iceberg
7. ✓ Convert to Apache Hudi
8. ✓ Run 99 queries on Delta
9. ✓ Run 99 queries on Iceberg
10. ✓ Run 99 queries on Hudi
11. ✓ Copy results to `results/metrics/`

## Configuration

Edit `.env` before running:

```bash
# Scale factor (1=3GB, 10=30GB)
TPCDS_SCALE_FACTOR=1

# Query tier (tier1, tier2, tier3, all)
TPCDS_QUERY_TIER=all
```

## Custom Scale Factor

```bash
make SF=10
```

## Individual Phases

```bash
make build    # Build images only
make up       # Start cluster only
make bronze   # Generate data only
make silver   # Convert formats only
make gold     # Run queries only
```

## Query Tiers

```bash
make tier1    # 10 queries (~2 min)
make tier2    # 15 queries (~5 min)
make tier3    # 10 queries (~10 min)
make lhbench  # 5 queries (~1 min)
```

## Results

Results are in `results/metrics/*.json`:

```bash
ls results/metrics/
# gold_delta_tpcds_sf1_all.json
# gold_iceberg_tpcds_sf1_all.json
# gold_hudi_tpcds_sf1_all.json
```

## Cleanup

```bash
make down         # Stop cluster
make clean        # Stop + remove containers
make clean-all    # Complete cleanup
```

## Monitoring

- Spark UI: http://localhost:8080
- Worker 1: http://localhost:8081
- Worker 2: http://localhost:8082

## Troubleshooting

```bash
make status   # Check cluster status
make logs     # View logs
make shell    # Open shell in master
```

## Requirements

- Docker & Docker Compose
- 16GB RAM (minimum)
- 50GB disk space
- Linux/WSL2/macOS
