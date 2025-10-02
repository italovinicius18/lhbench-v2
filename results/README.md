# Results Directory

This directory contains benchmark execution results.

## Structure

```
results/
├── metrics/     # JSON metrics from benchmark runs (committed to git)
├── logs/        # Execution logs (ignored by git)
└── reports/     # Generated reports (ignored by git)
```

## Metrics Files

Metrics are saved in `metrics/` directory with the following naming convention:

- `bronze_sf{N}.json` - Bronze phase (data generation)
- `silver_{framework}_sf{N}.json` - Silver phase (conversion to lakehouse format)
- `gold_{framework}_sf{N}.json` - Gold phase (TPC-H query execution)

Where:
- `{framework}` = `delta`, `iceberg`, or `hudi`
- `{N}` = scale factor (1, 3, 10, 100, etc.)

## Example

```bash
# View latest Gold phase results
cat results/metrics/gold_delta_sf1.json | jq '.summary'
cat results/metrics/gold_iceberg_sf1.json | jq '.summary'

# Compare frameworks
jq -s '.[0].summary, .[1].summary' \
  results/metrics/gold_delta_sf1.json \
  results/metrics/gold_iceberg_sf1.json
```

## Viewing Results

All metrics are also saved to `/mnt/c/Users/italo/WSL_DATA/lakehouse-data/gold/metrics/` for backup purposes.
