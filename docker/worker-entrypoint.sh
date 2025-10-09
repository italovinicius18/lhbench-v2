#!/bin/bash
set -e

# Default values
CORES=${SPARK_WORKER_CORES:-5}
MEMORY=${SPARK_WORKER_MEMORY:-3g}

echo "Starting Spark Worker with $CORES cores and $MEMORY memory"

exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    spark://spark-master:7077 \
    --cores "$CORES" \
    --memory "$MEMORY"
