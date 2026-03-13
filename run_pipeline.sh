#!/bin/bash

# Exit immediately if any command fails
set -e

echo "====================================================="
echo "🚀 Starting E-commerce Analytics Spark Pipeline"
echo "====================================================="

# Define common spark-submit flags for our baseline 2-worker setup
# Note: We allocate 1500m to executors to safely stay under the 2GB container limit
SPARK_SUBMIT_ARGS="--master spark://spark-master:7077 \
  --num-executors 2 \
  --executor-cores 2 \
  --executor-memory 1500m \
  --driver-memory 1g \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.default.parallelism=8"

# The path to the jobs folder INSIDE the Docker container
JOB_PATH="/opt/bitnami/spark/project/jobs"

echo ""
echo "➔ Stage 1: Preprocessing & Catalog Join"
# The 'time' command will output the wall-clock runtime for your scale experiments
time docker exec spark-master spark-submit $SPARK_SUBMIT_ARGS $JOB_PATH/preprocess.py

echo ""
echo "➔ Stage 2: Sessionization (Parallel Window Functions)"
time docker exec spark-master spark-submit $SPARK_SUBMIT_ARGS $JOB_PATH/sessionization.py

echo ""
echo "➔ Stage 3: Funnel Analysis (Shuffle Operations)"
time docker exec spark-master spark-submit $SPARK_SUBMIT_ARGS $JOB_PATH/funnel_analysis.py

echo ""
echo "➔ Stage 4: Attribution (Distributed Sort-Merge Join)"
time docker exec spark-master spark-submit $SPARK_SUBMIT_ARGS $JOB_PATH/attribution.py

echo ""
echo "➔ Stage 5: Anomaly Detection (Rolling Aggregations)"
time docker exec spark-master spark-submit $SPARK_SUBMIT_ARGS $JOB_PATH/anomaly_detection.py

echo ""
echo "====================================================="
echo "✅ Pipeline Execution Completed Successfully!"
echo "Open Jupyter at http://localhost:8888 to view results."
echo "====================================================="