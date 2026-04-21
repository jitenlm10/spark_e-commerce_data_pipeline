# Exit immediately if any command fails
$ErrorActionPreference = "Stop"

Write-Host "STARTING E-COMMERCE DATA PIPELINE" -ForegroundColor Cyan

Write-Host "-> [1/5] Running Preprocessing..." -ForegroundColor Yellow
docker exec spark-master spark-submit --master spark://spark-master:7077 --num-executors 2 --executor-cores 2 --executor-memory 2000m /opt/bitnami/spark/project/jobs/preprocess.py

Write-Host "-> [2/5] Running Sessionization..." -ForegroundColor Yellow
docker exec spark-master spark-submit --master spark://spark-master:7077 --num-executors 2 --executor-cores 2 --executor-memory 2000m /opt/bitnami/spark/project/jobs/sessionization.py

Write-Host "-> [3/5] Running Funnel Analysis..." -ForegroundColor Yellow
docker exec spark-master spark-submit --master spark://spark-master:7077 --num-executors 2 --executor-cores 2 --executor-memory 2000m /opt/bitnami/spark/project/jobs/funnel_analysis.py

Write-Host "-> [4/5] Running Attribution (Sort-Merge Join)..." -ForegroundColor Yellow
docker exec spark-master spark-submit --master spark://spark-master:7077 --num-executors 2 --executor-cores 2 --executor-memory 2000m --conf spark.sql.shuffle.partitions=8 /opt/bitnami/spark/project/jobs/attribution.py

Write-Host "-> [5/5] Running Anomaly Detection..." -ForegroundColor Yellow
docker exec spark-master spark-submit --master spark://spark-master:7077 --num-executors 2 --executor-cores 2 --executor-memory 2000m /opt/bitnami/spark/project/jobs/anomaly_detection.py

Write-Host "PIPELINE COMPLETED SUCCESSFULLY" -ForegroundColor Green
