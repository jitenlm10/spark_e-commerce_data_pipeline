from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ViewAnomalies").getOrCreate()

print("Reading full anomaly metrics...")
full_df = spark.read.parquet("output/anomaly_metrics")
full_df.show(50, truncate=False)

print("Reading anomaly-only results...")
anom_df = spark.read.parquet("output/anomalies_only")
anom_df.show(50, truncate=False)

spark.stop()