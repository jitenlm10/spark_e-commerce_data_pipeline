from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

print("Initializing Spark Session for Attribution Analysis...")

spark = SparkSession.builder \
    .appName("AttributionAnalysis") \
    .getOrCreate()

print("Loading sessionized data...")

sessions_df = spark.read.parquet("output/sessionized")

print("Computing attribution by referrer...")

attribution_df = sessions_df.groupBy("referrer").agg(
    count("session_id").alias("total_sessions")
).orderBy(col("total_sessions").desc())

print("Saving attribution results...")

attribution_df.write.mode("overwrite").parquet("output/attribution_metrics")

print("Attribution Analysis Complete!")