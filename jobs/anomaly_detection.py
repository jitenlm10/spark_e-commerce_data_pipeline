from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, when, lit
from pyspark.sql.window import Window

print("Initializing Spark Session for Anomaly Detection...")

spark = SparkSession.builder \
    .appName("AnomalyDetection") \
    .getOrCreate()

base_path = Path(__file__).resolve().parent.parent

print("Loading funnel metrics...")

funnel_df = spark.read.parquet(str(base_path / "output" / "funnel_metrics"))

print("Building rolling baseline...")

window_spec = Window.partitionBy("category").orderBy("date").rowsBetween(-7, -1)

baseline_df = funnel_df.withColumn(
    "rolling_avg_conversion",
    avg("overall_conversion_rate").over(window_spec)
).withColumn(
    "rolling_std_conversion",
    stddev("overall_conversion_rate").over(window_spec)
)

print("Computing deviation and anomaly flags...")

result_df = baseline_df.withColumn(
    "z_score",
    when(
        col("rolling_std_conversion").isNull() | (col("rolling_std_conversion") == 0),
        lit(None)
    ).otherwise(
        (col("overall_conversion_rate") - col("rolling_avg_conversion")) / col("rolling_std_conversion")
    )
)

result_df = result_df.withColumn(
    "anomaly_flag",
    when(col("z_score") > 2, "spike").otherwise("normal")
)

print("Filtering anomaly rows only...")

anomalies_df = result_df.filter(col("z_score") > 2)

print("Saving full anomaly analysis...")

result_df.write.mode("overwrite").parquet(
    str(base_path / "output" / "anomaly_metrics")
)

print("Saving detected anomalies only...")

anomalies_df.write.mode("overwrite").parquet(
    str(base_path / "output" / "anomalies_only")
)

print("Stage 5 Anomaly Detection Complete!")

spark.stop()