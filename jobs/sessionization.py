from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, sum as _sum, concat_ws
from pyspark.sql.window import Window


def main():
    print("Initializing Spark Session for Sessionization...")
    spark = (
        SparkSession.builder
        .appName("Stage2_Sessionization")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .getOrCreate()
    )

    num_partitions = 8
    base_path = "/home/jovyan/work"

    print("Loading cleaned events Parquet...")
    events_clean = spark.read.parquet(f"{base_path}/cleaned/events_clean") \
        .repartition(num_partitions, "user_id")

    print("Defining PySpark Window Specifications...")
    window_spec_lag = Window.partitionBy("user_id").orderBy("timestamp")
    window_spec_cum = Window.partitionBy("user_id").orderBy("timestamp") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    print("Calculating time gaps and flagging new sessions...")
    events_with_lag = events_clean.withColumn(
        "prev_timestamp",
        lag("timestamp").over(window_spec_lag)
    )

    events_with_gap = events_with_lag.withColumn(
        "gap_seconds",
        when(col("prev_timestamp").isNull(), 0)
        .otherwise(col("timestamp").cast("long") - col("prev_timestamp").cast("long"))
    )

    events_with_flags = events_with_gap.withColumn(
        "is_new_session",
        when((col("gap_seconds") > 1800) | (col("prev_timestamp").isNull()), 1).otherwise(0)
    )

    print("Building unique session IDs...")
    events_with_session_num = events_with_flags.withColumn(
        "session_number",
        _sum("is_new_session").over(window_spec_cum)
    )

    events_final = events_with_session_num.withColumn(
        "derived_session_id",
        concat_ws("_", col("user_id"), col("session_number"))
    )

    events_final = events_final.withColumn("session_id", col("derived_session_id")) \
        .drop("prev_timestamp", "gap_seconds", "is_new_session", "session_number", "derived_session_id")

    print("Saving sessionized data to output folder...")
    events_final.repartition(num_partitions, "user_id") \
        .write.mode("overwrite").parquet(f"{base_path}/output/events_with_sessions")

    print("Stage 2 Sessionization Complete!")
    spark.stop()


if __name__ == "__main__":
    main()