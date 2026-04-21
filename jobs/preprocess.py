from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lag, when, sum as _sum, concat_ws


def main():
    print("Initializing Spark Session for Sessionization...")
    spark = SparkSession.builder.appName("Stage2_Sessionization").getOrCreate()
    num_partitions = int(spark.conf.get("spark.sql.shuffle.partitions", "8"))
    base_path = "/opt/bitnami/spark/project"

    print("Loading cleaned events Parquet...")
    events_clean = spark.read.parquet(f"{base_path}/cleaned/events_clean")

    print("Defining PySpark Window Specifications...")
    # Window 1: Look at previous row
    window_spec_lag = Window.partitionBy("user_id").orderBy("timestamp")

    # Window 2: Look at all rows from the beginning of the user's history up to the current row
    window_spec_cum = Window.partitionBy("user_id").orderBy("timestamp") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    print("Calculating time gaps and flagging new sessions...")
    # 1. Get the previous event's timestamp
    events_with_lag = events_clean.withColumn("prev_timestamp", lag("timestamp").over(window_spec_lag))

    # 2. Calculate the gap in seconds (cast timestamps to long integer seconds)
    events_with_gap = events_with_lag.withColumn(
        "gap_seconds",
        when(col("prev_timestamp").isNull(), 0)
        .otherwise(col("timestamp").cast("long") - col("prev_timestamp").cast("long"))
    )

    # 3. Flag as 1 if gap > 30 mins (1800s) or if it is the first event ever
    events_with_flags = events_with_gap.withColumn(
        "is_new_session",
        when((col("gap_seconds") > 1800) | (col("prev_timestamp").isNull()), 1).otherwise(0)
    )

    print("Building unique session IDs...")
    # 4. Calculate cumulative sum and concatenate directly into the final session_id
    events_final = events_with_flags.withColumn(
        "session_id",
        concat_ws("_", col("user_id"), _sum("is_new_session").over(window_spec_cum))
    ).drop("prev_timestamp", "gap_seconds", "is_new_session")

    print("Saving sessionized data to output folder...")
    # We maintain the partition by customer_id for the upcoming Attribution join!
    events_final.repartition(num_partitions, "user_id") \
        .write.mode("overwrite").parquet(f"{base_path}/output/events_with_sessions")

    print("Stage 2 Sessionization Complete!")
    spark.stop()


if __name__ == "__main__":
    main()
