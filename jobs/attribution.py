from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, coalesce, lit


def main():
    print("Initializing Spark Session for Attribution...")
    spark = SparkSession.builder.appName("Stage4_Attribution").getOrCreate()
    num_partitions = int(spark.conf.get("spark.sql.shuffle.partitions", "8"))
    base_path = "/home/jovyan/work"

    print("Loading sessions and orders...")
    events_df = spark.read.parquet(f"{base_path}/output/events_with_sessions")
    orders_df = spark.read.parquet(f"{base_path}/cleaned/orders_clean")

    # ==========================================
    # 1. ISOLATE MARKETING TOUCHPOINTS
    # ==========================================
    print("Filtering for non-direct marketing events...")
    # Keep events with a known referrer, excluding 'direct' traffic
    touchpoints = events_df.filter(
        col("referrer").isNotNull() & (col("referrer") != "direct")
    ).select(
        col("user_id"),
        col("timestamp").alias("event_time"),
        col("referrer").alias("marketing_channel"),
        col("session_id")
    )

    # Rename order timestamp to avoid ambiguity during the join
    orders = orders_df.withColumnRenamed("timestamp", "order_time")

    # ==========================================
    # 2. DISTRIBUTED JOIN & TIME CONSTRAINTS
    # ==========================================
    print("Joining orders to touchpoints and applying 7-day lookback window...")
    # Because both tables were partitioned by user_id in Stage 1, this join is highly optimized
    joined_df = orders.join(touchpoints, on="user_id", how="left")

    # Event must happen BEFORE the order, but <= 7 days (604,800 seconds) prior
    valid_touchpoints = joined_df.filter(
        (col("event_time").cast("long") <= col("order_time").cast("long")) &
        (col("order_time").cast("long") - col("event_time").cast("long") <= 604800)
    )

    # ==========================================
    # 3. LAST-TOUCH RANKING
    # ==========================================
    print("Calculating the most recent touchpoint per order...")
    # Window to rank events per order, sorting by the most recent event first
    window_spec = Window.partitionBy("order_id").orderBy(col("event_time").desc())

    ranked_touchpoints = valid_touchpoints.withColumn("rank", row_number().over(window_spec))

    # Filter for rank == 1 to keep only the absolute last touchpoint before the purchase
    attributed_orders = ranked_touchpoints.filter(col("rank") == 1).drop("rank", "event_time")

    # ==========================================
    # 4. RESTORE UNATTRIBUTED ORDERS
    # ==========================================
    print("Finalizing attribution mapping...")
    # We join back to the original orders table so we don't accidentally delete orders
    # that were purely 'direct' and had no matching marketing events.
    final_attribution = orders.join(
        attributed_orders.select("order_id", "marketing_channel", "session_id"),
        on="order_id",
        how="left"
    ).withColumn(
        # If the marketing channel is null, fill it with 'direct'
        "final_channel", coalesce(col("marketing_channel"), lit("direct"))
    ).drop("marketing_channel").withColumnRenamed("final_channel", "marketing_channel")

    print("Saving attributed orders to output folder...")
    final_attribution.repartition(num_partitions, "user_id") \
        .write.mode("overwrite").parquet(f"{base_path}/output/attributed_orders")

    print("Stage 4 Attribution Complete!")
    spark.stop()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, count
#
# print("Initializing Spark Session for Attribution Analysis...")
#
# spark = SparkSession.builder \
#     .appName("AttributionAnalysis") \
#     .getOrCreate()
#
# print("Loading sessionized data...")
#
# sessions_df = spark.read.parquet("output/sessionized")
#
# print("Computing attribution by referrer...")
#
# attribution_df = sessions_df.groupBy("referrer").agg(
#     count("session_id").alias("total_sessions")
# ).orderBy(col("total_sessions").desc())
#
# print("Saving attribution results...")
#
# attribution_df.write.mode("overwrite").parquet("output/attribution_metrics")
#
# print("Attribution Analysis Complete!")

if __name__ == "__main__":
    main()
