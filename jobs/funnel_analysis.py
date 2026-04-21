from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, max as _max, sum as _sum, broadcast, round


def main():
    print("Initializing Spark Session for Funnel Analysis...")
    spark = SparkSession.builder.appName("Stage3_FunnelAnalysis").getOrCreate()
    base_path = "/opt/bitnami/spark/project"

    print("Loading sessionized events and catalog Parquet files...")
    events_df = spark.read.parquet(f"{base_path}/output/events_with_sessions")
    catalog_df = spark.read.parquet(f"{base_path}/cleaned/catalog_clean")

    # 1. BROADCAST JOIN & DATE EXTRACTION
    print("Extracting date and performing Broadcast Join with Catalog...")
    events_with_date = events_df.withColumn("date", to_date(col("timestamp")))

    # Broadcast the small catalog to all workers to avoid a shuffle here
    events_enriched = events_with_date.join(broadcast(catalog_df), on="product_id", how="left")

    # 2. SESSION-LEVEL AGGREGATION (Pre-computation)
    print("Calculating funnel progression per session...")
    # Did this specific session view, cart, or purchase this specific category?
    session_flags = events_enriched.groupBy("session_id", "category", "device", "referrer", "date").agg(
        _max(when(col("event_type") == "view", 1).otherwise(0)).alias("has_view"),
        _max(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("has_cart"),
        _max(when(col("event_type") == "purchase", 1).otherwise(0)).alias("has_purchase")
    )

    # 3. DISTRIBUTED SHUFFLE & AGGREGATION
    print("Triggering distributed shuffle to calculate daily conversion metrics...")
    # This groupBy forces data to move across the network so identical groupings land on the same worker
    funnel_metrics = session_flags.groupBy("category", "device", "referrer", "date").agg(
        _sum("has_view").alias("total_views"),
        _sum("has_cart").alias("total_carts"),
        _sum("has_purchase").alias("total_purchases")
    )

    # 4. CONVERSION RATE MATH
    print("Calculating final conversion rates...")
    funnel_final = funnel_metrics.withColumn(
        "view_to_cart_rate",
        when(col("total_views") > 0, round(col("total_carts") / col("total_views"), 4)).otherwise(0.0)
    ).withColumn(
        "cart_to_purchase_rate",
        when(col("total_carts") > 0, round(col("total_purchases") / col("total_carts"), 4)).otherwise(0.0)
    ).withColumn(
        "overall_conversion_rate",
        when(col("total_views") > 0, round(col("total_purchases") / col("total_views"), 4)).otherwise(0.0)
    )

    print("Saving funnel metrics to output folder...")
    # We coalesce to 1 file here because the aggregated output is very small (just summary statistics)
    funnel_final.coalesce(1).write.mode("overwrite").parquet(f"{base_path}/output/funnel_metrics")

    print("Stage 3 Funnel Analysis Complete!")
    spark.stop()


if __name__ == "__main__":
    main()
