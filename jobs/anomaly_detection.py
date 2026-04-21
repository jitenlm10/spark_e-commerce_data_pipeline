from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sum as _sum, round, unix_timestamp, avg, when, concat, lit


def main():
    print("Initializing Spark Session for Anomaly Detection...")
    spark = SparkSession.builder.appName("Stage5_AnomalyDetection").getOrCreate()
    base_path = "/opt/bitnami/spark/project"

    print("Loading funnel metrics...")
    funnel_df = spark.read.parquet(f"{base_path}/output/funnel_metrics")

    # 1. DAILY AGGREGATION
    print("Aggregating metrics to a daily category level...")
    daily_category_df = funnel_df.groupBy("date", "category").agg(
        _sum("total_views").alias("daily_views"),
        _sum("total_purchases").alias("daily_purchases")
    ).withColumn(
        "daily_conversion_rate",
        when(col("daily_views") > 0, round(col("daily_purchases") / col("daily_views"), 4)).otherwise(0.0)
    )

    # 2. ROLLING 7-DAY BASELINE (Window Setup)
    print("Defining 7-day rolling window specifications...")
    # Convert date to unix seconds. 1 day = 86400 seconds.
    # Look back 7 days (-7 * 86400) up to yesterday (-86400) so today's anomaly doesn't skew its own baseline.
    window_spec = Window.partitionBy("category").orderBy(unix_timestamp(col("date"))) \
        .rangeBetween(-7 * 86400, -86400)

    # 3 & 4. COMBINED: CALCULATION & FILTERING
    print("Calculating baseline, detecting anomalies, and generating alerts...")

    # Chain the baseline calculation and the variance calculation in one go
    metrics_enriched = daily_category_df.withColumn(
        "trailing_7d_avg", round(avg("daily_conversion_rate").over(window_spec), 4)
    ).withColumn(
        "pct_change",
        when(col("trailing_7d_avg") > 0,
             round(((col("daily_conversion_rate") - col("trailing_7d_avg")) / col("trailing_7d_avg")) * 100, 2)
             ).otherwise(0.0)
    )

    # Filter for outliers IMMEDIATELY, dropping normal days from memory
    anomalies = metrics_enriched.filter((col("pct_change") <= -50.0) | (col("pct_change") >= 50.0))

    # Generate the explanations ONLY on the drastically reduced dataset
    final_anomalies = anomalies.withColumn(
        "anomaly_type", when(col("pct_change") > 0, "SPIKE").otherwise("DROP")
    ).withColumn(
        "explanation",
        concat(
            lit("ALERT: [Category: "), col("category"), lit("] conversion rate on "), col("date"),
            when(col("anomaly_type") == "SPIKE", lit(" spiked by +")).otherwise(lit(" dropped by ")),
            col("pct_change"), lit("% compared to the 7-day baseline of "),
            col("trailing_7d_avg")
        )
    ).orderBy("date", "category")

    # 5. SAVE FINAL OUTPUT
    print("Saving anomalies to output folder...")
    # Since anomalies are rare, the final dataset is tiny. Coalesce to 1 file to avoid generating dozens of empty part-files.
    final_anomalies.coalesce(1).write.mode("overwrite").parquet(f"{base_path}/output/anomalies")

    print("Stage 5 Anomaly Detection Complete!")
    spark.stop()


if __name__ == "__main__":
    main()

