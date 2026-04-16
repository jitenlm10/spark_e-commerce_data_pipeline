from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    stddev,
    when,
    lit,
    sum as _sum,
    round,
    unix_timestamp,
    concat,
)
from pyspark.sql.window import Window


def main():
    print("Initializing Spark Session for Anomaly Detection...")
    spark = SparkSession.builder.appName("Stage5_AnomalyDetection").getOrCreate()

    # Local VS Code path
    base_path = "/home/jovyan/work"

    print("Loading funnel metrics...")
    funnel_df = spark.read.parquet(str(base_path / "output" / "funnel_metrics"))

    # ==========================================
    # 1. DAILY AGGREGATION
    # ==========================================
    print("Aggregating metrics to a daily category level...")
    daily_category_df = (
        funnel_df.groupBy("date", "category")
        .agg(
            _sum("total_views").alias("daily_views"),
            _sum("total_purchases").alias("daily_purchases"),
        )
        .withColumn(
            "daily_conversion_rate",
            when(
                col("daily_views") > 0,
                round(col("daily_purchases") / col("daily_views"), 4),
            ).otherwise(lit(0.0)),
        )
    )

    # ==========================================
    # 2. ROLLING 7-DAY BASELINE
    # ==========================================
    print("Calculating trailing 7-day moving averages...")
    days_back_7 = -7 * 86400
    yesterday = -86400

    rolling_window = (
        Window.partitionBy("category")
        .orderBy(unix_timestamp(col("date")))
        .rangeBetween(days_back_7, yesterday)
    )

    with_baseline = daily_category_df.withColumn(
        "trailing_7d_avg",
        round(avg("daily_conversion_rate").over(rolling_window), 4),
    )

    # ==========================================
    # 3. VARIANCE & THRESHOLDING
    # ==========================================
    print("Detecting spikes and drops...")
    with_variance = with_baseline.withColumn(
        "pct_change_from_baseline",
        when(
            col("trailing_7d_avg") > 0,
            round(
                (
                    (col("daily_conversion_rate") - col("trailing_7d_avg"))
                    / col("trailing_7d_avg")
                )
                * 100,
                2,
            ),
        ).otherwise(lit(0.0)),
    )

    anomalies = with_variance.filter(
        (col("pct_change_from_baseline") <= -50.0)
        | (col("pct_change_from_baseline") >= 50.0)
    )

    # ==========================================
    # 4. GENERATE EXPLANATIONS
    # ==========================================
    print("Generating automated explanations...")
    final_anomalies = (
        anomalies.withColumn(
            "anomaly_type",
            when(col("pct_change_from_baseline") > 0, "SPIKE").otherwise("DROP"),
        )
        .withColumn(
            "explanation",
            concat(
                lit("ALERT: "),
                col("category"),
                lit(" conversion "),
                when(col("anomaly_type") == "SPIKE", lit("spiked by +")).otherwise(
                    lit("dropped by ")
                ),
                col("pct_change_from_baseline").cast("string"),
                lit("% compared to the 7-day baseline of "),
                col("trailing_7d_avg").cast("string"),
            ),
        )
        .orderBy("date", "category")
    )

    print("Saving anomalies to output folder...")
    final_anomalies.coalesce(1).write.mode("overwrite").parquet(
        str(base_path / "output" / "anomalies")
    )

    print("Stage 5 Anomaly Detection Complete!")
    spark.stop()


if __name__ == "__main__":
    main()