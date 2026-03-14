from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, unix_timestamp, when, sum as _sum
from pyspark.sql.window import Window


def main():
    print("Initializing Spark Session for Sessionization...")
    spark = SparkSession.builder.appName("Stage2_Sessionization").getOrCreate()

    base_path = Path(__file__).resolve().parent.parent

    print("Loading cleaned events Parquet...")
    events_clean = spark.read.parquet(str(base_path / "cleaned" / "events_clean"))

    print("Building sessions using inactivity threshold...")
    user_window = Window.partitionBy("user_id").orderBy("timestamp")

    events_with_prev = events_clean.withColumn(
        "prev_ts",
        lag("timestamp").over(user_window)
    )

    events_with_gap = events_with_prev.withColumn(
        "gap_seconds",
        unix_timestamp(col("timestamp")) - unix_timestamp(col("prev_ts"))
    )

    events_flagged = events_with_gap.withColumn(
        "new_session_flag",
        when(col("prev_ts").isNull() | (col("gap_seconds") > 1800), 1).otherwise(0)
    )

    events_sessionized = events_flagged.withColumn(
        "session_number",
        _sum("new_session_flag").over(user_window)
    )

    events_sessionized = events_sessionized.withColumn(
        "session_id",
        col("user_id").cast("string")
    )

    print("Saving sessionized events...")
    output_path = base_path / "output" / "sessionized"
    events_sessionized.write.mode("overwrite").parquet(str(output_path))

    print("Stage 2 Sessionization Complete!")
    spark.stop()


if __name__ == "__main__":
    main()