from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, max as _max
import json
import os


def safe_float(value):
    return float(value) if value is not None else 0.0


def safe_int(value):
    return int(value) if value is not None else 0


spark = SparkSession.builder \
    .appName("ExportFrontendData") \
    .master("local[*]") \
    .getOrCreate()

base_output = "output"
funnel_path = os.path.join(base_output, "funnel_metrics")
anomaly_path = os.path.join(base_output, "anomalies_only")

# -----------------------------
# 1. Read Spark outputs
# -----------------------------
funnel_df = spark.read.parquet(funnel_path)
anomaly_df = spark.read.parquet(anomaly_path)

# -----------------------------
# 2. Create summary.json
# -----------------------------
summary_row = funnel_df.agg(
    _sum("total_views").alias("total_views"),
    _sum("total_carts").alias("total_carts"),
    _sum("total_purchases").alias("total_purchases")
).collect()[0]

total_views = safe_int(summary_row["total_views"])
total_carts = safe_int(summary_row["total_carts"])
total_purchases = safe_int(summary_row["total_purchases"])

summary_data = {
    "total_views": total_views,
    "total_carts": total_carts,
    "total_purchases": total_purchases,
    "view_to_cart_rate": round(total_carts / total_views, 4) if total_views > 0 else 0.0,
    "cart_to_purchase_rate": round(total_purchases / total_carts, 4) if total_carts > 0 else 0.0,
    "overall_conversion_rate": round(total_purchases / total_views, 4) if total_views > 0 else 0.0
}

with open(os.path.join(base_output, "summary.json"), "w", encoding="utf-8") as f:
    json.dump(summary_data, f, indent=2)

# -----------------------------
# 3. Create funnel.json
# -----------------------------
funnel_data = [
    {"step": "Views", "count": total_views},
    {"step": "Carts", "count": total_carts},
    {"step": "Purchases", "count": total_purchases}
]

with open(os.path.join(base_output, "funnel.json"), "w", encoding="utf-8") as f:
    json.dump(funnel_data, f, indent=2)

# -----------------------------
# 4. Create anomalies.json
# -----------------------------
anomaly_rows = anomaly_df.select(
    "date",
    "category",
    "device",
    "referrer",
    "overall_conversion_rate",
    "rolling_avg_conversion",
    "z_score",
    "anomaly_flag"
).orderBy(col("z_score").desc()).limit(50).collect()

anomalies_data = []
for row in anomaly_rows:
    anomalies_data.append({
        "date": str(row["date"]) if row["date"] is not None else "",
        "category": row["category"] if row["category"] is not None else "Unknown",
        "device": row["device"] if row["device"] is not None else "Unknown",
        "referrer": row["referrer"] if row["referrer"] is not None else "Unknown",
        "overall_conversion_rate": safe_float(row["overall_conversion_rate"]),
        "rolling_avg_conversion": safe_float(row["rolling_avg_conversion"]),
        "z_score": round(safe_float(row["z_score"]), 4),
        "anomaly_flag": row["anomaly_flag"] if row["anomaly_flag"] is not None else "unknown"
    })

with open(os.path.join(base_output, "anomalies.json"), "w", encoding="utf-8") as f:
    json.dump(anomalies_data, f, indent=2)

print("Frontend JSON files created successfully:")
print("- output/summary.json")
print("- output/funnel.json")
print("- output/anomalies.json")

spark.stop()