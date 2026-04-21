from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lower, trim

def main():
    print("Initializing Spark Session for Preprocessing...")
    spark = SparkSession.builder.appName("Stage1_Preprocessing").getOrCreate()
    num_partitions = int(spark.conf.get("spark.sql.shuffle.partitions", "8"))
    base_path = "/opt/bitnami/spark/project"

    print("Loading core CSV files...")
    events_raw = spark.read.csv(f"{base_path}/data/events_massive", header=True, inferSchema=True)
    transactions_raw = spark.read.csv(f"{base_path}/data/transactions_massive", header=True, inferSchema=True)
    products_raw = spark.read.csv(f"{base_path}/data/products.csv", header=True, inferSchema=True)

    # 1. FORMATTING 'EVENTS'
    # Required: customer id, timestamp, event type, product id, device, referrer

    print("Formatting events...")
    # Filter first, format second
    events_clean = events_raw.dropna(subset=["customer_id", "timestamp", "product_id"]) \
        .select(
        col("customer_id").alias("user_id"),
        to_timestamp(col("timestamp"), "dd-MM-yyyy HH:mm").alias("timestamp"),
        lower(trim(col("event_type"))).alias("event_type"),
        col("product_id"),
        lower(trim(col("device_type"))).alias("device"),
        lower(trim(col("traffic_source"))).alias("referrer")
    )

    # 2. FORMATTING 'ORDERS'
    # Required: transaction id, customer id, timestamp, total amount

    print("Formatting orders (from transactions.csv)...")
    orders_clean = transactions_raw.select(
        col("transaction_id").alias("order_id"),
        col("customer_id").alias("user_id"),
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("timestamp"),
        col("gross_revenue").alias("total_amount")
    ).dropna(subset=["order_id", "user_id", "timestamp"])

    # 3. FORMATTING 'CATALOG'
    # Required: product id, category, brand, price

    print("Formatting catalog (from products.csv)...")
    catalog_clean = products_raw.select(
        col("product_id"),
        lower(trim(col("category"))).alias("category"),
        col("brand"),
        col("base_price").alias("price")
    ).dropna(subset=["product_id"]).dropDuplicates(["product_id"])

    # 4. SAVE TO PARQUET

    print("Saving strictly aligned Parquet files...")

    # Partition the large tables by user_id to optimize downstream windowing and joins
    events_clean.repartition(num_partitions, "user_id") \
        .write.mode("overwrite").parquet(f"{base_path}/cleaned/events_clean")

    orders_clean.repartition(num_partitions, "user_id") \
        .write.mode("overwrite").parquet(f"{base_path}/cleaned/orders_clean")

    # Catalog is tiny, so no need to partition
    catalog_clean.coalesce(1) \
        .write.mode("overwrite").parquet(f"{base_path}/cleaned/catalog_clean")

    print("Stage 1 Preprocessing Complete!")
    spark.stop()


if __name__ == "__main__":
    main()
