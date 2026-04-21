from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, explode, array


def main():
    print("Initializing Advanced Data Inflation Script...")
    spark = SparkSession.builder.appName("Data_Multiplier").getOrCreate()
    base_path = "/opt/bitnami/spark/project"

    print("Loading original raw data...")
    events_raw = spark.read.csv(f"{base_path}/data/events.csv", header=True)
    orders_raw = spark.read.csv(f"{base_path}/data/transactions.csv", header=True)

    # Define how many times to duplicate the data
    multiplier = 10
    print(f"Multiplying dataset by {multiplier}x using distributed array explosion...")

    # 1. Creating an array of suffixes: ['customer_id_1', '_2', ... '_20']
    suffixes = array(*[lit(f"_{i}") for i in range(1, multiplier + 1)])

    # 2. Explode the array to multiply the rows, then append the suffix to customer_id
    massive_events = events_raw.withColumn("suffix", explode(suffixes)) \
        .withColumn("customer_id", concat(col("customer_id"), col("suffix"))) \
        .drop("suffix")

    massive_orders = orders_raw.withColumn("suffix", explode(suffixes)) \
        .withColumn("customer_id", concat(col("customer_id"), col("suffix"))) \
        .drop("suffix")

    print(f"Saving massive datasets to disk. This might take a minute...")
    massive_events.coalesce(4).write.mode("overwrite").csv(f"{base_path}/data/events_massive", header=True)
    massive_orders.coalesce(4).write.mode("overwrite").csv(f"{base_path}/data/transactions_massive", header=True)

    print("Inflation Complete! Update preprocess.py to point to these new folders.")
    spark.stop()


if __name__ == "__main__":
    main()
