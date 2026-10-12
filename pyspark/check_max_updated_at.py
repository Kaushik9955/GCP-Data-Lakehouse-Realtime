from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "gcp-lakehouse-realtime-data-2026"

ORDERS_PATH = (
    f"gs://{BUCKET}/raw/orders/orders.csv"
)

spark = (
    SparkSession.builder
    .appName("CheckMaxUpdatedAt")
    .getOrCreate()
)

orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ORDERS_PATH)
)

orders_df = orders_df.withColumn(
    "updated_at",
    F.to_timestamp("updated_at")
)

max_timestamp = (
    orders_df
    .select(F.max("updated_at").alias("max_updated_at"))
    .collect()[0]["max_updated_at"]
)

print("======================================")
print("INITIAL WATERMARK")
print("======================================")
print(f"Maximum updated_at: {max_timestamp}")
print("======================================")

spark.stop()