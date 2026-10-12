from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)
import sys


# ============================================================
# CONFIGURATION
# ============================================================

BUCKET = "gcp-lakehouse-realtime-data-2026"

INCREMENTAL_PATH = (
    f"gs://{BUCKET}/quality_test/incremental/orders_incremental.csv"
)

EXISTING_ORDERS_PATH = (
    f"gs://{BUCKET}/raw/orders/orders.csv"
)

QUARANTINE_PATH = (
    f"gs://{BUCKET}/quality_test/incremental/quarantine/"
)

PROCESSED_PATH = (
    f"gs://{BUCKET}/quality_test/incremental/processed/"
)


# ============================================================
# WATERMARK
# ============================================================

if len(sys.argv) < 2:
    raise ValueError(
        "Please provide watermark as an argument."
    )

watermark = sys.argv[1]


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("EcommerceIncrementalProcessing")
    .getOrCreate()
)


# ============================================================
# SCHEMA
# ============================================================

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("updated_at", StringType(), True)
])


# ============================================================
# READ INCREMENTAL DATA
# ============================================================

incremental_df = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv(INCREMENTAL_PATH)
)

incremental_df = (
    incremental_df
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
)


# ============================================================
# WATERMARK FILTER
# ============================================================

watermark_ts = F.to_timestamp(
    F.lit(watermark)
)

incremental_df = incremental_df.filter(
    F.col("updated_at") > watermark_ts
)


# ============================================================
# READ EXISTING PRODUCTION ORDERS
# ============================================================

existing_df = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv(EXISTING_ORDERS_PATH)
)

existing_order_ids = (
    existing_df
    .select("order_id")
    .dropDuplicates()
)


# ============================================================
# VALIDATION
# ============================================================

valid_df = incremental_df.filter(
    (F.col("order_id").isNotNull()) &
    (F.col("customer_id").isNotNull()) &
    (F.col("product_id").isNotNull()) &
    (F.col("quantity") > 0) &
    (F.col("amount") >= 0) &
    (F.col("updated_at").isNotNull())
)

invalid_df = incremental_df.subtract(valid_df)


# ============================================================
# COUNT INVALID RECORDS
# ============================================================

invalid_count = invalid_df.count()


# ============================================================
# WRITE INVALID RECORDS TO QUARANTINE
# ============================================================

if invalid_count > 0:

    (
        invalid_df
        .write
        .mode("overwrite")
        .parquet(QUARANTINE_PATH)
    )


# ============================================================
# DEDUPLICATION
# ============================================================

valid_before_dedup = valid_df.count()

deduplicated_df = (
    valid_df
    .dropDuplicates(["order_id"])
)

valid_after_dedup = deduplicated_df.count()

duplicate_count = (
    valid_before_dedup - valid_after_dedup
)


# ============================================================
# IDENTIFY NEW VS UPDATED ORDERS
# ============================================================

new_orders_df = (
    deduplicated_df
    .join(
        existing_order_ids,
        on="order_id",
        how="left_anti"
    )
)

updated_orders_df = (
    deduplicated_df
    .join(
        existing_order_ids,
        on="order_id",
        how="left_semi"
    )
)


# ============================================================
# COUNTS
# ============================================================

total_incremental = incremental_df.count()
valid_count = valid_df.count()
new_count = new_orders_df.count()
updated_count = updated_orders_df.count()


# ============================================================
# WRITE VALID PROCESSED DATA
# ============================================================

(
    deduplicated_df
    .write
    .mode("overwrite")
    .parquet(PROCESSED_PATH)
)


# ============================================================
# DISPLAY RESULTS
# ============================================================

print("==============================================")
print("INCREMENTAL PIPELINE TEST")
print("==============================================")

print(f"Watermark: {watermark}")

print(f"Records after watermark: {total_incremental}")

print(f"Valid records: {valid_count}")

print(f"Invalid records: {invalid_count}")

print(f"Records before deduplication: {valid_before_dedup}")

print(f"Records after deduplication: {valid_after_dedup}")

print(f"Duplicate records removed: {duplicate_count}")

print(f"New orders: {new_count}")

print(f"Updated orders: {updated_count}")

print("==============================================")

print(
    f"Processed output: {PROCESSED_PATH}"
)

if invalid_count > 0:
    print(
        f"Quarantine output: {QUARANTINE_PATH}"
    )

print("==============================================")


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()