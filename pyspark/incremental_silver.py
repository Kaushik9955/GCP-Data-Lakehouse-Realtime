from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ingestion import create_spark_session


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ID = "gcp-data-lakehouse-realtime"

PIPELINE_NAME = "orders_incremental"

# Bronze incremental raw data
BRONZE_INCREMENTAL = (
    f"{PROJECT_ID}.ecommerce_bronze.orders_incremental_raw"
)

# Pipeline control table
PIPELINE_CONTROL = (
    f"{PROJECT_ID}.ecommerce_bronze.pipeline_control"
)

# Main Silver table
SILVER_ORDERS = (
    f"{PROJECT_ID}.ecommerce_silver.orders"
)

# Silver staging table
SILVER_STAGING = (
    f"{PROJECT_ID}.ecommerce_silver.orders_incremental_staging"
)

# Reference tables
BRONZE_CUSTOMERS = (
    f"{PROJECT_ID}.ecommerce_bronze.customers"
)

BRONZE_PRODUCTS = (
    f"{PROJECT_ID}.ecommerce_bronze.products"
)

BRONZE_PAYMENTS = (
    f"{PROJECT_ID}.ecommerce_bronze.payments"
)

# GCS bucket
BUCKET = "gcp-lakehouse-realtime-data-2026"

# Quarantine location
QUARANTINE_PATH = (
    f"gs://{BUCKET}/quarantine/incremental_orders/"
)


# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = create_spark_session()


# ============================================================
# READ BRONZE INCREMENTAL DATA
# ============================================================

print("Reading incremental Bronze data...")

incremental_df = (
    spark.read
    .format("bigquery")
    .option("table", BRONZE_INCREMENTAL)
    .load()
)


# ============================================================
# READ PIPELINE CONTROL TABLE
# ============================================================

print("Reading pipeline_control table...")

pipeline_control_df = (
    spark.read
    .format("bigquery")
    .option("table", PIPELINE_CONTROL)
    .load()
)


# ============================================================
# GET CURRENT WATERMARK
# ============================================================

watermark_row = (
    pipeline_control_df
    .filter(
        F.col("pipeline_name") == PIPELINE_NAME
    )
    .select("last_processed_at")
    .first()
)


if watermark_row is None:
    raise ValueError(
        f"No pipeline_control row found for pipeline: "
        f"{PIPELINE_NAME}"
    )


watermark_ts = watermark_row["last_processed_at"]


print("----------------------------------------")
print(f"Pipeline name : {PIPELINE_NAME}")
print(f"Watermark     : {watermark_ts}")
print("----------------------------------------")


# ============================================================
# CONVERT UPDATED_AT TO TIMESTAMP
# ============================================================

incremental_df = (
    incremental_df
    .withColumn(
        "updated_at",
        F.to_timestamp("updated_at")
    )
)


# ============================================================
# FILTER RECORDS AFTER WATERMARK
# ============================================================

incremental_df = incremental_df.filter(
    F.col("updated_at") > F.lit(watermark_ts)
)


records_after_watermark = incremental_df.count()

print(
    f"Records after watermark: "
    f"{records_after_watermark}"
)


# ============================================================
# STOP IF NO NEW DATA
# ============================================================

if records_after_watermark == 0:

    print("No new or changed records found.")

    spark.stop()

    raise SystemExit(0)


# ============================================================
# VALIDATE INCREMENTAL DATA
# ============================================================

print("Validating incremental records...")


valid_df = incremental_df.filter(
    (F.col("order_id").isNotNull()) &
    (F.col("customer_id").isNotNull()) &
    (F.col("product_id").isNotNull()) &
    (F.col("quantity") > 0) &
    (F.col("amount") >= 0) &
    (F.col("updated_at").isNotNull())
)


invalid_df = incremental_df.subtract(valid_df)


valid_count = valid_df.count()
invalid_count = invalid_df.count()


print(f"Valid records   : {valid_count}")
print(f"Invalid records : {invalid_count}")


# ============================================================
# WRITE INVALID RECORDS TO QUARANTINE
# ============================================================

if invalid_count > 0:

    print("Writing invalid records to quarantine...")

    (
        invalid_df.write
        .mode("overwrite")
        .parquet(QUARANTINE_PATH)
    )

    print(
        f"Invalid records written to: "
        f"{QUARANTINE_PATH}"
    )

else:

    print("No invalid records found.")


# ============================================================
# DEDUPLICATE INCREMENTAL DATA
# ============================================================

print("Deduplicating incremental records...")


window_spec = (
    Window
    .partitionBy("order_id")
    .orderBy(
        F.col("updated_at").desc()
    )
)


deduplicated_df = (
    valid_df
    .withColumn(
        "row_number",
        F.row_number().over(window_spec)
    )
    .filter(
        F.col("row_number") == 1
    )
    .drop("row_number")
)


before_dedup_count = valid_count
after_dedup_count = deduplicated_df.count()

duplicates_removed = (
    before_dedup_count - after_dedup_count
)


print(
    f"Records before dedup : "
    f"{before_dedup_count}"
)

print(
    f"Records after dedup  : "
    f"{after_dedup_count}"
)

print(
    f"Duplicates removed   : "
    f"{duplicates_removed}"
)


# ============================================================
# READ CURRENT SILVER DATA
# ============================================================

print("Reading current Silver orders...")

silver_df = (
    spark.read
    .format("bigquery")
    .option("table", SILVER_ORDERS)
    .load()
)


# ============================================================
# READ REFERENCE DATA
# ============================================================

print("Reading customer, product and payment data...")


customers_df = (
    spark.read
    .format("bigquery")
    .option("table", BRONZE_CUSTOMERS)
    .load()
)


products_df = (
    spark.read
    .format("bigquery")
    .option("table", BRONZE_PRODUCTS)
    .load()
)


payments_df = (
    spark.read
    .format("bigquery")
    .option("table", BRONZE_PAYMENTS)
    .load()
)


# ============================================================
# SELECT CURRENT SILVER WATERMARK PER ORDER
# ============================================================

silver_current = (
    silver_df
    .select(
        "order_id",
        F.col("updated_at")
        .alias("silver_updated_at")
    )
)


# ============================================================
# COMPARE INCOMING DATA WITH SILVER
# ============================================================

comparison_df = (
    deduplicated_df
    .alias("incoming")
    .join(
        silver_current.alias("silver"),
        on="order_id",
        how="left"
    )
)


# ============================================================
# IDENTIFY NEW ORDERS
# ============================================================

new_orders_df = (
    comparison_df
    .filter(
        F.col("silver_updated_at").isNull()
    )
    .select("incoming.*")
)


# ============================================================
# IDENTIFY UPDATED ORDERS
# ============================================================

updated_orders_df = (
    comparison_df
    .filter(
        (F.col("silver_updated_at").isNotNull()) &
        (
            F.col("incoming.updated_at")
            > F.col("silver_updated_at")
        )
    )
    .select("incoming.*")
)


# ============================================================
# IDENTIFY IGNORED ORDERS
# ============================================================

ignored_orders_df = (
    comparison_df
    .filter(
        (F.col("silver_updated_at").isNotNull()) &
        (
            F.col("incoming.updated_at")
            <= F.col("silver_updated_at")
        )
    )
)


# ============================================================
# COUNTS
# ============================================================

new_count = new_orders_df.count()
updated_count = updated_orders_df.count()
ignored_count = ignored_orders_df.count()


print("----------------------------------------")
print(f"New orders     : {new_count}")
print(f"Updated orders : {updated_count}")
print(f"Ignored orders : {ignored_count}")
print("----------------------------------------")


# ============================================================
# COMBINE NEW + UPDATED RECORDS
# ============================================================

silver_changes_df = (
    new_orders_df
    .unionByName(updated_orders_df)
)


changes_count = silver_changes_df.count()


print(
    f"Total Silver changes: "
    f"{changes_count}"
)


# ============================================================
# TRANSFORM DATA TYPES
# ============================================================

silver_changes_df = (
    silver_changes_df
    .withColumn(
        "order_date",
        F.to_date("order_date")
    )
    .withColumn(
        "updated_at",
        F.to_timestamp("updated_at")
    )
    .withColumn(
        "amount",
        F.col("amount").cast("double")
    )
    .withColumn(
        "quantity",
        F.col("quantity").cast("integer")
    )
)


# ============================================================
# SELECT REFERENCE DATA
# ============================================================

customer_data = customers_df.select(
    "customer_id",
    "customer_name",
    "city",
    "state"
)


product_data = products_df.select(
    "product_id",
    "product_name",
    "category",
    "price"
)


payment_data = payments_df.select(
    "payment_id",
    "order_id",
    "payment_method",
    "payment_status",
    "payment_date"
)


# ============================================================
# ENRICH ORDERS
# ============================================================

enriched_df = (
    silver_changes_df

    .join(
        customer_data,
        on="customer_id",
        how="left"
    )

    .join(
        product_data,
        on="product_id",
        how="left"
    )

    .join(
        payment_data,
        on="order_id",
        how="left"
    )
)


# ============================================================
# FINAL COLUMN ORDER
# ============================================================

enriched_df = enriched_df.select(
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "quantity",
    "amount",
    "status",
    "updated_at",
    "customer_name",
    "city",
    "state",
    "product_name",
    "category",
    "price",
    "payment_id",
    "payment_method",
    "payment_status",
    "payment_date"
)


# ============================================================
# WRITE CLEAN DATA TO BIGQUERY STAGING
# ============================================================

if changes_count > 0:

    print(
        "Writing incremental Silver changes "
        "to BigQuery staging..."
    )

    (
        enriched_df.write
        .format("bigquery")
        .option(
            "table",
            SILVER_STAGING
        )
        .mode("overwrite")
        .save()
    )

    print(
        "Silver staging table updated:"
    )

    print(
        SILVER_STAGING
    )

else:

    print(
        "No Silver changes to write."
    )


# ============================================================
# FINAL SUMMARY
# ============================================================

print("")
print("========================================")
print("INCREMENTAL SILVER PROCESSING COMPLETE")
print("========================================")
print(f"Pipeline           : {PIPELINE_NAME}")
print(f"Watermark          : {watermark_ts}")
print(
    f"After watermark    : "
    f"{records_after_watermark}"
)
print(f"Valid              : {valid_count}")
print(f"Invalid            : {invalid_count}")
print(
    f"After dedup        : "
    f"{after_dedup_count}"
)
print(
    f"Duplicates removed : "
    f"{duplicates_removed}"
)
print(f"New orders         : {new_count}")
print(f"Updated orders     : {updated_count}")
print(f"Ignored orders     : {ignored_count}")
print(
    f"Silver changes     : "
    f"{changes_count}"
)
print("========================================")


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()