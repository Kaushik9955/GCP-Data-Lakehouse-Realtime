from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

from ingestion import create_spark_session, read_csv_data
from validation import validate_orders, remove_duplicate_orders
from transformations import transform_orders, enrich_orders


# --------------------------------------------------
# GCS Paths - Quality Test
# --------------------------------------------------

BUCKET = "gcp-lakehouse-realtime-data-2026"

CUSTOMERS_PATH = (
    f"gs://{BUCKET}/raw/customers/customers.csv"
)

PRODUCTS_PATH = (
    f"gs://{BUCKET}/raw/products/products.csv"
)

ORDERS_PATH = (
    f"gs://{BUCKET}/quality_test/orders/orders_quality_test.csv"
)

QUARANTINE_PATH = (
    f"gs://{BUCKET}/quality_test/quarantine/orders/"
)

PROCESSED_PATH = (
    f"gs://{BUCKET}/quality_test/processed/orders/"
)


# --------------------------------------------------
# Create Spark
# --------------------------------------------------

spark = create_spark_session()


# --------------------------------------------------
# Define schemas
# --------------------------------------------------

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", StringType(), True),
])


product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
])


order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("updated_at", StringType(), True),
])


# --------------------------------------------------
# Read data from GCS
# --------------------------------------------------

customers_df = read_csv_data(
    spark,
    CUSTOMERS_PATH,
    customer_schema
)

products_df = read_csv_data(
    spark,
    PRODUCTS_PATH,
    product_schema
)

orders_df = read_csv_data(
    spark,
    ORDERS_PATH,
    order_schema
)


# --------------------------------------------------
# Source counts
# --------------------------------------------------

print("Customers:", customers_df.count())
print("Products:", products_df.count())
print("Orders:", orders_df.count())


# --------------------------------------------------
# Validate orders
# --------------------------------------------------

valid_orders_df, invalid_orders_df = validate_orders(
    orders_df
)

invalid_count = invalid_orders_df.count()
valid_count = valid_orders_df.count()

print("Valid orders:", valid_count)
print("Invalid orders:", invalid_count)


# --------------------------------------------------
# Write invalid records to quarantine
# --------------------------------------------------

if invalid_count > 0:

    invalid_orders_df.write \
        .mode("overwrite") \
        .parquet(QUARANTINE_PATH)

    print(
        "Invalid records written to:",
        QUARANTINE_PATH
    )

else:

    print("No invalid records found.")


# --------------------------------------------------
# Remove duplicates from valid records
# --------------------------------------------------

valid_orders_df = remove_duplicate_orders(
    valid_orders_df
)

print(
    "Valid orders after deduplication:",
    valid_orders_df.count()
)


# --------------------------------------------------
# Transform
# --------------------------------------------------

transformed_orders_df = transform_orders(
    valid_orders_df
)


# --------------------------------------------------
# Enrich
# --------------------------------------------------

enriched_orders_df = enrich_orders(
    transformed_orders_df,
    customers_df,
    products_df
)


# --------------------------------------------------
# Write valid processed records
# --------------------------------------------------

enriched_orders_df.write \
    .mode("overwrite") \
    .parquet(PROCESSED_PATH)

print(
    "Valid processed records written to:",
    PROCESSED_PATH
)


# --------------------------------------------------
# Display sample
# --------------------------------------------------

print("Sample processed records:")

enriched_orders_df.show(
    10,
    truncate=False
)


# --------------------------------------------------
# Final schema
# --------------------------------------------------

print("Final schema:")

enriched_orders_df.printSchema()


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()