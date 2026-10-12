from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from ingestion import create_spark_session, read_csv_data
from validation import validate_orders, remove_duplicate_orders
from transformations import transform_orders, enrich_orders


# ============================================================
# CONFIGURATION
# ============================================================

BUCKET = "gcp-lakehouse-realtime-data-2026"

CUSTOMERS_PATH = f"gs://{BUCKET}/raw/customers/customers.csv"
PRODUCTS_PATH = f"gs://{BUCKET}/raw/products/products.csv"
ORDERS_PATH = f"gs://{BUCKET}/raw/orders/orders.csv"
PAYMENTS_PATH = f"gs://{BUCKET}/raw/payments/payments.csv"


# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = create_spark_session()

print("=" * 70)
print("GCP ECOMMERCE PYSPARK - GCS TEST")
print("=" * 70)


# ============================================================
# SCHEMAS
# ============================================================

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
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


payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_date", StringType(), True),
])


# ============================================================
# READ DATA FROM GCS
# ============================================================

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

payments_df = read_csv_data(
    spark,
    PAYMENTS_PATH,
    payment_schema
)


# ============================================================
# INPUT COUNTS
# ============================================================

print(f"Customers: {customers_df.count()}")
print(f"Products: {products_df.count()}")
print(f"Orders: {orders_df.count()}")
print(f"Payments: {payments_df.count()}")


# ============================================================
# VALIDATE ORDERS
# ============================================================

valid_orders_df, invalid_orders_df = validate_orders(orders_df)

print(f"Invalid orders: {invalid_orders_df.count()}")
print(f"Valid orders: {valid_orders_df.count()}")


# ============================================================
# REMOVE DUPLICATES
# ============================================================

deduplicated_orders_df = remove_duplicate_orders(valid_orders_df)

print(
    f"Orders after deduplication: "
    f"{deduplicated_orders_df.count()}"
)


# ============================================================
# TRANSFORM ORDERS
# ============================================================

transformed_orders_df = transform_orders(
    deduplicated_orders_df
)


# ============================================================
# ENRICH WITH CUSTOMER + PRODUCT + PAYMENT
# ============================================================

enriched_orders_df = enrich_orders(
    transformed_orders_df,
    customers_df,
    products_df,
    payments_df
)


# ============================================================
# FINAL RESULT
# ============================================================

print("=" * 70)
print("FINAL ENRICHED DATA")
print("=" * 70)

final_count = enriched_orders_df.count()

print(f"Final enriched orders: {final_count}")

print("\nFinal schema:")
enriched_orders_df.printSchema()

print("\nSample records:")
enriched_orders_df.show(10, truncate=False)


# ============================================================
# PAYMENT ENRICHMENT CHECK
# ============================================================

print("=" * 70)
print("PAYMENT ENRICHMENT CHECK")
print("=" * 70)

enriched_orders_df.select(
    "order_id",
    "payment_id",
    "payment_method",
    "payment_status",
    "payment_date"
).show(10, truncate=False)


# ============================================================
# COUNT CHECK
# ============================================================

print("=" * 70)
print("COUNT CHECK")
print("=" * 70)

original_count = orders_df.count()

print(f"Original orders:      {original_count}")
print(f"Final enriched orders: {final_count}")

if original_count == final_count:
    print("PASS: Payment join did not increase order count.")
else:
    print("WARNING: Final order count changed.")


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()