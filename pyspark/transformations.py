from pyspark.sql import functions as F


def transform_orders(orders_df):
    return (
        orders_df
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("integer"))
    )


def enrich_orders(orders_df, customers_df, products_df, payments_df):

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

    enriched_df = (
        orders_df
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

    return enriched_df