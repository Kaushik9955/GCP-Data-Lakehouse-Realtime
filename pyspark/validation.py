from pyspark.sql import functions as F


def validate_orders(df):
    valid_df = df.filter(
        (F.col("order_id").isNotNull()) &
        (F.col("customer_id").isNotNull()) &
        (F.col("product_id").isNotNull()) &
        (F.col("quantity") > 0) &
        (F.col("amount") >= 0) &
        (F.col("updated_at").isNotNull())
    )

    invalid_df = df.subtract(valid_df)

    return valid_df, invalid_df


def remove_duplicate_orders(df):
    return df.dropDuplicates(["order_id"])