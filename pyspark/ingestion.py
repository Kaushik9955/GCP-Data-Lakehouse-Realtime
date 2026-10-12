from pyspark.sql import SparkSession


def create_spark_session():
    return (
        SparkSession.builder
        .appName("EcommerceDataLakehouse")
        .master("local[*]")
        .getOrCreate()
    )


def read_csv_data(spark, file_path, schema):
    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(file_path)
    )