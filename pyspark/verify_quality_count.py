from pyspark.sql import SparkSession

BUCKET = "gcp-lakehouse-realtime-data-2026"

PROCESSED_PATH = (
    f"gs://{BUCKET}/quality_test/processed/orders/"
)

QUARANTINE_PATH = (
    f"gs://{BUCKET}/quality_test/quarantine/orders/"
)

spark = (
    SparkSession.builder
    .appName("VerifyQualityTest")
    .getOrCreate()
)

# Read processed valid records
processed_df = spark.read.parquet(PROCESSED_PATH)

# Read quarantined invalid records
quarantine_df = spark.read.parquet(QUARANTINE_PATH)

print("========================================")
print("QUALITY TEST VERIFICATION")
print("========================================")

print(f"Processed valid records: {processed_df.count()}")
print(f"Quarantined invalid records: {quarantine_df.count()}")
print(f"Total records: {processed_df.count() + quarantine_df.count()}")

print("========================================")

spark.stop()