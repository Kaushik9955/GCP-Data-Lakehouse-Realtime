import csv
from pathlib import Path


# --------------------------------------------------
# Paths
# --------------------------------------------------

SOURCE_FILE = Path(
    r"C:\Desktop\gcp-data-lakehouse-realtime\data\sample\orders.csv"
)

OUTPUT_DIR = Path(
    r"C:\Desktop\gcp-data-lakehouse-realtime\data\quality_test"
)

OUTPUT_FILE = OUTPUT_DIR / "orders_quality_test.csv"


# --------------------------------------------------
# Create output directory
# --------------------------------------------------

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# --------------------------------------------------
# Read original orders
# --------------------------------------------------

with open(
    SOURCE_FILE,
    "r",
    newline="",
    encoding="utf-8"
) as file:

    reader = csv.DictReader(file)

    rows = list(reader)

    fieldnames = reader.fieldnames


# --------------------------------------------------
# Create invalid records
# --------------------------------------------------

invalid_records = [

    {
        "order_id": "INVALID_001",
        "customer_id": "",
        "product_id": "P00001",
        "order_date": "2026-09-01",
        "quantity": "2",
        "amount": "1000.00",
        "status": "COMPLETED",
        "updated_at": "2026-09-01 10:00:00",
    },

    {
        "order_id": "INVALID_002",
        "customer_id": "C00001",
        "product_id": "",
        "order_date": "2026-09-02",
        "quantity": "1",
        "amount": "500.00",
        "status": "COMPLETED",
        "updated_at": "2026-09-02 10:00:00",
    },

    {
        "order_id": "INVALID_003",
        "customer_id": "C00002",
        "product_id": "P00002",
        "order_date": "2026-09-03",
        "quantity": "0",
        "amount": "500.00",
        "status": "COMPLETED",
        "updated_at": "2026-09-03 10:00:00",
    },

    {
        "order_id": "INVALID_004",
        "customer_id": "C00003",
        "product_id": "P00003",
        "order_date": "2026-09-04",
        "quantity": "1",
        "amount": "-100.00",
        "status": "COMPLETED",
        "updated_at": "2026-09-04 10:00:00",
    },

    {
        "order_id": "INVALID_005",
        "customer_id": "C00004",
        "product_id": "P00004",
        "order_date": "2026-09-05",
        "quantity": "1",
        "amount": "100.00",
        "status": "COMPLETED",
        "updated_at": "",
    },
]


# --------------------------------------------------
# Add invalid records
# --------------------------------------------------

rows.extend(invalid_records)


# --------------------------------------------------
# Write quality-test dataset
# --------------------------------------------------

with open(
    OUTPUT_FILE,
    "w",
    newline="",
    encoding="utf-8"
) as file:

    writer = csv.DictWriter(
        file,
        fieldnames=fieldnames
    )

    writer.writeheader()

    writer.writerows(rows)


# --------------------------------------------------
# Result
# --------------------------------------------------

print(
    "Quality test file created:"
)

print(
    OUTPUT_FILE
)

print(
    "Total records:",
    len(rows)
)

print(
    "Invalid test records:",
    len(invalid_records)
)