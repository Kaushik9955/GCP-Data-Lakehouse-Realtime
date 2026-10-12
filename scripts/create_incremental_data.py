import pandas as pd
import random


INPUT_FILE = r"C:\Desktop\gcp-data-lakehouse-realtime\data\sample\orders.csv"
OUTPUT_FILE = r"C:\Desktop\gcp-data-lakehouse-realtime\data\quality_test\orders_incremental.csv"

random.seed(42)

# Read everything as strings first so IDs keep their exact format
df = pd.read_csv(INPUT_FILE, dtype=str)

# Convert only the fields that need numeric/timestamp operations
df["quantity"] = pd.to_numeric(df["quantity"])
df["amount"] = pd.to_numeric(df["amount"])
df["updated_at"] = pd.to_datetime(df["updated_at"])

watermark = pd.Timestamp("2026-09-29 21:57:10")

# Select 4 existing records before the watermark
existing = df[df["updated_at"] <= watermark].head(4).copy()

incremental_records = []

# --------------------------------------------------
# 1. NEW ORDERS - 10
# --------------------------------------------------

# Use REAL customer/product IDs from production data
customer_ids = df["customer_id"].dropna().tolist()
product_ids = df["product_id"].dropna().tolist()

for i in range(1, 11):
    incremental_records.append({
        "order_id": f"ORD_NEW_{i:03d}",
        "customer_id": random.choice(customer_ids),
        "product_id": random.choice(product_ids),
        "order_date": "2026-10-01",
        "quantity": random.randint(1, 5),
        "amount": round(random.uniform(100, 5000), 2),
        "status": "Completed",
        "updated_at": "2026-10-01 10:00:00"
    })

# --------------------------------------------------
# 2. UPDATES TO EXISTING ORDERS - 4
# --------------------------------------------------

for i, (_, row) in enumerate(existing.iterrows(), start=1):

    incremental_records.append({
        "order_id": row["order_id"],
        "customer_id": row["customer_id"],
        "product_id": row["product_id"],
        "order_date": row["order_date"],
        "quantity": int(row["quantity"]) + 1,
        "amount": round(float(row["amount"]) + 100, 2),
        "status": "Updated",
        "updated_at": "2026-10-01 11:00:00"
    })

# --------------------------------------------------
# 3. EXACT DUPLICATES - 3
# --------------------------------------------------

duplicate_records = incremental_records[:3].copy()

incremental_records.extend(duplicate_records)

# --------------------------------------------------
# 4. INVALID RECORDS - 3
# --------------------------------------------------

incremental_records.append({
    "order_id": "INVALID_INC_001",
    "customer_id": None,
    "product_id": product_ids[0],
    "order_date": "2026-10-01",
    "quantity": 2,
    "amount": 500,
    "status": "Completed",
    "updated_at": "2026-10-01 12:00:00"
})

incremental_records.append({
    "order_id": "INVALID_INC_002",
    "customer_id": customer_ids[0],
    "product_id": product_ids[0],
    "order_date": "2026-10-01",
    "quantity": 0,
    "amount": 500,
    "status": "Completed",
    "updated_at": "2026-10-01 12:00:00"
})

incremental_records.append({
    "order_id": "INVALID_INC_003",
    "customer_id": customer_ids[0],
    "product_id": product_ids[0],
    "order_date": "2026-10-01",
    "quantity": 2,
    "amount": -500,
    "status": "Completed",
    "updated_at": "2026-10-01 12:00:00"
})

# --------------------------------------------------
# CREATE DATAFRAME
# --------------------------------------------------

incremental_df = pd.DataFrame(incremental_records)

incremental_df.to_csv(
    OUTPUT_FILE,
    index=False
)

print("Incremental dataset created successfully.")
print(f"Total records: {len(incremental_df)}")
print("New orders: 10")
print("Updated orders: 4")
print("Duplicate records: 3")
print("Invalid records: 3")
print(f"Output: {OUTPUT_FILE}")