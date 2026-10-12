import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

OUTPUT_DIR = Path("../data/sample")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# Configuration
# --------------------------------------------------

NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 100
NUM_ORDERS = 10000

START_DATE = datetime(2026, 1, 1)
END_DATE = datetime(2026, 9, 30)

# --------------------------------------------------
# Reference data
# --------------------------------------------------

cities = [
    ("Hyderabad", "Telangana"),
    ("Bengaluru", "Karnataka"),
    ("Mumbai", "Maharashtra"),
    ("Pune", "Maharashtra"),
    ("Chennai", "Tamil Nadu"),
    ("Delhi", "Delhi"),
    ("Kolkata", "West Bengal"),
    ("Ahmedabad", "Gujarat"),
]

categories = [
    "Electronics",
    "Home Appliances",
    "Fashion",
    "Books",
    "Sports",
    "Beauty",
]

product_names = [
    "Wireless Mouse",
    "Mechanical Keyboard",
    "Laptop",
    "Monitor",
    "Headphones",
    "Smartphone",
    "USB Cable",
    "Power Bank",
    "Smart Watch",
    "Bluetooth Speaker",
]

payment_methods = [
    "UPI",
    "CARD",
    "NET_BANKING",
    "COD",
]

order_statuses = [
    "COMPLETED",
    "COMPLETED",
    "COMPLETED",
    "SHIPPED",
    "PENDING",
    "CANCELLED",
]

payment_statuses = [
    "SUCCESS",
    "SUCCESS",
    "SUCCESS",
    "PENDING",
    "FAILED",
]

# --------------------------------------------------
# Helper functions
# --------------------------------------------------

def random_date(start, end):
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)


# --------------------------------------------------
# 1. Generate Customers
# --------------------------------------------------

customers = []

for i in range(1, NUM_CUSTOMERS + 1):

    city, state = random.choice(cities)

    customers.append({
        "customer_id": f"C{i:05d}",
        "customer_name": f"Customer_{i}",
        "email": f"customer{i}@gmail.com",
        "city": city,
        "state": state,
        "country": "India",
        "signup_date": random_date(
            datetime(2024, 1, 1),
            datetime(2026, 9, 1)
        ).strftime("%Y-%m-%d"),
    })

with open(
    OUTPUT_DIR / "customers.csv",
    "w",
    newline="",
    encoding="utf-8"
) as f:

    writer = csv.DictWriter(
        f,
        fieldnames=customers[0].keys()
    )

    writer.writeheader()
    writer.writerows(customers)


# --------------------------------------------------
# 2. Generate Products
# --------------------------------------------------

products = []

for i in range(1, NUM_PRODUCTS + 1):

    product_name = random.choice(product_names)

    products.append({
        "product_id": f"P{i:05d}",
        "product_name": f"{product_name}_{i}",
        "category": random.choice(categories),
        "price": round(
            random.uniform(300, 100000),
            2
        ),
    })

with open(
    OUTPUT_DIR / "products.csv",
    "w",
    newline="",
    encoding="utf-8"
) as f:

    writer = csv.DictWriter(
        f,
        fieldnames=products[0].keys()
    )

    writer.writeheader()
    writer.writerows(products)


# --------------------------------------------------
# 3. Generate Orders
# --------------------------------------------------

orders = []

for i in range(1, NUM_ORDERS + 1):

    product = random.choice(products)

    quantity = random.randint(1, 5)

    price = float(product["price"])

    amount = round(
        price * quantity,
        2
    )

    order_datetime = random_date(
        START_DATE,
        END_DATE
    )

    orders.append({
        "order_id": f"O{i:06d}",
        "customer_id": random.choice(customers)["customer_id"],
        "product_id": product["product_id"],
        "order_date": order_datetime.strftime("%Y-%m-%d"),
        "quantity": quantity,
        "amount": amount,
        "status": random.choice(order_statuses),
        "updated_at": order_datetime.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    })

with open(
    OUTPUT_DIR / "orders.csv",
    "w",
    newline="",
    encoding="utf-8"
) as f:

    writer = csv.DictWriter(
        f,
        fieldnames=orders[0].keys()
    )

    writer.writeheader()
    writer.writerows(orders)


# --------------------------------------------------
# 4. Generate Payments
# --------------------------------------------------

payments = []

for i, order in enumerate(orders, start=1):

    payments.append({
        "payment_id": f"PAY{i:06d}",
        "order_id": order["order_id"],
        "payment_method": random.choice(payment_methods),
        "payment_status": random.choice(payment_statuses),
        "payment_date": order["order_date"],
    })

with open(
    OUTPUT_DIR / "payments.csv",
    "w",
    newline="",
    encoding="utf-8"
) as f:

    writer = csv.DictWriter(
        f,
        fieldnames=payments[0].keys()
    )

    writer.writeheader()
    writer.writerows(payments)


print("Data generation completed!")
print(f"Output directory: {OUTPUT_DIR.resolve()}")
print(f"Customers: {len(customers)}")
print(f"Products: {len(products)}")
print(f"Orders: {len(orders)}")
print(f"Payments: {len(payments)}")