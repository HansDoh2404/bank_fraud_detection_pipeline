import os
import csv
import uuid
import random


from datetime import datetime, timedelta
from faker import Faker

# Config
DATA_DIR = "./data"
NUM_TRANSACTIONS = 500000
NUM_CUSTOMERS = 20000
NUM_MERCHANTS = 500
FRAUD_RATE = 0.02  # 2% fraude

fake = Faker()
random.seed(42)

os.makedirs(DATA_DIR, exist_ok=True)

def generate_bank_transactions():
    file_path = os.path.join(
        DATA_DIR, f"bank_transactions_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    )

    fieldnames = [
        "transaction_id", "customer_id", "transaction_amount",
        "transaction_timestamp", "merchant_id", "merchant_category",
        "payment_type", "country", "device_type",
        "account_age_days", "is_foreign_transaction",
        "transaction_frequency_24h", "avg_amount_7d",
        "fraud_label"
    ]

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for _ in range(NUM_TRANSACTIONS):
            customer_id = random.randint(1, NUM_CUSTOMERS)
            amount = round(random.uniform(1, 3000), 2)
            account_age = random.randint(1, 4000)

            is_foreign = random.random() < 0.1
            freq_24h = random.randint(1, 20)
            avg_7d = round(random.uniform(10, 500), 2)

            fraud = 0
            risk_score = 0

            if amount > 2000:
                risk_score += 2
            if is_foreign:
                risk_score += 2
            if freq_24h > 10:
                risk_score += 1
            if account_age < 30:
                risk_score += 2

            if risk_score >= 4 and random.random() < 0.7:
                fraud = 1
            elif random.random() < FRAUD_RATE:
                fraud = 1

            row = {
                "transaction_id": str(uuid.uuid4()),
                "customer_id": customer_id,
                "transaction_amount": amount,
                "transaction_timestamp": (
                    datetime.now() - timedelta(minutes=random.randint(0, 100000))
                ).isoformat(),
                "merchant_id": random.randint(1, NUM_MERCHANTS),
                "merchant_category": random.choice([
                    "electronics", "grocery", "travel",
                    "fashion", "fuel", "online_services"
                ]),
                "payment_type": random.choice([
                    "credit_card", "debit_card", "mobile_payment"
                ]),
                "country": random.choice(["FR", "DE", "US", "ES", "IT", "CN"]),
                "device_type": random.choice([
                    "mobile", "desktop", "tablet"
                ]),
                "account_age_days": account_age,
                "is_foreign_transaction": int(is_foreign),
                "transaction_frequency_24h": freq_24h,
                "avg_amount_7d": avg_7d,
                "fraud_label": fraud
            }

            writer.writerow(row)

    print(f"[OK] Dataset généré : {file_path}")

if __name__ == "__main__":
    generate_bank_transactions()
