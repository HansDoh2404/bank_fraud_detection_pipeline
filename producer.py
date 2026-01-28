import os
import json
import time
import csv
import uuid
import random

from config import producer_conf, DATA_DIR, NUM_CUSTOMERS, NUM_TRANSACTIONS, NUM_MERCHANTS, FRAUD_RATE, KAFKA_BOOTSTRAP_SERVERS, STREAM_INTERVAL, TOPIC, S3_ACCESS_KEY, S3_SECRET_KEY, S3_URL
from datetime import datetime, timedelta
from faker import Faker
from faker_commerce import Provider
from confluent_kafka import Producer

fake = Faker()
random.seed(42)
producer = Producer(producer_conf)

os.makedirs(DATA_DIR, exist_ok=True)

def delivery_report(err, msg):
    if err is not None:
        print(f"[DELIVERY ERROR] {err}")

def generate_bank_transactions():
    file_name = f"bank_transactions_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    file_path = os.path.join(
        DATA_DIR, f"{file_name}.csv"
    )

    fieldnames = [
        "transaction_id", "customer_id", "transaction_amount",
        "transaction_timestamp", "merchant_id", "merchant_category",
        "payment_type", "country", "device_type",
        "account_age_days", "is_foreign_transaction",
        "transaction_frequency_24h", "avg_amount_7d",
        "fraud_label", "file_name"
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

            transaction = {
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
                "fraud_label": fraud, 
                "file_name": file_name
            }
            key = str(transaction['customer_id'])
            producer.produce(
                topic=TOPIC,
                key=key,
                value=json.dumps(transaction),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            writer.writerow(transaction)
        producer.flush()

    print(f"[OK] Dataset généré : {file_path}")

while True:
    generate_bank_transactions()
    time.sleep(STREAM_INTERVAL)
