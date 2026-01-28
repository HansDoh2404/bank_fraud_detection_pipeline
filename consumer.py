import io
import json
from confluent_kafka import Consumer
from config import consumer_conf, BATCH_SIZE, S3_ACCESS_KEY, S3_BUCKET, S3_SECRET_KEY, S3_URL, TOPIC
from minio import Minio
from datetime import datetime

minio_client = Minio(
    S3_URL,
    access_key=S3_ACCESS_KEY,
    secret_key=S3_SECRET_KEY,
    secure=False
)

consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC])
batch_number = 0
buffer = []

while True:
    msgs = consumer.consume(num_messages=10000, timeout=15.0)

    if not msgs:
        print("Aucun message reçu en 15 secondes, arrêt du consumer.")
        consumer.close()
        exit(0)

    last_msg = None

    for msg in msgs:
        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue

        data = json.loads(msg.value().decode("utf-8"))
        buffer.append(data)
        last_msg = msg   # on garde le dernier message valide

    if len(buffer) >= BATCH_SIZE:
        if batch_number == 50 :
            batch_number = 1
        else :
            batch_number += 1
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        file_name = f"consumed_{timestamp}_file_{data['file_name']}.json"

        json_bytes = json.dumps(buffer).encode("utf-8")

        minio_client.put_object(
            bucket_name=S3_BUCKET,
            object_name=file_name,
            data=io.BytesIO(json_bytes),
            length=len(json_bytes),
            content_type="application/json"
        )

        print(f"Batch number {batch_number} of {len(buffer)} records to MinIO: {file_name}")

        consumer.commit(message=last_msg, asynchronous=False)
        buffer.clear()
