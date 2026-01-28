# Config
DATA_DIR = "./data"
NUM_TRANSACTIONS = 500000
NUM_CUSTOMERS = 20000
NUM_MERCHANTS = 500
FRAUD_RATE = 0.02  # 2% fraude
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092" # "192.168.100.104:9092"
STREAM_INTERVAL = 10  # secondes entre les batches
TOPIC = "transactions"
GROUP_ID = "minio-consumer-group"
S3_URL = "localhost:9000"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password"
S3_BUCKET = "bronze"
BATCH_SIZE = 1000

producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "linger.ms": 10,
    "batch.num.messages": 1000,
    "queue.buffering.max.messages": 100000,  # taille du buffer en messages
    "queue.buffering.max.kbytes": 1048576,   # buffer max en Ko
    "acks": "all",
}

consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
}