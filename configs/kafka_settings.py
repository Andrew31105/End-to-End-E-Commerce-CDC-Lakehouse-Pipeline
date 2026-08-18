import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ecommerce_group")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_topic")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

KAFKA_CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
    "enable.auto.commit": False,
    "fetch.min.bytes": 1,
    "max.poll.interval.ms": 300000,
}

KAFKA_PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 5,
    "linger.ms": 5,
    "batch.size": 16384,
}
