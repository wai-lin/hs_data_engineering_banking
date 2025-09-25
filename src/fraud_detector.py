import json
import os
import signal
import threading
import time
import random
from kafka import KafkaConsumer, KafkaProducer

# ---------------- Configuration ----------------
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
RAW_TRANSACTIONS_TOPIC = os.getenv(
    "RAW_TRANSACTIONS_TOPIC", "transactions_raw")
PROCESSED_TRANSACTIONS_TOPIC = os.getenv(
    "PROCESSED_TRANSACTIONS_TOPIC", "transactions_processed")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "fraud_detector_group")

# Chance of a transaction being marked as 'suspicious' (e.g., 0.05 for 5%)
FRAUD_PROBABILITY = float(os.getenv("FRAUD_PROBABILITY", "0.05"))
# How long consumer waits for messages
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "1000"))

# ---------------- Globals ----------------
shutdown_flag = threading.Event()

# ---------------- Kafka Producer Setup (for processed transactions) ----------------
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER_URL],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
    linger_ms=10,
    request_timeout_ms=30000,
    max_in_flight_requests_per_connection=5,
    buffer_memory=64 * 1024 * 1024,  # 64MB
)

# ---------------- Kafka Consumer Setup (for raw transactions) ----------------
consumer = KafkaConsumer(
    RAW_TRANSACTIONS_TOPIC,
    bootstrap_servers=[KAFKA_BROKER_URL],
    group_id=CONSUMER_GROUP_ID,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


def detect_fraud_by_chance(transaction: dict) -> dict:
    """
    Simulates fraud detection using chance-based randomization.
    Adds a 'fraud_status' field to the transaction.
    """
    is_suspicious = random.random() < FRAUD_PROBABILITY

    transaction['fraud_status'] = 'suspicious' if is_suspicious else 'clean'

    if is_suspicious:
        transaction['fraud_detection_reason'] = "Simulated: Random chance"
        print(
            f"FRAUD DETECTED (Simulated): Event ID {transaction['event_id']}, Amount {transaction['amount']}, Customer {transaction['customer_id']}")

    return transaction


def fraud_detection_loop():
    """Main loop for the fraud detector."""
    print(
        f"Fraud detector started, listening to '{RAW_TRANSACTIONS_TOPIC}'...")
    print(
        f"Will mark {FRAUD_PROBABILITY*100:.2f}% of transactions as 'suspicious'.")

    while not shutdown_flag.is_set():
        try:
            messages = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)

            if not messages:
                time.sleep(0.1)
                continue

            for topic_partition, records in messages.items():
                for record in records:
                    raw_transaction = record.value
                    if raw_transaction is None:
                        print(
                            f"Received malformed message (None value) from offset {record.offset} in {topic_partition}")
                        continue

                    processed_transaction = detect_fraud_by_chance(
                        raw_transaction)

                    future = producer.send(
                        PROCESSED_TRANSACTIONS_TOPIC,
                        value=processed_transaction,
                        key=processed_transaction['customer_id'].encode(
                            'utf-8')
                    )

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON message: {e}")
        except Exception as e:
            print(f"An unexpected error occurred in fraud detection loop: {e}")

    print("Fraud detector stopping...")


def graceful_shutdown(signum=None, frame=None):
    print("\nShutdown signal received. Initiating graceful shutdown...")
    shutdown_flag.set()


def main():
    # Handle Ctrl+C and SIGTERM
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    try:
        fraud_detection_loop()
    finally:
        try:
            producer.flush(timeout=5000)  # Flush for 5 seconds
        except Exception as e:
            print(f"Producer flush error: {e}")
        producer.close()

        consumer.close()
        print("Kafka producer and consumer closed. Exiting.")


if __name__ == "__main__":
    main()
