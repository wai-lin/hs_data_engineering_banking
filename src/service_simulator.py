import json
import os
import random
import signal
import threading
import time
from typing import Dict

from faker import Faker
from kafka import KafkaProducer
from dotenv import load_dotenv


load_dotenv()
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL")
RAW_TRANSACTIONS_TOPIC = os.getenv("RAW_TRANSACTIONS_TOPIC")

DEFAULT_INTERVALS: Dict[str, float] = {
    "Mobile App": float(os.getenv("INTERVAL_MOBILE", "1.0")),
    "ATM": float(os.getenv("INTERVAL_ATM", "2.0")),
    "Card Processor": float(os.getenv("INTERVAL_CARD", "1.5")),
}

MAX_QUEUE_BUFFERED_MS = int(os.getenv("MAX_QUEUE_BUFFERED_MS", "5000"))

fake = Faker()
shutdown_flag = threading.Event()


# ---------------- Kafka Producer ----------------
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


def generate_transaction(source_type: str) -> dict:
    """Generate a single fake transaction event."""
    event_id = fake.uuid4()
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    customer_id = f"C{random.randint(1, 100):03d}"
    amount = round(random.uniform(5.0, 1500.0), 2)
    currency = random.choice(["USD", "EUR", "GBP", "CAD"])
    from_account = f"ACC{random.randint(100000000, 999999999)}"
    to_account = f"ACC{random.randint(100000000, 999999999)}"
    transaction_type = random.choice(
        ["P2P_TRANSFER", "PURCHASE", "WITHDRAWAL", "DEPOSIT", "BILL_PAYMENT", "REFUND"]
    )
    device_id = f"DEV_{fake.md5()[:10]}"
    ip_address = fake.ipv4()
    location = fake.address().replace("\n", ", ")
    authentication_method = random.choice(
        ["PASSWORD", "PIN", "BIOMETRIC_SUCCESS", "OTP_SUCCESS", "NONE"]
    )
    is_international = random.choice([True, False])

    return {
        "event_id": event_id,
        "timestamp": timestamp,
        "customer_id": customer_id,
        "amount": amount,
        "currency": currency,
        "from_account": from_account,
        "to_account": to_account,
        "transaction_type": transaction_type,
        "device_id": device_id,
        "ip_address": ip_address,
        "location": location,
        "authentication_method": authentication_method,
        "is_international": is_international,
        "source_platform": source_type,
    }


def producer_loop(source_type: str, interval_seconds: float):
    """Continuously produce transactions for a specific source at a fixed interval."""
    print(f"[{source_type}] Producer started with interval {interval_seconds}s")
    next_tick = time.monotonic()

    while not shutdown_flag.is_set():
        try:
            txn = generate_transaction(source_type)
            producer.send(
                RAW_TRANSACTIONS_TOPIC,
                value=txn,
                key=txn["customer_id"].encode("utf-8"),
            )
        except Exception as e:
            print(f"[{source_type}] Error sending message: {e}")

        next_tick += interval_seconds
        sleep_for = max(0.0, next_tick - time.monotonic())
        time.sleep(sleep_for)

    print(f"[{source_type}] Producer stopping...")


def graceful_shutdown(signum=None, frame=None):
    print("Shutdown signal received. Flushing and closing producer...")
    shutdown_flag.set()


def main():
    # Handle Ctrl+C and SIGTERM
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    print("Starting continuous transaction generation...")
    print(f"Kafka broker: {KAFKA_BROKER_URL}")
    print(f"Topic: {RAW_TRANSACTIONS_TOPIC}")
    for src, interval in DEFAULT_INTERVALS.items():
        print(f"  - {src}: every {interval}s")

    threads = []
    for source, interval in DEFAULT_INTERVALS.items():
        t = threading.Thread(target=producer_loop, args=(source, interval), daemon=True)
        threads.append(t)
        t.start()

    try:
        while not shutdown_flag.is_set():
            time.sleep(0.5)
    finally:
        try:
            producer.flush(MAX_QUEUE_BUFFERED_MS)
        except Exception as e:
            print(f"Flush error: {e}")
        producer.close()
        print("Producer closed. Bye.")


if __name__ == "__main__":
    main()
