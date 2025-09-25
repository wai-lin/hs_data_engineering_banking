import json
import os
import signal
import threading
import time
import psycopg2
from psycopg2 import extras
from kafka import KafkaConsumer
from typing import List

# ---------------- Configuration ----------------
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
PROCESSED_TRANSACTIONS_TOPIC = os.getenv(
    "PROCESSED_TRANSACTIONS_TOPIC", "transactions_processed")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "oltp_loader_group")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "bank_transactions")
POSTGRES_USER = os.getenv("POSTGRES_USER", "pg")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
BATCH_INTERVAL_SEC = float(os.getenv("BATCH_INTERVAL_SEC", "2.0"))
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "1000"))

# ---------------- Globals ----------------
shutdown_flag = threading.Event()
records_to_insert: List[dict] = []
last_flush_time = time.monotonic()


def get_db_connection():
    """Establishes and returns a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        print(
            f"Successfully connected to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

# ---------------- Data Insertion Function ----------------


def flush_batch(conn, cursor, batch):
    """Inserts a batch of records into the PostgreSQL transactions table."""
    if not batch:
        return

    insert_sql = """
        INSERT INTO transactions (
            event_id, timestamp, customer_id, amount, currency,
            from_account, to_account, transaction_type, device_id, ip_address,
            location, authentication_method, is_international, source_platform,
            fraud_status, fraud_detection_reason
        ) VALUES (
            %(event_id)s, %(timestamp)s, %(customer_id)s, %(amount)s, %(currency)s,
            %(from_account)s, %(to_account)s, %(transaction_type)s, %(device_id)s, %(ip_address)s,
            %(location)s, %(authentication_method)s, %(is_international)s, %(source_platform)s,
            %(fraud_status)s, %(fraud_detection_reason)s
        ) ON CONFLICT (event_id) DO NOTHING;
    """

    try:
        extras.execute_batch(cursor, insert_sql, batch, page_size=len(batch))
        conn.commit()
        print(f"Inserted {len(batch)} records into PostgreSQL.")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting batch into PostgreSQL: {e}")

# ---------------- Consumer Loop ----------------


def oltp_loader_loop():
    """Main loop for consuming Kafka messages and loading into OLTP."""
    global records_to_insert, last_flush_time

    consumer = KafkaConsumer(
        PROCESSED_TRANSACTIONS_TOPIC,
        bootstrap_servers=[KAFKA_BROKER_URL],
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print(
        f"OLTP loader started, listening to '{PROCESSED_TRANSACTIONS_TOPIC}'...")

    conn = None
    cursor = None
    while not conn and not shutdown_flag.is_set():
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
        else:
            print("Retrying DB connection in 5 seconds...")
            time.sleep(5)

    if shutdown_flag.is_set():
        if consumer:
            consumer.close()
        print("OLTP loader stopped before DB connection established.")
        return

    try:
        while not shutdown_flag.is_set():
            messages = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)

            if not messages:
                if len(records_to_insert) > 0 and (time.monotonic() - last_flush_time) >= BATCH_INTERVAL_SEC:
                    flush_batch(conn, cursor, records_to_insert)
                    records_to_insert = []
                    last_flush_time = time.monotonic()
                time.sleep(0.1)
                continue

            for topic_partition, records in messages.items():
                for record in records:
                    processed_transaction = record.value
                    if processed_transaction is None:
                        print(
                            f"Received malformed message (None value) from offset {record.offset} in {topic_partition}")
                        continue

                    records_to_insert.append(processed_transaction)

                    if len(records_to_insert) >= BATCH_SIZE:
                        flush_batch(conn, cursor, records_to_insert)
                        records_to_insert = []
                        last_flush_time = time.monotonic()

            if len(records_to_insert) > 0 and (time.monotonic() - last_flush_time) >= BATCH_INTERVAL_SEC:
                flush_batch(conn, cursor, records_to_insert)
                records_to_insert = []
                last_flush_time = time.monotonic()

    except Exception as e:
        print(f"An unexpected error occurred in OLTP loader loop: {e}")
    finally:
        # On shutdown or error, flush any remaining records and close connections
        if records_to_insert:
            print("Flushing remaining records before shutdown...")
            flush_batch(conn, cursor, records_to_insert)
            records_to_insert = []

        if cursor:
            cursor.close()
        if conn:
            conn.close()
        if consumer:
            consumer.close()
        print("OLTP loader gracefully shut down. DB and Kafka connections closed.")


def graceful_shutdown(signum=None, frame=None):
    print("\nShutdown signal received. Initiating graceful shutdown for OLTP loader...")
    shutdown_flag.set()


def main():
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    oltp_loader_loop()


if __name__ == "__main__":
    main()
