# source: Postgres
# target: Clickhouse
# transform_engine: Spark
# strategy: every 10m timeframe
# Orchestrator: Airflow

from __future__ import annotations
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from clickhouse_connect import get_client
from airflow.operators.python import get_current_context
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os
import shutil


# DAG
DAG_ID = "etl_10m_pg_to_ch"
SCHEDULE = "*/5 * * * *"

# from container name
OLTP_CONN_ID = "oltp_postgres"
OLAP_CONN_ID = "olap_clickhouse"

# tables
PG_CUSTOMERS = "public.customers"
PG_TRANSACTIONS = "public.transactions"


def ts_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2025, 9, 25),
    schedule=SCHEDULE,
    catchup=False,
    default_args={
        "owner": "nyein_chan",
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["etl", "etl_transactions"],
    max_active_runs=1,
)
def etl_10m_pg_to_ch():
    @task
    def ensure_postgres_tables():
        pg = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {PG_CUSTOMERS} (
            customer_id VARCHAR PRIMARY KEY,
            name TEXT,
            email TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS {PG_TRANSACTIONS} (
            event_id UUID PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            customer_id VARCHAR REFERENCES {PG_CUSTOMERS}(customer_id),
            amount NUMERIC(12,2) NOT NULL,
            currency VARCHAR(3) NOT NULL,
            from_account VARCHAR,
            to_account VARCHAR,
            transaction_type VARCHAR,
            device_id VARCHAR,
            ip_address VARCHAR,
            location TEXT,
            authentication_method VARCHAR,
            is_international BOOLEAN DEFAULT FALSE,
            fraud_status VARCHAR(20),
            fraud_detection_reason TEXT,
            source_platform VARCHAR(50),
            ingestion_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        pg.run(create_sql)

    @task
    def ensure_clickhouse_schema():
        conn = BaseHook.get_connection(OLAP_CONN_ID)
        ch = get_client(
            host=conn.host,
            port=conn.port or 8123,
            username=conn.login or "default",
            password=conn.password or "",
            database=conn.schema or "default",
        )
        db = conn.schema or "staging"
        ch.query(f"CREATE DATABASE IF NOT EXISTS {db}")

        dim_table_sqls = [
            f"""
                CREATE TABLE IF NOT EXISTS {db}.dim_customer (
                    customer_id String,
                    name String,
                    email String
                ) ENGINE = MergeTree ORDER BY customer_id
            """,
            f"""
                CREATE TABLE IF NOT EXISTS {db}.dim_transaction_type (
                    transaction_type String,
                    authentication_method String,
                    is_international Bool,
                    source_platform Nullable(String)
                ) ENGINE = MergeTree ORDER BY transaction_type
            """,
            f"""
                CREATE TABLE IF NOT EXISTS {db}.dim_location (
                    location String,
                    ip_address String,
                    device_id String
                ) ENGINE = MergeTree ORDER BY location
            """,
            f"""
                CREATE TABLE IF NOT EXISTS {db}.dim_fraud_status (
                    fraud_status String,
                    fraud_detection_reason Nullable(String),
                ) ENGINE = MergeTree ORDER BY fraud_status
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {db}.dim_time (
                    full_date Date,
                    day_of_week LowCardinality(String),
                    hour_of_day UInt8,
                    month LowCardinality(String),
                    year UInt16
                ) ENGINE = MergeTree
                ORDER BY (full_date, hour_of_day)
            """,
        ]
        for sql in dim_table_sqls:
            ch.query(sql)

        create_fact_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {db}.fact_txn_events (
                event_id String,
                customer_id String,
                amount Decimal(19,4),
                currency String,
                transaction_type String,
                location String,
                fraud_status String,
                timestamp DateTime64(3)
            ) 
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(timestamp)
            ORDER BY (event_id, timestamp)
        """
        ch.query(create_fact_table_sql)

        row_count = ch.query(f"SELECT count() FROM {db}.dim_time").result_rows[0][0]
        if row_count == 0:
            start_date = "2025-01-01"
            end_date = "2025-12-31"

            seed_dim_sql = f"""
                INSERT INTO {db}.dim_time
                SELECT
                    d AS full_date,
                    toDayOfWeek(d)        AS day_of_week,   -- 1=Mon … 7=Sun
                    toUInt8(h)            AS hour_of_day,   -- 0..23
                    toMonth(d)            AS month,         -- 1..12
                    toYear(d)             AS year
                FROM
                (
                    SELECT toDate('{start_date}') + number AS d
                    FROM numbers(dateDiff('day', toDate('{start_date}'), toDate('{end_date}')) + 1)
                ) AS dates
                CROSS JOIN
                (
                    SELECT number AS h FROM numbers(24)
                ) AS hours
            """
            ch.query(seed_dim_sql)

    @task
    def compute_time() -> dict:
        ctx = get_current_context()
        start = ctx["data_interval_start"].to_iso8601_string()
        end = ctx["data_interval_end"].to_iso8601_string()
        print(f"start: {start}, end: {end}")
        # {'start': '2025-09-25T15:50:00Z', 'end': '2025-09-25T16:00:00Z'}
        return {
            "start": start,
            "end": end,
        }

    @task
    def extract(window_time):
        pg_hook = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
        spark = (
            SparkSession.builder.appName("spark_etl_job")
            .config("spark.driver.memory", "512m")
            .config("spark.executor.memory", "512m")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

        schema = T.StructType(
            [
                T.StructField("event_id", T.StringType(), False),
                T.StructField("timestamp", T.TimestampType(), False),
                T.StructField("customer_id", T.StringType(), True),
                T.StructField("amount", T.DoubleType(), False),
                T.StructField("currency", T.StringType(), False),
                T.StructField("from_account", T.StringType(), True),
                T.StructField("to_account", T.StringType(), True),
                T.StructField("transaction_type", T.StringType(), True),
                T.StructField("device_id", T.StringType(), True),
                T.StructField("ip_address", T.StringType(), True),
                T.StructField("location", T.StringType(), True),
                T.StructField("authentication_method", T.StringType(), True),
                T.StructField("is_international", T.BooleanType(), True),
                T.StructField("source_platform", T.StringType(), True),
                T.StructField("fraud_status", T.StringType(), True),
                T.StructField("fraud_detection_reason", T.StringType(), True),
                T.StructField("ingestion_timestamp", T.TimestampType(), True),
                T.StructField("name", T.StringType(), True),
                T.StructField("email", T.StringType(), True),
            ]
        )

        txn_query = f"""
            SELECT t.*, c.name, c.email
            FROM {PG_TRANSACTIONS} t
            LEFT JOIN {PG_CUSTOMERS} c ON t.customer_id = c.customer_id
            WHERE t.timestamp >= %(start)s::timestamptz
            AND t.timestamp <  %(end)s::timestamptz
        """

        df = pg_hook.get_pandas_df(
            txn_query,
            parameters={
                "start": window_time["start"],
                "end": window_time["end"],
            },
        )
        spark_df = spark.createDataFrame(df, schema=schema)

        out_dir = "/opt/airflow/tmp"
        os.makedirs(out_dir, exist_ok=True)
        folder_path = os.path.join(out_dir, "transactions")
        spark_df.write.mode("overwrite").parquet(folder_path)
        return folder_path

    @task
    def transform(folder_path: str):
        spark = (
            SparkSession.builder.appName("spark_etl_job")
            .config("spark.driver.memory", "512m")
            .config("spark.executor.memory", "512m")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        df = spark.read.parquet(f"{folder_path}/*.parquet")

        # dimensions
        dim_customer = df.select("customer_id", "name", "email").dropDuplicates(
            ["customer_id"]
        )

        dim_fraud_status = df.select(
            "fraud_status", "fraud_detection_reason"
        ).dropDuplicates(["fraud_status"])

        dim_location = df.select("location", "ip_address", "device_id").dropDuplicates(
            ["location"]
        )

        dim_transaction_type = df.select(
            "transaction_type",
            "authentication_method",
            "is_international",
        ).dropDuplicates(["transaction_type"])

        # fact table
        fact_txn_events = df.select(
            F.col("event_id"),
            "customer_id",
            "amount",
            "currency",
            "transaction_type",
            "location",
            "fraud_status",
            "timestamp",
        )

        base_dir = "/opt/airflow/tmp/star_schema"
        dim_customer.write.mode("overwrite").parquet(f"{base_dir}/dim_customer")
        dim_fraud_status.write.mode("overwrite").parquet(f"{base_dir}/dim_fraud_status")
        dim_location.write.mode("overwrite").parquet(f"{base_dir}/dim_location")
        dim_transaction_type.write.mode("overwrite").parquet(
            f"{base_dir}/dim_transaction_type"
        )
        fact_txn_events.write.mode("overwrite").parquet(f"{base_dir}/fact_txn_events")
        print(f"base dir: {base_dir}")

        try:
            shutil.rmtree(folder_path)
        except FileNotFoundError:
            print(f"Temp file already gone: {folder_path}")

        return base_dir

    @task
    def load(base_dir: str):
        ch_conn = BaseHook.get_connection(OLAP_CONN_ID)
        ch = get_client(
            host=ch_conn.host,
            port=ch_conn.port or 8123,
            username=ch_conn.login or "default",
            password=ch_conn.password or "",
            database=ch_conn.schema or "staging",
        )

        spark = (
            SparkSession.builder.appName("spark_loader")
            .config("spark.driver.memory", "512m")
            .config("spark.executor.memory", "512m")
            .getOrCreate()
        )

        schemas = {
            "dim_customer": T.StructType(
                [
                    T.StructField("customer_id", T.StringType(), False),
                    T.StructField("name", T.StringType(), True),
                    T.StructField("email", T.StringType(), True),
                ]
            ),
            "dim_fraud_status": T.StructType(
                [
                    T.StructField("fraud_status", T.StringType(), False),
                ]
            ),
            "dim_location": T.StructType(
                [
                    T.StructField("location", T.StringType(), True),
                    T.StructField("ip_address", T.StringType(), True),
                    T.StructField("device_id", T.StringType(), True),
                ]
            ),
            "dim_transaction_type": T.StructType(
                [
                    T.StructField("transaction_type", T.StringType(), False),
                    T.StructField("authentication_method", T.StringType(), True),
                    T.StructField("is_international", T.BooleanType(), True),
                    T.StructField("source_platform", T.StringType(), True),
                ]
            ),
            "fact_txn_events": T.StructType(
                [
                    T.StructField("event_id", T.StringType(), False),
                    T.StructField("customer_id", T.StringType(), False),
                    T.StructField("amount", T.DecimalType(19, 4), False),
                    T.StructField("currency", T.StringType(), False),
                    T.StructField("transaction_type", T.StringType(), True),
                    T.StructField("location", T.StringType(), True),
                    T.StructField("fraud_status", T.StringType(), False),
                    T.StructField("timestamp", T.TimestampType(), False),
                ]
            ),
        }

        for table, schema in schemas.items():
            parquet_path = os.path.join(base_dir, table)
            if not os.path.exists(parquet_path):
                continue
            sdf = spark.read.parquet(f"{parquet_path}/*.parquet")
            pdf = sdf.toPandas()
            if pdf.empty:
                continue
            ch.insert_df(table, pdf)

        try:
            shutil.rmtree(base_dir)
        except FileNotFoundError:
            print(f"Temp file already gone: {base_dir}")
        return "done"

    # tasks
    postgres_ready = ensure_postgres_tables()
    clickhouse_ready = ensure_clickhouse_schema()
    window_time = compute_time()
    extract_parquet = extract(window_time)
    extract_fact_and_dim = transform(extract_parquet)
    dump_to_clickhouse = load(extract_fact_and_dim)

    (
        [postgres_ready, clickhouse_ready]
        >> window_time
        >> extract_parquet
        >> extract_fact_and_dim
        >> dump_to_clickhouse
    )


dag_inst = etl_10m_pg_to_ch()
