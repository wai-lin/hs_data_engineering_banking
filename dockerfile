FROM apache/airflow:2.11.0

USER root

# OpenJDK 17 for spark
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    clickhouse-connect \
    psycopg2-binary