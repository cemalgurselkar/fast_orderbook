# Builds the Airflow image with MarketStream platform dependencies.
FROM apache/airflow:3.1.0

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN mkdir -p /opt/fast-orderbook \
    && chown -R airflow:root /opt/fast-orderbook

COPY --chown=airflow:root pyproject.toml /opt/fast-orderbook/pyproject.toml
COPY --chown=airflow:root src /opt/fast-orderbook/src

USER airflow

RUN pip install --no-cache-dir "/opt/fast-orderbook[platform]"
