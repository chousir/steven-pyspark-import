FROM spark:3.5.5-scala2.12-java17-python3-ubuntu

USER root

ARG KAFKA_CONNECTOR_VERSION=3.5.5
ARG ELASTICSEARCH_CONNECTOR_VERSION=8.17.4

ENV PYTHONUNBUFFERED=1

WORKDIR /opt/spark/work-dir

COPY requirements.txt ./
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY pyspark-import ./pyspark-import

# Resolve Spark connectors at build time and copy them into Spark's local jar path.
RUN set -eux; \
    mkdir -p /tmp/ivy; \
  printf 'print("connector resolution complete")\n' > /tmp/resolve_packages.py; \
    /opt/spark/bin/spark-submit \
      --master local[1] \
      --conf spark.jars.ivy=/tmp/ivy \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:${KAFKA_CONNECTOR_VERSION},org.elasticsearch:elasticsearch-spark-30_2.12:${ELASTICSEARCH_CONNECTOR_VERSION} \
      /tmp/resolve_packages.py; \
    cp -a /tmp/ivy/jars/*.jar /opt/spark/jars/; \
    rm -rf /tmp/ivy /tmp/resolve_packages.py

RUN chown -R spark:spark /opt/spark/work-dir /opt/spark/jars

USER spark
