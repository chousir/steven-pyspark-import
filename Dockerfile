ARG BASE_IMAGE

FROM ${BASE_IMAGE}

COPY --chown=spark:spark es.jks /
COPY --chown=spark:spark pyspark-import/* /opt/spark/work-dir/
