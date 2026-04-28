"""
main.py
─────────────────────────────────────────────────────────────
PySpark Streaming: Kafka (Suricata BGP eve.json)
                   → Elasticsearch
                   → FTP Server (JSON files)
─────────────────────────────────────────────────────────────
Read Kafka messages and write them to Elasticsearch and/or FTP
via configurable sinks using PySpark Structured Streaming.
"""

import os
from functools import partial

from pyspark.sql import SparkSession
from pyhocon import ConfigFactory

from parse_bgp import parse_bgp_updates
from writer import write_batch_to_es, write_batch_to_ftp, write_batch_to_all


# ─────────────────────────────────────────────
# Configuration Loading (HOCON format)
# ─────────────────────────────────────────────
# Supports environment variable IMPORT_CONFIG to override
# default config path
config_path = os.getenv("IMPORT_CONFIG", "application.conf")
config = ConfigFactory.parse_file(config_path)


def get_conf(*keys: str, default=None, required: bool = True):
    for key in keys:
        try:
            return config.get(key)
        except Exception:
            continue

    if required:
        raise KeyError(f"Missing required config key. Tried: {', '.join(keys)}")
    return default


checkpoint_location = get_conf("spark.checkpoint", "spark.checkpoint")
spark_trigger_processing_time = str(
    get_conf("spark.triggerProcessingTime", default="30 seconds", required=False)
)

kafka_bootstrap_servers = get_conf("kafka.input.brokers", "kafka.input.brokers")
kafka_topic = get_conf("kafka.input.topic")
kafka_starting_offsets = get_conf("kafka.input.startingOffsets")
kafka_max_offsets_per_trigger = str(get_conf("kafka.input.maxOffsetsPerTrigger"))

elasticsearch_url = get_conf("elasticsearch.url")
elasticsearch_user = get_conf("elasticsearch.user")
elasticsearch_password = get_conf("elasticsearch.password")
elasticsearch_cert = get_conf("elasticsearch.cert")
elasticsearch_cert_password = get_conf(
    "elasticsearch.cert_password",
    "elasticsearch.elasticsearch_cert_password",
)
elasticsearch_index = get_conf("elasticsearch.index")
elasticsearch_port = str(get_conf("elasticsearch.port"))

ftp_hostname = get_conf("ftp.hostname")
ftp_user = get_conf("ftp.user")
ftp_password = get_conf("ftp.password")
ftp_remote_path = get_conf("ftp.paths")

vlan_map_alias_url = get_conf("vlan_map_alias_url", required=False, default="")
default_alias = get_conf("default_alias", required=False, default="")
asn_csv_path = get_conf("asn_csv_path", required=False, default="as.csv")


# ─────────────────────────────────────────────
# Package configuration as dicts for passing to writers
# ─────────────────────────────────────────────
ES_KWARGS = dict(
    es_user          = elasticsearch_user,
    es_password      = elasticsearch_password,
    es_cert          = elasticsearch_cert,
    es_cert_password = elasticsearch_cert_password,
    es_url           = elasticsearch_url,
    es_index         = elasticsearch_index,
    es_port          = elasticsearch_port,
)

FTP_KWARGS = dict(
    ftp_hostname    = ftp_hostname,
    ftp_user        = ftp_user,
    ftp_password    = ftp_password,
    ftp_remote_path = ftp_remote_path,
)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    _kafka_consumer_logger = "org.apache.kafka.clients.consumer.internals.KafkaDataConsumer"
    spark = (
        SparkSession
        .builder
        .appName("EvelogImport")
        .config(
            "spark.driver.extraJavaOptions",
            f"-Dlog4j.logger.{_kafka_consumer_logger}=ERROR"
            f" -Dlog4j2.logger.kafkadataconsumer.name={_kafka_consumer_logger}"
            f" -Dlog4j2.logger.kafkadataconsumer.level=ERROR",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Read from Kafka ───────────────────
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe",               kafka_topic)
        .option("startingOffsets",         kafka_starting_offsets)
        .option("maxOffsetsPerTrigger",    kafka_max_offsets_per_trigger)
        .load()
    )

    # ── 2. Cast Kafka value to JSON string ──
    df_json = kafka_df.selectExpr("CAST(value AS STRING) AS value")

    # ── 3. Parse BGP UPDATE payload fields ─
    df_parsed = parse_bgp_updates(
        df_json,
        value_col="value",
        asn_csv_path=asn_csv_path,
        vlan_map_alias_url=vlan_map_alias_url,
        default_alias=default_alias,
    )

    # ── 4. Select sink mode ────────────────
    #
    #  ┌─ Elasticsearch only ───────────────────────────────────────┐
    #  │ sink_fn = partial(write_batch_to_es,  **ES_KWARGS)         │
    #  ├─ FTP only  ────────────────────────────────────────────────┤
    #  │ sink_fn = partial(write_batch_to_ftp, **FTP_KWARGS)        │
    #  ├─ Both (ES + FTP) ──────────────────────────────────────────┤
    #  │ sink_fn = partial(write_batch_to_all,                      │
    #  │               es_kwargs=ES_KWARGS, ftp_kwargs=FTP_KWARGS)  │
    #  └────────────────────────────────────────────────────────────┘
    #
    sink_fn = partial(
        write_batch_to_all,
        es_kwargs=ES_KWARGS,
        ftp_kwargs=FTP_KWARGS,
    )

    # ── 5. Start streaming ─────────────────
    query = (
        df_parsed.writeStream
        .foreachBatch(sink_fn)
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .trigger(processingTime=spark_trigger_processing_time)
        .start()
    )

    print("[Streaming] Query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
