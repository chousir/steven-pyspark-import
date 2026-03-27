"""
writer.py
─────────────────────────────────────────────────────────────
Sink writer modules
  - write_batch_to_es  : Write to Elasticsearch
  - write_batch_to_ftp : Upload to FTP server (UUID-named files)
  - write_batch_to_all : Write to both sinks synchronously
─────────────────────────────────────────────────────────────
"""

import io
import uuid
import ftplib

from pyspark.sql import DataFrame


# ─────────────────────────────────────────────
# Elasticsearch Sink
# ─────────────────────────────────────────────
def write_batch_to_es(
    batch_df: DataFrame,
    batch_id: int,
    *,
    es_user:           str,
    es_password:       str,
    es_cert:           str,
    es_cert_password:  str,
    es_url:            str,
    es_index:          str,
    es_port:           str,
) -> None:
    """
    Write each micro-batch of data to Elasticsearch.

    Parameters
    ----------
    batch_df        : The current micro-batch DataFrame
    batch_id        : Auto-incremented batch ID from Spark
    es_user         : Elasticsearch username
    es_password     : Elasticsearch password
    es_cert         : Path to TrustStore certificate
    es_cert_password: TrustStore password
    es_url          : Elasticsearch node URL
    es_index        : Target index name
    es_port         : Elasticsearch port
    """
    if batch_df.isEmpty():
        print(f"[ES] Batch {batch_id} is empty, skipping.")
        return

    (
        batch_df.write
        .format("org.elasticsearch.spark.sql")
        .option("es.net.http.auth.user",             es_user)
        .option("es.net.http.auth.pass",             es_password)
        .option("es.net.ssl",                        "true")
        .option("es.net.ssl.cert.allow.self.signed", "true")
        .option("es.net.ssl.truststore.location",    es_cert)
        .option("es.net.ssl.truststore.password",    es_cert_password)
        .option("es.nodes.wan.only",                 "true")
        .option("es.nodes",                          es_url)
        .option("es.resource",                       es_index)
        .option("es.port",                           es_port)
        .option("es.spark.dataframe.write.null",     "true")
        .mode("append")
        .save()
    )
    print(f"[ES] Batch {batch_id} written → index: {es_index}")


# ─────────────────────────────────────────────
# FTP Sink
# ─────────────────────────────────────────────
def write_batch_to_ftp(
    batch_df: DataFrame,
    batch_id: int,
    *,
    ftp_hostname:    str,
    ftp_user:        str,
    ftp_password:    str,
    ftp_remote_path: str,
) -> None:
    """
    Upload each micro-batch of data to FTP server in JSON Lines format.
    Each batch produces a UUID-named .json file to ensure uniqueness.

    Parameters
    ----------
    batch_df        : The current micro-batch DataFrame
    batch_id        : Auto-incremented batch ID from Spark
    ftp_hostname    : FTP server hostname
    ftp_user        : FTP username
    ftp_password    : FTP password
    ftp_remote_path : FTP remote directory path (e.g., /upload/suricata)
    """
    if batch_df.isEmpty():
        print(f"[FTP] Batch {batch_id} is empty, skipping.")
        return

    # Convert each row to JSON string and encode as bytes in JSON Lines format
    rows_json    = batch_df.toJSON().collect()
    json_content = "\n".join(rows_json).encode("utf-8")

    # Use UUID to ensure absolute uniqueness of filenames
    remote_file = f"{ftp_remote_path}/{uuid.uuid4()}.json"

    try:
        with ftplib.FTP(ftp_hostname) as ftp:
            ftp.login(user=ftp_user, passwd=ftp_password)
            ftp.storbinary(f"STOR {remote_file}", io.BytesIO(json_content))
        print(f"[FTP] Batch {batch_id} uploaded → {remote_file}")
    except ftplib.all_errors as e:
        print(f"[FTP] ERROR on batch {batch_id}: {e}")
        raise


# ─────────────────────────────────────────────
# Combined Sink (ES + FTP synchronously)
# ─────────────────────────────────────────────
def write_batch_to_all(
    batch_df: DataFrame,
    batch_id: int,
    *,
    es_kwargs:  dict,
    ftp_kwargs: dict,
) -> None:
    """
    Write synchronously to both Elasticsearch and FTP sinks.
    Failure in one sink does not prevent the other from executing.

    Parameters
    ----------
    batch_df   : The current micro-batch DataFrame
    batch_id   : Auto-incremented batch ID from Spark
    es_kwargs  : Keyword arguments dict for write_batch_to_es
    ftp_kwargs : Keyword arguments dict for write_batch_to_ftp
    """
    print(f"[Batch {batch_id}] Processing {batch_df.count()} records...")

    try:
        write_batch_to_es(batch_df, batch_id, **es_kwargs)
    except Exception as e:
        print(f"[ES]  FATAL on batch {batch_id}: {e}")

    try:
        write_batch_to_ftp(batch_df, batch_id, **ftp_kwargs)
    except Exception as e:
        print(f"[FTP] FATAL on batch {batch_id}: {e}")
