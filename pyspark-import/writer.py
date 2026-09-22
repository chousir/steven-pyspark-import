"""
writer.py
─────────────────────────────────────────────────────────────
Sink writer modules
  - write_batch_to_es  : Write to Elasticsearch
  - write_batch_to_ftp : Upload to FTP server (UUID-named files)
  - write_batch_to_all : Write to both sinks synchronously
─────────────────────────────────────────────────────────────
"""

from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel


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

    Each executor uploads its own partition directly to FTP without sending
    data back to the driver, avoiding driver OOM on large batches (this
    pipeline's maxOffsetsPerTrigger can be in the millions of rows). Each
    partition produces one UUID-named .json file.

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

    # Capture into local variables so the closure serializes only the
    # values, not the entire enclosing frame.
    _hostname = ftp_hostname
    _user     = ftp_user
    _password = ftp_password
    _path     = ftp_remote_path

    def _upload_partition(rows) -> None:
        import ftplib
        import io
        import json
        import uuid

        lines = [json.dumps(row.asDict(recursive=True)) for row in rows]
        if not lines:
            print("[FTP][partition] 0 rows, skipping upload")
            return
        content  = "\n".join(lines).encode("utf-8")
        filename = f"{uuid.uuid4()}.json"
        with ftplib.FTP(_hostname, timeout=30) as ftp:
            ftp.set_pasv(True)
            ftp.login(user=_user, passwd=_password)
            ftp.cwd(_path)
            ftp.storbinary(f"STOR {filename}", io.BytesIO(content))
        print(f"[FTP][partition] uploaded {len(lines)} rows → {filename}")

    num_partitions = batch_df.rdd.getNumPartitions()
    batch_df.foreachPartition(_upload_partition)
    print(f"[FTP] Batch {batch_id} done ({num_partitions} partitions) → {_path}")


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

    Failure in one sink does not prevent the other from being attempted,
    but any failure is re-raised once both have run. foreachBatch only
    advances the checkpoint offset when the batch function returns without
    raising, so swallowing a sink failure here would make Spark treat a
    partially-written (or fully lost) batch as successfully processed,
    with no retry and no record of the data loss beyond a log line.

    Parameters
    ----------
    batch_df   : The current micro-batch DataFrame
    batch_id   : Auto-incremented batch ID from Spark
    es_kwargs  : Keyword arguments dict for write_batch_to_es
    ftp_kwargs : Keyword arguments dict for write_batch_to_ftp
    """
    cached_df = batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    errors = []
    try:
        print(f"[Batch {batch_id}] Processing {cached_df.count()} records...")

        try:
            write_batch_to_es(cached_df, batch_id, **es_kwargs)
        except Exception as e:
            print(f"[ES]  FAILED on batch {batch_id}: {e}")
            errors.append(e)

        try:
            write_batch_to_ftp(cached_df, batch_id, **ftp_kwargs)
        except Exception as e:
            print(f"[FTP] FAILED on batch {batch_id}: {e}")
            errors.append(e)
    finally:
        cached_df.unpersist(blocking=False)

    if errors:
        raise RuntimeError(
            f"Batch {batch_id} had {len(errors)} sink failure(s): "
            + "; ".join(str(e) for e in errors)
        )
