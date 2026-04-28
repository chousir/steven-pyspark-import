"""
writer.py
─────────────────────────────────────────────────────────────
Sink writer modules
  - create_es_index_template : Register ES index template (IP field types)
  - write_batch_to_es        : Write to Elasticsearch
  - write_batch_to_ftp       : Upload to FTP server (UUID-named files)
  - write_batch_to_all       : Write to both sinks synchronously
─────────────────────────────────────────────────────────────
"""

import base64
import io
import json
import ssl
import urllib.error
import urllib.request
import uuid
import ftplib

from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel


# ─────────────────────────────────────────────
# Elasticsearch Index Template
# ─────────────────────────────────────────────

#: Field mapping applied by create_es_index_template.
#: src_ip / dest_ip / bgp.next_hop → ip
#: bgp.nlri                        → ip_range  (array of CIDR strings)
_BGP_IP_MAPPINGS: dict = {
    "properties": {
        "src_ip":  {"type": "ip"},
        "dest_ip": {"type": "ip"},
        "bgp": {
            "type": "object",
            "properties": {
                "next_hop": {"type": "ip"},
                "nlri":     {"type": "ip_range"},
            },
        },
    }
}


def create_es_index_template(
    *,
    es_user:          str,
    es_password:      str,
    es_url:           str,
    es_port:          str,
    es_index:         str,
    template_name:    str = "bgp-template",
) -> None:
    """Register an Elasticsearch index template that forces IP field types.

    Must be called once before writing data so that ES dynamic mapping does
    not coerce src_ip / dest_ip / bgp.next_hop / bgp.nlri to text/keyword.

    Parameters
    ----------
    es_user        : Elasticsearch username
    es_password    : Elasticsearch password
    es_url         : Elasticsearch node URL (hostname or IP, no scheme)
    es_port        : Elasticsearch port
    es_index       : Index pattern the template applies to (e.g. ``bgp-*``)
    template_name  : Name of the composable index template in ES
    """
    template_body = {
        "index_patterns": [es_index],
        "template": {
            "mappings": _BGP_IP_MAPPINGS,
        },
    }

    url = f"https://{es_url}:{es_port}/_index_template/{template_name}"
    body = json.dumps(template_body).encode("utf-8")

    credentials = base64.b64encode(f"{es_user}:{es_password}".encode()).decode()
    req = urllib.request.Request(
        url,
        data=body,
        method="PUT",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {credentials}",
        },
    )

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    try:
        with urllib.request.urlopen(req, context=ssl_ctx, timeout=30) as resp:
            print(f"[ES] Index template '{template_name}' registered (HTTP {resp.status})")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(
            f"[ES] Failed to register index template '{template_name}': "
            f"HTTP {exc.code} {exc.reason}"
        ) from exc


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
        .option("es.spark.dataframe.write.null",     "false")
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

    Each Executor uploads its own partition directly to FTP without sending
    data back to the Driver, avoiding Driver OOM on large batches.
    Each partition produces one UUID-named .json file.

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

    # Capture into local variables so the closure serialises only the values,
    # not the entire enclosing frame.
    _hostname = ftp_hostname
    _user     = ftp_user
    _password = ftp_password
    _path     = ftp_remote_path

    def _upload_partition(rows) -> None:
        import ftplib
        import io
        import json
        import uuid as _uuid

        lines = [json.dumps(row.asDict(recursive=True)) for row in rows]
        if not lines:
            print("[FTP][partition] 0 rows, skipping upload")
            return
        content   = "\n".join(lines).encode("utf-8")
        filename  = f"{_uuid.uuid4()}.json"
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
    Failure in one sink does not prevent the other from executing.

    Parameters
    ----------
    batch_df   : The current micro-batch DataFrame
    batch_id   : Auto-incremented batch ID from Spark
    es_kwargs  : Keyword arguments dict for write_batch_to_es
    ftp_kwargs : Keyword arguments dict for write_batch_to_ftp
    """
    cached_df = batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        print(f"[Batch {batch_id}] Processing {cached_df.count()} records...")

        try:
            write_batch_to_es(cached_df, batch_id, **es_kwargs)
        except Exception as e:
            print(f"[ES]  FATAL on batch {batch_id}: {e}")

        try:
            write_batch_to_ftp(cached_df, batch_id, **ftp_kwargs)
        except Exception as e:
            print(f"[FTP] FATAL on batch {batch_id}: {e}")
    finally:
        cached_df.unpersist(blocking=False)
