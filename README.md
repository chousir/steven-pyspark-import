# PySpark Import

- Read messages from Kafka
- Process one micro-batch at configurable intervals
- Write to Sink (Elasticsearch, FTP) via `foreachBatch`

## Project Structure

```text
.
├── application.conf
├── README.md
├── requirements.txt
└── pyspark-import
    ├── main.py
    ├── parse_bgp.py
    └── writer.py
```

## Installation and Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Optional: override config path (defaults to application.conf)
export IMPORT_CONFIG=/path/to/application.conf

# Run streaming job
python pyspark-import/main.py
```

## Configuration (application.conf)

```hocon
kafka {
    input {
        brokers: "localhost:9092"
        topic: "bgp-evelog"
        maxOffsetsPerTrigger: 4000000
        startingOffsets: "latest"
    }
}
spark {
    checkpoint: "/path/to/checkpoint"
    triggerProcessingTime: "30 seconds"
}
elasticsearch {
    url: "https://localhost:9200"
    port: "9200"
    cert: "/path/to/cert.jks"
    user: "elastic"
    password: "password"
    index: "bgp-evelog"
    cert_password: "cert_password"
}
ftp {
    hostname: "ftp.example.com"
    user: "ftp_user"
    password: "ftp_password"
    paths: "/path/to/ftp/directory"
}
```

## Data Flow

1. Kafka value is cast to string JSON in pyspark-import/main.py
2. parse_bgp_updates in pyspark-import/parse_bgp.py parses and filters records
3. Only event_type=bgp and bgp.message_type=update are kept
4. Result is sent to selected sink writer in pyspark-import/writer.py

## BGP Parsing Strategy

Path attribute handling in pyspark-import/parse_bgp.py:

1. type=2 (AS_PATH): auto-detect ASN width (2-byte or 4-byte) and parse accordingly
2. type=17 (AS4_PATH): parse as fixed 4-byte ASN
3. Final as_path selection: if type=17 exists, it overrides type=2 result
4. type=3 (NEXT_HOP): parse IPv4 next_hop
5. NLRI: parse IPv4 prefixes from UPDATE tail

## Organization Lookup (AS Metadata)

ASN → Organization mappings from [ipverse/as-metadata](https://github.com/ipverse/as-metadata):

- As.csv file is read at stream startup and broadcast to all Spark workers
- Each as_path array is mapped to organization names in-place
- Unknown ASNs default to `AS{number}` format (e.g., `AS65000`)

Download as.csv:
```bash
curl -O https://raw.githubusercontent.com/ipverse/as-metadata/master/as.csv
```

## Sink Selection

Change sink_fn in pyspark-import/main.py:

1. Elasticsearch only: partial(write_batch_to_es, **ES_KWARGS)
2. FTP only: partial(write_batch_to_ftp, **FTP_KWARGS)
3. Both: partial(write_batch_to_all, es_kwargs=ES_KWARGS, ftp_kwargs=FTP_KWARGS)

## Example

Input (Suricata eve.json BGP update):

```json
{"timestamp":"2022-08-24T17:13:25.029636+0000","flow_id":939008963394962,"pcap_cnt":26,"event_type":"bgp","src_ip":"192.168.51.1","src_port":179,"dest_ip":"192.168.51.2","dest_port":54402,"proto":"TCP","ip_v":4,"pkt_src":"wire/pcap","vlan":[100],"bgp":{"message_type":"update","payload_length":42,"payload":"00000023400101005002000602010000fde8400304c0a8330180040400000000c00804007b01c8100a0a"}}
```

Output shape:

```json
{"timestamp":"2022-08-24T17:13:25.029636+0000","flow_id":939008963394962,"pcap_cnt":26,"event_type":"bgp","src_ip":"192.168.51.1","src_port":179,"dest_ip":"192.168.51.2","dest_port":54402,"proto":"TCP","ip_v":4,"pkt_src":"wire/pcap","vlan":[100],"bgp":{"message_type":"update","as_path":[0,1],"organization":["Internet Assigned Numbers Authority","Level 3 Parent LLC"],"next_hop":"192.168.51.1","nlri":["10.10.0.0/16"]}}
```

