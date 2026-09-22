# PySpark Import (Simple)

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
    └── writer.py
```

## Installation & Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Optional: override config path (defaults to application.conf)
export IMPORT_CONFIG=/path/to/application.conf

# Run the application
python pyspark-import/main.py
```

## Configuration (`application.conf`)

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

## Sink Selection

You can switch between different sink modes in `main.py`:

- **Elasticsearch only**: `sink_fn = partial(write_batch_to_es, **ES_KWARGS)`
- **FTP only**: `sink_fn = partial(write_batch_to_ftp, **FTP_KWARGS)`
- **Both (ES + FTP)**: `sink_fn = partial(write_batch_to_all, es_kwargs=ES_KWARGS, ftp_kwargs=FTP_KWARGS)`

## Docker Image

This branch's `Dockerfile` builds the shared **base image** — Spark plus
the Kafka and Elasticsearch connector jars, resolved once at build time.
It contains no pipeline code or config. Other branches (e.g. `cento`,
`bgp`) build their own app image FROM this one via `--build-arg
BASE_IMAGE=...`, so this image has to exist locally (or be pulled) before
those branches can be built.

```bash
make build   # produces steven-pyspark-import:20260410-base
```

The tag is pinned, not date-generated, because downstream Dockerfiles
reference it by name. Only bump it (and update every downstream branch's
`Makefile` to match) when the Spark version or connector versions here
actually change — rebuilding this image with the same tag would silently
change what every app branch builds on.

If there's no shared registry to `docker push`/`pull` this image through,
export it instead and load it on the machine that needs to build `cento`
or `bgp`:

```bash
make save                                      # writes steven-pyspark-import-20260410-base.tar.gz
docker load -i steven-pyspark-import-20260410-base.tar.gz   # on the other machine
```
