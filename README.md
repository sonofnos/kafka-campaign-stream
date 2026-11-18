# kafka-campaign-stream

Real-time marketing event pipeline using **Apache Kafka** + **PySpark Structured Streaming**. Consumes a high-throughput campaign event stream, applies windowed aggregations, raises conversion alerts, and writes live metrics to PostgreSQL.

> PySpark Structured Streaming is the same engine Azure Databricks uses for real-time pipelines. The only difference in production is the checkpoint path (ADLS Gen2 instead of local disk) and the Kafka bootstrap server.

---

## Architecture

```
┌─────────────────────────┐
│   Campaign Platform     │
│   (producer)            │
│                         │
│  impression → click →   │
│  open → conversion      │
└──────────┬──────────────┘
           │  kafka-python producer
           │  key=campaign_id (ordered per campaign)
           │  compression=snappy, acks=all
           ▼
┌─────────────────────────┐
│  Kafka Topic            │
│  campaign.events        │
│  (3 partitions)         │
│                         │
│  campaign.conversions   │  ← filtered high-value stream
│  campaign.events.dlq    │  ← dead-letter queue
└──────────┬──────────────┘
           │  Spark readStream (Kafka connector)
           │  watermark: 2 minutes (late data tolerance)
           ▼
┌─────────────────────────────────────────────────────────┐
│  PySpark Structured Streaming (Databricks-compatible)   │
│                                                         │
│  Stream 1 — 5-min tumbling window                       │
│    campaign_event_counts: impressions/clicks/conversions│
│    per campaign, per window                             │
│                                                         │
│  Stream 2 — 5-min tumbling window                       │
│    conversion_alerts: campaigns with ≥10 conversions    │
│    triggers alerting webhook in production              │
│                                                         │
│  Stream 3 — 10-min sliding window (5-min slide)         │
│    channel_performance: CTR + CVR per channel           │
└──────────┬──────────────────────────────────────────────┘
           │  foreachBatch → JDBC (PostgreSQL)
           ▼
┌─────────────────────────┐
│  PostgreSQL (sink)       │
│                         │
│  stream_campaign_counts  │
│  stream_conversion_alerts│
│  stream_channel_perf.    │
│                         │
│  3 live reporting views  │
└─────────────────────────┘
```

---

## Skills demonstrated

| Area | Implementation |
|------|----------------|
| **Apache Kafka** | Producer with `acks=all`, snappy compression, partition-by-key ordering, DLQ pattern, graceful shutdown |
| **PySpark Structured Streaming** | `readStream` from Kafka, event-time watermarking, tumbling and sliding windows, `foreachBatch` JDBC sink |
| **Windowed aggregations** | 5-min tumbling (counts), 10-min sliding with 5-min slide (channel CTR/CVR) |
| **Late data handling** | Watermark of 2 minutes handles out-of-order events correctly |
| **Conversion alerting** | Stateful aggregation detects campaigns with ≥10 conversions per window |
| **Schema definition** | Dataclass schema + Avro schema for Schema Registry integration |
| **Kafka patterns** | DLQ, partition ordering, `max_in_flight_requests=1`, offset management |
| **Testing** | Streaming transforms tested against static DataFrames — no Kafka cluster needed for unit tests |
| **PostgreSQL** | Streaming sink tables with indexes, 3 live reporting views |

---

## Quick start

```bash
# 1. Start Kafka + PostgreSQL
docker compose up -d
# Kafka UI at http://localhost:8080

# 2. Install dependencies
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Produce events (1000 events, then stop)
python -m producer.event_producer --count 1000

# 4. Run the streaming consumer (separate terminal)
python -m consumer.streaming_consumer

# 5. Run tests (no Kafka needed)
pytest tests/ -v
```

---

## Kafka topics

| Topic | Purpose |
|-------|---------|
| `campaign.events` | All campaign events (all types) |
| `campaign.conversions` | High-value events only (filtered) |
| `campaign.events.dlq` | Failed events for reprocessing |

---

## Production (Azure Databricks)

Change two config values in `streaming_consumer.py`:

```python
# 1. Kafka: use Azure Event Hubs compatible endpoint
KAFKA_BOOTSTRAP = "<event-hubs-namespace>.servicebus.windows.net:9093"

# 2. Checkpoints: use ADLS Gen2 instead of local disk
CHECKPOINT_BASE = "abfss://checkpoints@<storage>.dfs.core.windows.net/kafka-stream"
```

The PySpark code is identical to what runs in Databricks — no other changes needed.

---

## Project structure

```
kafka-campaign-stream/
├── producer/
│   └── event_producer.py    # Kafka producer with retry, DLQ, rate control
├── consumer/
│   └── streaming_consumer.py # PySpark Structured Streaming (3 aggregation streams)
├── schemas/
│   └── campaign_event.py    # Event dataclass + Avro schema
├── sql/
│   └── schema.sql           # PostgreSQL sink tables + 3 reporting views
├── tests/
│   ├── test_schema.py        # Schema serialisation tests (no Spark needed)
│   └── test_streaming_transforms.py  # Streaming transform tests (local Spark)
├── docker-compose.yml        # Kafka + Zookeeper + PostgreSQL + Kafka UI
└── requirements.txt
```
