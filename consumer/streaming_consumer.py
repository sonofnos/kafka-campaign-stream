"""
PySpark Structured Streaming consumer for campaign events.

Reads from Kafka topic `campaign.events`, applies transformations,
and writes windowed aggregates to PostgreSQL every minute.

This is the same PySpark API used in Azure Databricks Structured Streaming jobs.
The only change needed for Databricks is the checkpoint location (ADLS path)
and the Kafka bootstrap servers.

Aggregations produced (written to PostgreSQL):
  - campaign_event_counts: event counts per campaign per 5-minute window
  - conversion_alerts:     campaigns exceeding 10 conversions in 5 minutes
  - channel_performance:   CTR + CVR by channel (tumbling 10-minute window)
"""
import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, StringType, StructField, StructType,
                                TimestampType)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "campaign.events"
CHECKPOINT_BASE = "/tmp/kafka-campaign-stream/checkpoints"
JDBC_URL = os.getenv("JDBC_URL", "jdbc:postgresql://localhost:5433/campaign_stream")
JDBC_PROPS = {
    "user": os.getenv("POSTGRES_USER", "streamuser"),
    "password": os.getenv("POSTGRES_PASSWORD", "streampass"),
    "driver": "org.postgresql.Driver",
}

EVENT_SCHEMA = StructType([
    StructField("event_id",         StringType(),    False),
    StructField("customer_id",      StringType(),    False),
    StructField("campaign_id",      StringType(),    False),
    StructField("event_type",       StringType(),    False),
    StructField("channel",          StringType(),    True),
    StructField("device",           StringType(),    True),
    StructField("event_timestamp",  StringType(),    False),
    StructField("revenue",          DoubleType(),    True),
    StructField("session_id",       StringType(),    True),
    StructField("country",          StringType(),    True),
])


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CampaignEventConsumer")
        # Kafka connector (spark-sql-kafka) must be on the classpath
        # In Databricks: add com.datastax.spark:spark-sql-kafka as a Maven dependency
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "org.postgresql:postgresql:42.7.4")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10000)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw_df):
    return (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_ts")
        .select(
            F.from_json("json_value", EVENT_SCHEMA).alias("e"),
            F.col("kafka_ts"),
        )
        .select("e.*", "kafka_ts")
        .withColumn(
            "event_timestamp",
            F.to_timestamp("event_timestamp"),
        )
        # Use event_timestamp as the watermark anchor (event time, not processing time)
        .withWatermark("event_timestamp", "2 minutes")
    )


def campaign_event_counts(parsed_df):
    """
    5-minute tumbling window: count event types per campaign.
    Useful for real-time campaign pacing dashboards.
    """
    return (
        parsed_df
        .groupBy(
            F.window("event_timestamp", "5 minutes"),
            "campaign_id",
            "event_type",
        )
        .agg(
            F.count("*").alias("event_count"),
            F.sum("revenue").alias("total_revenue"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "campaign_id",
            "event_type",
            "event_count",
            "total_revenue",
            "unique_customers",
        )
    )


def conversion_alerts(parsed_df):
    """
    Raise an alert row when any campaign gets >= 10 conversions in a 5-min window.
    Would fan out to a webhook / Slack in production.
    """
    return (
        parsed_df
        .filter(F.col("event_type") == "conversion")
        .groupBy(
            F.window("event_timestamp", "5 minutes"),
            "campaign_id",
        )
        .agg(
            F.count("*").alias("conversion_count"),
            F.sum("revenue").alias("total_revenue"),
        )
        .filter(F.col("conversion_count") >= 10)
        .select(
            F.col("window.start").alias("window_start"),
            "campaign_id",
            "conversion_count",
            "total_revenue",
            F.lit("HIGH_CONVERSION_RATE").alias("alert_type"),
            F.current_timestamp().alias("alerted_at"),
        )
    )


def channel_performance(parsed_df):
    """
    10-minute sliding window (slide every 5 min): CTR and CVR per channel.
    """
    return (
        parsed_df
        .groupBy(
            F.window("event_timestamp", "10 minutes", "5 minutes"),
            "channel",
        )
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "impression", 1).otherwise(0)).alias("impressions"),
            F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("clicks"),
            F.sum(F.when(F.col("event_type") == "conversion", 1).otherwise(0)).alias("conversions"),
        )
        .withColumn(
            "ctr_pct",
            F.round(F.col("clicks") / F.nullif(F.col("impressions"), 0) * 100, 2),
        )
        .withColumn(
            "cvr_pct",
            F.round(F.col("conversions") / F.nullif(F.col("clicks"), 0) * 100, 2),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "channel", "total_events", "impressions", "clicks", "conversions",
            "ctr_pct", "cvr_pct",
        )
    )


def _write_to_postgres(df, epoch_id: int, table: str) -> None:
    """foreachBatch sink: write micro-batch to PostgreSQL."""
    if df.isEmpty():
        return
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        .option("driver", JDBC_PROPS["driver"])
        .mode("append")
        .save()
    )
    log.info("[epoch %d] wrote %d rows to %s", epoch_id, df.count(), table)


def run() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_kafka_stream(spark)
    parsed = parse_events(raw)

    # Stream 1: campaign event counts → PostgreSQL
    q1 = (
        campaign_event_counts(parsed)
        .writeStream
        .outputMode("update")
        .foreachBatch(lambda df, eid: _write_to_postgres(df, eid, "stream_campaign_counts"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/campaign_counts")
        .trigger(processingTime="1 minute")
        .start()
    )

    # Stream 2: conversion alerts → PostgreSQL
    q2 = (
        conversion_alerts(parsed)
        .writeStream
        .outputMode("update")
        .foreachBatch(lambda df, eid: _write_to_postgres(df, eid, "stream_conversion_alerts"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/conversion_alerts")
        .trigger(processingTime="1 minute")
        .start()
    )

    # Stream 3: channel performance → console (dev) / PostgreSQL (prod)
    q3 = (
        channel_performance(parsed)
        .writeStream
        .outputMode("update")
        .foreachBatch(lambda df, eid: _write_to_postgres(df, eid, "stream_channel_performance"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/channel_performance")
        .trigger(processingTime="2 minutes")
        .start()
    )

    log.info("All 3 streaming queries started. Waiting for termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
