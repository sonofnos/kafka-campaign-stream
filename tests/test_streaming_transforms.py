"""
Tests for PySpark Structured Streaming transform logic.

Runs with a local SparkSession — no Kafka or cluster needed.
We test the DataFrame transformation functions directly by feeding
them static DataFrames that mimic what the Kafka source produces.
"""
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, StringType, StructField, StructType,
                                TimestampType)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("test-kafka-stream")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


EVENT_SCHEMA = StructType([
    StructField("event_id",        StringType(), False),
    StructField("customer_id",     StringType(), False),
    StructField("campaign_id",     StringType(), False),
    StructField("event_type",      StringType(), False),
    StructField("channel",         StringType(), True),
    StructField("device",          StringType(), True),
    StructField("event_timestamp", TimestampType(), False),
    StructField("revenue",         DoubleType(), True),
    StructField("session_id",      StringType(), True),
    StructField("country",         StringType(), True),
])


def _make_events(spark, rows):
    return spark.createDataFrame(rows, EVENT_SCHEMA)


def _ts(s: str):
    from datetime import datetime
    return datetime.fromisoformat(s)


SAMPLE_ROWS = [
    ("e1", "C001", "CAMP01", "impression", "email",  "mobile",  _ts("2024-10-15T10:00:00"), 0.0,  "s1", "NG"),
    ("e2", "C001", "CAMP01", "click",      "email",  "mobile",  _ts("2024-10-15T10:01:00"), 0.0,  "s1", "NG"),
    ("e3", "C001", "CAMP01", "conversion", "email",  "mobile",  _ts("2024-10-15T10:02:00"), 99.0, "s1", "NG"),
    ("e4", "C002", "CAMP01", "impression", "sms",    "tablet",  _ts("2024-10-15T10:00:30"), 0.0,  "s2", "GH"),
    ("e5", "C002", "CAMP01", "click",      "sms",    "tablet",  _ts("2024-10-15T10:01:30"), 0.0,  "s2", "GH"),
    ("e6", "C003", "CAMP02", "impression", "push",   "desktop", _ts("2024-10-15T10:00:00"), 0.0,  "s3", "KE"),
    ("e7", "C003", "CAMP02", "conversion", "push",   "desktop", _ts("2024-10-15T10:03:00"), 50.0, "s3", "KE"),
]


class TestCampaignEventCounts:
    def test_aggregates_by_campaign_and_type(self, spark):
        from consumer.streaming_consumer import campaign_event_counts
        df = _make_events(spark, SAMPLE_ROWS).withWatermark("event_timestamp", "2 minutes")
        result = campaign_event_counts(df)
        camp01_clicks = result.filter(
            (F.col("campaign_id") == "CAMP01") & (F.col("event_type") == "click")
        )
        assert camp01_clicks.count() >= 1
        assert camp01_clicks.first()["event_count"] == 2

    def test_revenue_sums_conversions_only(self, spark):
        from consumer.streaming_consumer import campaign_event_counts
        df = _make_events(spark, SAMPLE_ROWS).withWatermark("event_timestamp", "2 minutes")
        result = campaign_event_counts(df)
        rev = result.filter(
            (F.col("campaign_id") == "CAMP01") & (F.col("event_type") == "conversion")
        ).agg(F.sum("total_revenue")).first()[0]
        assert rev == 99.0

    def test_has_required_columns(self, spark):
        from consumer.streaming_consumer import campaign_event_counts
        df = _make_events(spark, SAMPLE_ROWS).withWatermark("event_timestamp", "2 minutes")
        result = campaign_event_counts(df)
        for col in ["window_start", "window_end", "campaign_id", "event_type", "event_count"]:
            assert col in result.columns


class TestConversionAlerts:
    def _high_conversion_rows(self, spark):
        rows = [
            (f"e{i}", "C001", "CAMP01", "conversion", "email", "mobile",
             _ts("2024-10-15T10:00:00"), 50.0, f"s{i}", "NG")
            for i in range(15)
        ]
        return _make_events(spark, rows).withWatermark("event_timestamp", "2 minutes")

    def test_alert_raised_when_conversions_exceed_threshold(self, spark):
        from consumer.streaming_consumer import conversion_alerts
        df = self._high_conversion_rows(spark)
        result = conversion_alerts(df)
        assert result.count() >= 1
        row = result.first()
        assert row["alert_type"] == "HIGH_CONVERSION_RATE"
        assert row["conversion_count"] >= 10

    def test_no_alert_below_threshold(self, spark):
        from consumer.streaming_consumer import conversion_alerts
        df = _make_events(spark, SAMPLE_ROWS[:3]).withWatermark("event_timestamp", "2 minutes")
        result = conversion_alerts(df)
        assert result.count() == 0


class TestChannelPerformance:
    def test_ctr_in_valid_range(self, spark):
        from consumer.streaming_consumer import channel_performance
        df = _make_events(spark, SAMPLE_ROWS).withWatermark("event_timestamp", "2 minutes")
        result = channel_performance(df)
        ctrs = [r["ctr_pct"] for r in result.collect() if r["ctr_pct"] is not None]
        assert all(0.0 <= v <= 100.0 for v in ctrs)

    def test_groups_by_channel(self, spark):
        from consumer.streaming_consumer import channel_performance
        df = _make_events(spark, SAMPLE_ROWS).withWatermark("event_timestamp", "2 minutes")
        result = channel_performance(df)
        channels = {r["channel"] for r in result.collect()}
        assert "email" in channels
        assert "sms" in channels
