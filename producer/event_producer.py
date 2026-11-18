"""
Campaign event producer.

Simulates a marketing platform emitting events to Kafka:
  - Sends a realistic mix of event types (impression → click → conversion funnel)
  - Partitions by campaign_id for ordering guarantees per campaign
  - Handles delivery failures with retry + dead-letter fallback
  - Configurable rate and burst mode for load testing

Usage:
    python -m producer.event_producer                   # continuous stream
    python -m producer.event_producer --count 1000      # fixed batch
    python -m producer.event_producer --burst           # 10x rate spike
"""
import argparse
import logging
import random
import signal
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schemas.campaign_event import (
    KAFKA_TOPIC_DLQ, KAFKA_TOPIC_EVENTS,
    CampaignEvent, Channel, EventType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [producer] %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"

CUSTOMERS = [f"C{i:05d}" for i in range(1, 201)]
CAMPAIGNS = [f"CAMP{i:04d}" for i in range(1, 21)]
CHANNELS = list(Channel)
COUNTRIES = ["NG", "GH", "KE", "ZA", "EG"]
DEVICES = ["mobile", "desktop", "tablet"]

# Realistic funnel weights: impressions dominate, conversions are rare
FUNNEL_WEIGHTS = {
    EventType.IMPRESSION: 50,
    EventType.OPEN: 20,
    EventType.CLICK: 20,
    EventType.CONVERSION: 5,
    EventType.UNSUBSCRIBE: 5,
}
_EVENT_POPULATION = [e for e, w in FUNNEL_WEIGHTS.items() for _ in range(w)]


def _make_event() -> CampaignEvent:
    event_type = random.choice(_EVENT_POPULATION)
    revenue = round(random.uniform(10, 300), 2) if event_type == EventType.CONVERSION else 0.0
    return CampaignEvent.new(
        customer_id=random.choice(CUSTOMERS),
        campaign_id=random.choice(CAMPAIGNS),
        event_type=event_type,
        channel=random.choice(CHANNELS),
        device=random.choice(DEVICES),
        revenue=revenue,
        country=random.choice(COUNTRIES),
    )


def _build_producer(retries: int = 3) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: v.encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",          # wait for all in-sync replicas
                retries=5,
                max_in_flight_requests_per_connection=1,  # preserve ordering per partition
                compression_type="snappy",
            )
            log.info("Connected to Kafka at %s", BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            if attempt < retries:
                log.warning("Broker unavailable, retry %d/%d in 3s", attempt, retries)
                time.sleep(3)
            else:
                raise


def _on_error(exc: Exception, event: CampaignEvent, dlq_producer: KafkaProducer | None) -> None:
    log.error("Delivery failed for %s: %s", event.event_id, exc)
    if dlq_producer:
        try:
            dlq_producer.send(KAFKA_TOPIC_DLQ, key=event.event_id, value=event.to_json())
        except Exception as e:
            log.error("DLQ send also failed: %s", e)


def run(count: int | None = None, events_per_second: float = 10.0, burst: bool = False) -> None:
    producer = _build_producer()
    sent = 0
    interval = 1.0 / (events_per_second * (10 if burst else 1))

    running = True
    def _stop(sig, _frame):
        nonlocal running
        running = False
        log.info("Shutting down producer (sent %d events)", sent)

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    log.info("Producing events at %.0f/s%s", events_per_second * (10 if burst else 1),
             " (BURST)" if burst else "")

    while running and (count is None or sent < count):
        event = _make_event()
        producer.send(
            KAFKA_TOPIC_EVENTS,
            key=event.campaign_id,     # partition by campaign for ordering
            value=event.to_json(),
        ).add_errback(lambda exc, e=event: _on_error(exc, e, None))
        sent += 1

        if sent % 500 == 0:
            producer.flush()
            log.info("Sent %d events", sent)

        time.sleep(interval)

    producer.flush()
    producer.close()
    log.info("Producer finished. Total sent: %d", sent)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=None, help="Number of events (default: unlimited)")
    parser.add_argument("--rate", type=float, default=10.0, help="Events per second (default: 10)")
    parser.add_argument("--burst", action="store_true", help="10x rate spike")
    args = parser.parse_args()
    run(count=args.count, events_per_second=args.rate, burst=args.burst)
