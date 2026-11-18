"""Campaign event schema — shared between producer and consumer."""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional
import json
import uuid


class EventType(str, Enum):
    IMPRESSION = "impression"
    CLICK = "click"
    OPEN = "open"
    CONVERSION = "conversion"
    UNSUBSCRIBE = "unsubscribe"


class Channel(str, Enum):
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"
    SOCIAL = "social"
    PAID_SEARCH = "paid_search"


KAFKA_TOPIC_EVENTS = "campaign.events"
KAFKA_TOPIC_CONVERSIONS = "campaign.conversions"  # filtered high-value stream
KAFKA_TOPIC_DLQ = "campaign.events.dlq"           # dead-letter queue


@dataclass
class CampaignEvent:
    event_id: str
    customer_id: str
    campaign_id: str
    event_type: EventType
    channel: Channel
    device: str
    event_timestamp: str           # ISO 8601
    revenue: float = 0.0
    session_id: Optional[str] = None
    country: Optional[str] = None

    @classmethod
    def new(
        cls,
        customer_id: str,
        campaign_id: str,
        event_type: EventType,
        channel: Channel,
        device: str = "mobile",
        revenue: float = 0.0,
        country: str = "NG",
    ) -> "CampaignEvent":
        return cls(
            event_id=str(uuid.uuid4()),
            customer_id=customer_id,
            campaign_id=campaign_id,
            event_type=event_type,
            channel=channel,
            device=device,
            event_timestamp=datetime.utcnow().isoformat(),
            revenue=revenue,
            session_id=str(uuid.uuid4()),
            country=country,
        )

    def to_json(self) -> str:
        d = {
            "event_id": self.event_id,
            "customer_id": self.customer_id,
            "campaign_id": self.campaign_id,
            "event_type": self.event_type.value,
            "channel": self.channel.value,
            "device": self.device,
            "event_timestamp": self.event_timestamp,
            "revenue": self.revenue,
            "session_id": self.session_id,
            "country": self.country,
        }
        return json.dumps(d)

    @classmethod
    def from_json(cls, data: str | bytes | dict) -> "CampaignEvent":
        d = json.loads(data) if not isinstance(data, dict) else data
        return cls(
            event_id=d["event_id"],
            customer_id=d["customer_id"],
            campaign_id=d["campaign_id"],
            event_type=EventType(d["event_type"]),
            channel=Channel(d["channel"]),
            device=d.get("device", "unknown"),
            event_timestamp=d["event_timestamp"],
            revenue=float(d.get("revenue", 0.0)),
            session_id=d.get("session_id"),
            country=d.get("country"),
        )

    def is_valid(self) -> tuple[bool, str]:
        if not self.event_id:
            return False, "missing event_id"
        if not self.customer_id:
            return False, "missing customer_id"
        if not self.campaign_id:
            return False, "missing campaign_id"
        if self.revenue < 0:
            return False, f"negative revenue: {self.revenue}"
        return True, ""


# Avro schema (for Schema Registry / Confluent Cloud)
AVRO_SCHEMA = {
    "type": "record",
    "name": "CampaignEvent",
    "namespace": "com.marketingplatform.events",
    "fields": [
        {"name": "event_id",        "type": "string"},
        {"name": "customer_id",     "type": "string"},
        {"name": "campaign_id",     "type": "string"},
        {"name": "event_type",      "type": {"type": "enum", "name": "EventType",
                                             "symbols": ["impression", "click", "open", "conversion", "unsubscribe"]}},
        {"name": "channel",         "type": {"type": "enum", "name": "Channel",
                                             "symbols": ["email", "sms", "push", "social", "paid_search"]}},
        {"name": "device",          "type": "string"},
        {"name": "event_timestamp", "type": "string"},
        {"name": "revenue",         "type": "double", "default": 0.0},
        {"name": "session_id",      "type": ["null", "string"], "default": None},
        {"name": "country",         "type": ["null", "string"], "default": None},
    ],
}
