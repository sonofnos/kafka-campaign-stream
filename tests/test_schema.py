"""Tests for the CampaignEvent schema and serialisation."""
import json
import pytest
from schemas.campaign_event import CampaignEvent, Channel, EventType, AVRO_SCHEMA


class TestCampaignEventSchema:
    def test_serialise_deserialise_roundtrip(self):
        event = CampaignEvent.new(
            customer_id="C00001",
            campaign_id="CAMP0001",
            event_type=EventType.CONVERSION,
            channel=Channel.EMAIL,
            revenue=99.0,
        )
        restored = CampaignEvent.from_json(event.to_json())
        assert restored.event_id == event.event_id
        assert restored.event_type == EventType.CONVERSION
        assert restored.revenue == 99.0
        assert restored.channel == Channel.EMAIL

    def test_to_json_produces_valid_json(self):
        event = CampaignEvent.new("C001", "CAMP001", EventType.CLICK, Channel.SMS)
        parsed = json.loads(event.to_json())
        assert parsed["event_type"] == "click"
        assert parsed["channel"] == "sms"

    def test_from_json_dict(self):
        d = {
            "event_id": "abc", "customer_id": "C001", "campaign_id": "CAMP001",
            "event_type": "impression", "channel": "push", "device": "mobile",
            "event_timestamp": "2024-10-01T12:00:00", "revenue": 0.0,
        }
        event = CampaignEvent.from_json(d)
        assert event.event_type == EventType.IMPRESSION

    def test_validation_passes_for_valid_event(self):
        event = CampaignEvent.new("C001", "CAMP001", EventType.CLICK, Channel.EMAIL)
        ok, msg = event.is_valid()
        assert ok
        assert msg == ""

    def test_validation_fails_for_negative_revenue(self):
        event = CampaignEvent.new("C001", "CAMP001", EventType.CLICK, Channel.EMAIL, revenue=-1.0)
        ok, msg = event.is_valid()
        assert not ok
        assert "negative revenue" in msg

    def test_validation_fails_for_missing_customer_id(self):
        event = CampaignEvent.new("", "CAMP001", EventType.CLICK, Channel.EMAIL)
        ok, msg = event.is_valid()
        assert not ok
        assert "customer_id" in msg

    def test_avro_schema_has_all_fields(self):
        field_names = {f["name"] for f in AVRO_SCHEMA["fields"]}
        required = {"event_id", "customer_id", "campaign_id", "event_type",
                    "channel", "device", "event_timestamp", "revenue"}
        assert required.issubset(field_names)

    def test_event_new_generates_unique_ids(self):
        ids = {CampaignEvent.new("C001", "CAMP001", EventType.CLICK, Channel.EMAIL).event_id
               for _ in range(100)}
        assert len(ids) == 100  # all unique
