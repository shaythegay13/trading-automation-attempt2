"""Unit tests for database models."""

import uuid
from datetime import datetime, timezone

from src.db.models import Market, MarketSnapshot


def test_market_instantiation():
    m = Market(
        id=uuid.uuid4(),
        venue="kalshi",
        venue_market_id="TICKER-123",
        event_id="EVT-1",
        series_id="SERIES-1",
        title="Will Event X happen?",
        subtitle="Yes/No",
        outcomes=["Yes", "No"],
        clob_token_ids=["TICKER-123_YES", "TICKER-123_NO"],
        resolution_rules_text="Full text rules",
        resolution_source="https://example.com",
        status="active",
    )
    assert m.venue == "kalshi"
    assert m.venue_market_id == "TICKER-123"
    assert m.outcomes == ["Yes", "No"]
    assert m.status == "active"


def test_snapshot_instantiation():
    market_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    snap = MarketSnapshot(
        id=uuid.uuid4(),
        market_id=market_id,
        outcome_index=0,
        outcome_label="Yes",
        captured_at=now,
        bid=0.45,
        ask=0.48,
        mid=0.465,
        last_trade_price=0.46,
        volume_24h=10000.0,
        volume_total=50000.0,
        open_interest=25000.0,
        liquidity_at_5c=1200.0,
        liquidity_at_10c=3500.0,
        raw_orderbook={"bids": [], "asks": []},
    )
    assert snap.market_id == market_id
    assert snap.outcome_label == "Yes"
    assert snap.bid == 0.45
    assert snap.ask == 0.48
    assert snap.mid == 0.465
