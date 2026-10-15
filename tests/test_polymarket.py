"""Unit tests for Polymarket API client using respx HTTP mocking."""

import json
from datetime import datetime, timezone

import pytest
import respx
from httpx import Response

from src.clients.polymarket import PolymarketClient


@pytest.mark.asyncio
async def test_polymarket_fetch_open_markets():
    client = PolymarketClient(rate_limit_rps=100.0)

    mock_gamma_resp = [
        {
            "id": "540817",
            "question": "New Rihanna Album before GTA VI?",
            "description": "Resolves Yes if Rihanna releases an album before GTA VI.",
            "resolutionSource": "https://spotify.com",
            "startDate": "2025-05-02T15:48:10Z",
            "endDate": "2026-07-31T12:00:00Z",
            "active": True,
            "closed": False,
            "outcomes": json.dumps(["Yes", "No"]),
            "clobTokenIds": json.dumps(["token_yes_123", "token_no_456"]),
            "events": [{"id": "23784"}],
            "volume": 871416.9,
            "volume24hr": 580.98,
        }
    ]

    with respx.mock(base_url="https://gamma-api.polymarket.com") as respx_mock:
        respx_mock.get("/markets").mock(
            return_value=Response(200, json=mock_gamma_resp)
        )

        markets = await client.fetch_open_markets()
        assert len(markets) == 1
        assert markets[0].venue == "polymarket"
        assert markets[0].venue_market_id == "540817"
        assert markets[0].outcomes == ["Yes", "No"]
        assert markets[0].clob_token_ids == ["token_yes_123", "token_no_456"]


@pytest.mark.asyncio
async def test_polymarket_fetch_clob_snapshot():
    client = PolymarketClient(rate_limit_rps=100.0)

    mock_gamma_data = {
        "id": "540817",
        "question": "New Rihanna Album before GTA VI?",
        "outcomes": json.dumps(["Yes", "No"]),
        "clobTokenIds": json.dumps(["token_yes_123", "token_no_456"]),
        "volume": 871416.9,
        "volume24hr": 580.98,
    }

    mock_clob_yes = {
        "market": "0x1fad72...",
        "asset_id": "token_yes_123",
        "last_trade_price": "0.52",
        "bids": [{"price": "0.51", "size": "500.0"}],
        "asks": [{"price": "0.53", "size": "600.0"}],
    }
    mock_clob_no = {
        "market": "0x1fad72...",
        "asset_id": "token_no_456",
        "last_trade_price": "0.48",
        "bids": [{"price": "0.47", "size": "600.0"}],
        "asks": [{"price": "0.49", "size": "500.0"}],
    }

    from src.clients.base import NormalizedMarketDTO

    m = NormalizedMarketDTO(
        venue="polymarket",
        venue_market_id="540817",
        title="New Rihanna Album before GTA VI?",
        outcomes=["Yes", "No"],
        clob_token_ids=["token_yes_123", "token_no_456"],
    )
    setattr(m, "_gamma_data", mock_gamma_data)

    with respx.mock(base_url="https://clob.polymarket.com") as respx_mock:
        respx_mock.get("/book?token_id=token_yes_123").mock(
            return_value=Response(200, json=mock_clob_yes)
        )
        respx_mock.get("/book?token_id=token_no_456").mock(
            return_value=Response(200, json=mock_clob_no)
        )

        cycle_ts = datetime.now(timezone.utc)
        snaps = await client.fetch_snapshots_for_markets([m], cycle_ts)
        assert len(snaps) == 2

        yes_snap = next(s for s in snaps if s.outcome_label == "Yes")
        assert yes_snap.venue_market_id == "540817"
        assert yes_snap.bid == 0.51
        assert yes_snap.ask == 0.53
        assert yes_snap.mid == 0.52
