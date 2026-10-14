"""Unit tests for Kalshi API client using respx HTTP mocking."""

from datetime import datetime, timezone

import pytest
import respx
from httpx import Response

from src.clients.kalshi import KalshiClient


@pytest.mark.asyncio
async def test_kalshi_fetch_open_markets():
    client = KalshiClient(rate_limit_rps=100.0)

    mock_resp_page1 = {
        "cursor": "",
        "markets": [
            {
                "ticker": "K-TEST-1",
                "event_ticker": "EVT-1",
                "title": "Will SpaceX land on Mars?",
                "yes_sub_title": "Yes",
                "rules_primary": "Official announcement counts.",
                "open_time": "2026-01-01T00:00:00Z",
                "close_time": "2026-12-31T23:59:59Z",
                "status": "active",
            }
        ],
    }

    with respx.mock(base_url="https://api.elections.kalshi.com/trade-api/v2") as respx_mock:
        respx_mock.get("/markets").mock(
            return_value=Response(200, json=mock_resp_page1)
        )

        markets = await client.fetch_open_markets()
        assert len(markets) == 1
        assert markets[0].venue == "kalshi"
        assert markets[0].venue_market_id == "K-TEST-1"
        assert markets[0].outcomes == ["Yes", "No"]


@pytest.mark.asyncio
async def test_kalshi_fetch_orderbook_snapshot():
    client = KalshiClient(rate_limit_rps=100.0)

    mock_ob = {
        "orderbook_fp": {
            "yes_dollars": [["0.45", "100.0"]],
            "no_dollars": [["0.50", "200.0"]],  # no bid 0.50 => yes ask 0.50
        }
    }

    with respx.mock(base_url="https://api.elections.kalshi.com/trade-api/v2") as respx_mock:
        respx_mock.get("/markets/K-TEST-1/orderbook").mock(
            return_value=Response(200, json=mock_ob)
        )

        from src.clients.base import NormalizedMarketDTO

        m = NormalizedMarketDTO(
            venue="kalshi",
            venue_market_id="K-TEST-1",
            title="Test Kalshi Market",
            outcomes=["Yes", "No"],
            clob_token_ids=["K-TEST-1_YES", "K-TEST-1_NO"],
        )

        cycle_ts = datetime.now(timezone.utc)
        snaps = await client.fetch_snapshots_for_markets([m], cycle_ts)
        assert len(snaps) == 2

        yes_snap = next(s for s in snaps if s.outcome_label == "Yes")
        assert yes_snap.venue_market_id == "K-TEST-1"
        assert yes_snap.bid == 0.45
        assert yes_snap.ask == 0.50
        assert yes_snap.mid == 0.475
        # spread = 0.50 - 0.45 = 0.05, within default 0.10 cap
        assert yes_snap.spread == 0.05
        assert yes_snap.has_two_sided_book is True


@pytest.mark.asyncio
async def test_kalshi_wide_spread_rejected_as_two_sided():
    """A market with spread > max_spread must NOT be flagged has_two_sided_book=True."""
    # Default cap is 0.10; bid=0.02, ask=0.75 => spread=0.73 => rejected
    client = KalshiClient(rate_limit_rps=100.0, max_spread=0.10)

    mock_ob = {
        "orderbook_fp": {
            "yes_dollars": [["0.0200", "350.0"]],  # yes bid 0.02
            "no_dollars": [["0.2500", "10.0"]],    # no bid 0.25 => yes ask 0.75
        }
    }

    with respx.mock(base_url="https://api.elections.kalshi.com/trade-api/v2") as respx_mock:
        respx_mock.get("/markets/K-WIDE-1/orderbook").mock(
            return_value=Response(200, json=mock_ob)
        )

        from src.clients.base import NormalizedMarketDTO

        m = NormalizedMarketDTO(
            venue="kalshi",
            venue_market_id="K-WIDE-1",
            title="Wide Spread Market",
            outcomes=["Yes", "No"],
            clob_token_ids=["K-WIDE-1_YES", "K-WIDE-1_NO"],
        )

        cycle_ts = datetime.now(timezone.utc)
        snaps = await client.fetch_snapshots_for_markets([m], cycle_ts)
        yes_snap = next(s for s in snaps if s.outcome_label == "Yes")

        assert yes_snap.bid == 0.02
        assert yes_snap.ask == 0.75
        assert yes_snap.spread == 0.73
        assert yes_snap.has_two_sided_book is False  # spread 0.73 > cap 0.10475


@pytest.mark.asyncio
async def test_kalshi_fetch_events_with_snapshots_nested():
    """Test nested-event parsing, including an event with multiple markets."""
    client = KalshiClient(rate_limit_rps=100.0, max_spread=0.10)

    mock_events_resp = {
        "events": [
            {
                "event_ticker": "EVT-NESTED-1",
                "title": "Nested Event Title",
                "markets": [
                    {
                        "ticker": "K-NEST-A",
                        "title": "Nested Market A",
                        "status": "open",
                        "yes_bid_dollars": "0.4500",
                        "yes_ask_dollars": "0.4800",
                        "no_bid_dollars": "0.5200",
                        "no_ask_dollars": "0.5500",
                        "volume_fp": "12000.50",
                    },
                    {
                        "ticker": "K-NEST-B",
                        "title": "Nested Market B",
                        "status": "open",
                        "yes_bid_dollars": "0.1000",
                        "yes_ask_dollars": "0.3000",  # spread = 0.20 > 0.10 max_spread
                        "no_bid_dollars": "0.7000",
                        "no_ask_dollars": "0.9000",
                        "volume_fp": "150.00",
                    }
                ]
            }
        ],
        "cursor": ""
    }

    with respx.mock(base_url="https://api.elections.kalshi.com/trade-api/v2") as respx_mock:
        respx_mock.get("/events").mock(
            return_value=Response(200, json=mock_events_resp)
        )

        cycle_ts = datetime.now(timezone.utc)
        markets, snapshots = await client.fetch_events_with_snapshots(cycle_ts)

        assert len(markets) == 2
        assert len(snapshots) == 4  # 2 outcomes (Yes/No) per market

        # Market A check
        m_a = next(m for m in markets if m.venue_market_id == "K-NEST-A")
        assert m_a.title == "Nested Market A"
        assert m_a.event_id == "EVT-NESTED-1"

        snap_a_yes = next(s for s in snapshots if s.venue_market_id == "K-NEST-A" and s.outcome_label == "Yes")
        assert snap_a_yes.bid == 0.45
        assert snap_a_yes.ask == 0.48
        assert snap_a_yes.spread == 0.03
        assert snap_a_yes.has_two_sided_book is True
        assert snap_a_yes.depth_fetched is False  # Tier-1 does not fetch depth

        # Market B check
        snap_b_yes = next(s for s in snapshots if s.venue_market_id == "K-NEST-B" and s.outcome_label == "Yes")
        assert snap_b_yes.bid == 0.10
        assert snap_b_yes.ask == 0.30
        assert snap_b_yes.spread == 0.20
        assert snap_b_yes.has_two_sided_book is False  # rejected by spread cap (0.20 > 0.10)
        assert snap_b_yes.depth_fetched is False


@pytest.mark.asyncio
async def test_kalshi_cursor_kill_switch():
    """Test cursor exhaustion limit / kill switch in KalshiClient."""
    # Set max_pages to 2
    client = KalshiClient(rate_limit_rps=100.0, max_pages=2)

    # Mock response that always has a next cursor page
    def mock_handler(request):
        return Response(200, json={
            "events": [{
                "event_ticker": "EV-PAGED",
                "markets": []
            }],
            "cursor": "next-page-token"
        })

    with respx.mock(base_url="https://api.elections.kalshi.com/trade-api/v2") as respx_mock:
        respx_mock.get("/events").mock(side_effect=mock_handler)

        cycle_ts = datetime.now(timezone.utc)
        markets, snapshots = await client.fetch_events_with_snapshots(cycle_ts)

        # The cursor page count starts at 1. Since max_pages=2:
        # Page 1: processed (cursor='next-page-token')
        # Page 2: processed (page becomes 2 > max_pages = False? wait, logic is:
        # page += 1
        # if page > self.max_pages: break)
        # So page 3 breaks.
        # We should have fetched events twice. Let's verify the mock count.
        assert len(respx_mock.calls) == 2


