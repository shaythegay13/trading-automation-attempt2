"""Unit tests for the snapshot runner pipeline with failure isolation."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from src.clients.base import BaseVenueClient, NormalizedMarketDTO, NormalizedSnapshotDTO
from src.runner import enforce_retention_policy, execute_snapshot_cycle, run_venue_pipeline


class DummySuccessClient(BaseVenueClient):
    async def fetch_open_markets(self):
        return [
            NormalizedMarketDTO(
                venue="test_venue",
                venue_market_id="M1",
                title="Test Market 1",
                outcomes=["Yes", "No"],
                clob_token_ids=["T1", "T2"],
            )
        ]

    async def fetch_snapshots_for_markets(self, markets, cycle_timestamp):
        return [
            NormalizedSnapshotDTO(
                venue_market_id="M1",
                outcome_index=0,
                outcome_label="Yes",
                captured_at=cycle_timestamp,
                bid=0.50,
                ask=0.52,
                mid=0.51,
            )
        ]


class DummyFailingClient(BaseVenueClient):
    async def fetch_open_markets(self):
        raise RuntimeError("Venue API 500 Internal Server Error")

    async def fetch_snapshots_for_markets(self, markets, cycle_timestamp):
        return []


@pytest.mark.asyncio
async def test_venue_pipeline_isolation():
    failing_client = DummyFailingClient()
    cycle_ts = datetime.now(timezone.utc)

    # Pipeline should catch error and return error tuple without raising
    markets_count, snaps_count, err = await run_venue_pipeline(
        failing_client, "failing_venue", cycle_ts
    )
    assert markets_count == 0
    assert snaps_count == 0
    assert isinstance(err, RuntimeError)
    assert "500 Internal Server Error" in str(err)


@pytest.mark.asyncio
async def test_execute_snapshot_cycle_isolation(monkeypatch):
    mock_k_client = DummyFailingClient()
    mock_p_client = DummySuccessClient()

    with patch("src.runner.KalshiClient", return_value=mock_k_client), \
         patch("src.runner.PolymarketClient", return_value=mock_p_client), \
         patch("src.runner.get_db_session") as mock_db, \
         patch("src.runner.verify_current_month_partition_exists", return_value=None):

        session_mock = AsyncMock()
        mock_db.return_value.__aenter__.return_value = session_mock

        with patch("src.runner.sync_markets_to_db", return_value=({"M1": AsyncMock(id="uuid-1")}, {})), \
             patch("src.runner.save_snapshots_to_db", return_value=1):

            await execute_snapshot_cycle()


@pytest.mark.asyncio
async def test_enforce_retention_policy_disabled_is_noop():
    """retention_months <= 0 must not touch the session at all."""
    session_mock = AsyncMock()
    dropped = await enforce_retention_policy(session_mock, retention_months=0)
    assert dropped == []
    session_mock.execute.assert_not_called()
