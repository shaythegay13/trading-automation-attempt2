"""Base data transfer objects and abstract client interface for venue collectors."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class NormalizedMarketDTO:
    """Normalized slowly-changing market metadata."""

    venue: str  # 'kalshi' | 'polymarket'
    venue_market_id: str
    title: str
    outcomes: List[str]  # e.g. ["Yes", "No"] or ["Spurs", "Grizzlies"]
    clob_token_ids: List[str]  # Parallel to outcomes
    event_id: Optional[str] = None
    series_id: Optional[str] = None
    subtitle: Optional[str] = None
    resolution_rules_text: Optional[str] = None
    resolution_source: Optional[str] = None
    open_time: Optional[datetime] = None
    close_time: Optional[datetime] = None
    expected_resolution_time: Optional[datetime] = None
    status: str = "active"
    price_level_structure: Optional[str] = None
    price_ranges: Optional[List[Dict[str, Any]]] = None
    raw_market: Optional[Dict[str, Any]] = None


@dataclass
class NormalizedSnapshotDTO:
    """Normalized snapshot payload per outcome."""

    venue_market_id: str
    outcome_index: int
    outcome_label: str
    captured_at: datetime
    bid: Optional[float] = None
    ask: Optional[float] = None
    mid: Optional[float] = None
    last_trade_price: Optional[float] = None
    volume_24h: Optional[float] = None
    volume_total: Optional[float] = None
    open_interest: Optional[float] = None
    liquidity_at_5c: Optional[float] = None
    liquidity_at_10c: Optional[float] = None
    spread: Optional[float] = None  # raw ask - bid; None if either side missing
    raw_orderbook: Optional[Dict[str, Any]] = None
    has_two_sided_book: bool = False
    depth_fetched: bool = False  # True when liquidity_at_Nc computed from a live orderbook call
    raw_market: Optional[Dict[str, Any]] = None


class BaseVenueClient(ABC):
    """Abstract base class for Prediction Market venue API clients."""

    @abstractmethod
    async def fetch_open_markets(self) -> List[NormalizedMarketDTO]:
        """Fetch all open/active markets from venue."""
        pass

    @abstractmethod
    async def fetch_snapshots_for_markets(
        self, markets: List[NormalizedMarketDTO], cycle_timestamp: datetime
    ) -> List[NormalizedSnapshotDTO]:
        """Fetch current orderbook/price snapshots for a list of markets."""
        pass
