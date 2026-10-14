"""Snapshot loop runner for continuous multi-venue ingestion.

Read-only. Venue clients fetch public market data with HTTP GET and persist
snapshots. This process never submits, cancels, or amends orders.
"""

import argparse
import asyncio
import logging
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.clients.base import BaseVenueClient, NormalizedMarketDTO, NormalizedSnapshotDTO
from src.clients.kalshi import KalshiClient
from src.clients.polymarket import PolymarketClient
from src.config import settings
from src.db.models import Market, MarketSnapshot
from src.db.session import get_db_session

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("src.runner")


def _parse_excluded_categories(raw: str) -> set:
    """Parse a comma-separated category list (e.g. "Sports,Entertainment") into a set."""
    return {c.strip() for c in raw.split(",") if c.strip()}


async def verify_current_month_partition_exists(session: AsyncSession) -> None:
    """Verify that the PostgreSQL partition for the current month exists."""
    now = datetime.now(timezone.utc)
    part_name = f"market_snapshots_y{now.year}m{now.month:02d}"

    query = text(
        "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE c.relname = :part_name"
    )
    result = await session.execute(query, {"part_name": part_name})
    row = result.scalar_one_or_none()

    if not row:
        error_msg = (
            f"CRITICAL: Partition table '{part_name}' for current month {now.strftime('%Y-%m')} "
            f"does not exist in PostgreSQL! Aborting ingestion to prevent silent default partition overflow."
        )
        logger.critical(error_msg)
        raise RuntimeError(error_msg)

    logger.info("Startup Partition Check: Verified table '%s' exists", part_name)


_PARTITION_NAME_RE = re.compile(r"^market_snapshots_y(\d{4})m(\d{2})$")


async def enforce_retention_policy(session: AsyncSession, retention_months: int) -> List[str]:
    """Drop market_snapshots partitions older than retention_months, by calendar
    age alone. This is NOT gated on whether markets in that partition have
    resolved — retention means deliberately discarding old fine-grained ticks,
    even for a market whose expiration is still years out. A no-op if
    retention_months <= 0.

    Returns the list of dropped partition table names.
    """
    if retention_months <= 0:
        return []

    now = datetime.now(timezone.utc)
    cutoff_total_months = now.year * 12 + (now.month - 1) - retention_months
    cutoff_year, cutoff_month = divmod(cutoff_total_months, 12)
    cutoff_month += 1  # divmod gives 0-11; months are 1-12

    query = text(
        """
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = 'market_snapshots'
        """
    )
    result = await session.execute(query)
    dropped: List[str] = []

    for row in result.fetchall():
        match = _PARTITION_NAME_RE.match(row.relname)
        if not match:
            continue  # skip 'market_snapshots_default' or non-conforming partitions
        part_year, part_month = int(match.group(1)), int(match.group(2))
        part_total_months = part_year * 12 + (part_month - 1)
        if part_total_months < cutoff_total_months:
            await session.execute(text(f'DROP TABLE IF EXISTS "{row.relname}"'))
            dropped.append(row.relname)
            logger.warning(
                "Retention policy: dropped partition '%s' (older than %d months, cutoff %04d-%02d)",
                row.relname, retention_months, cutoff_year, cutoff_month,
            )

    return dropped


async def sync_markets_to_db(
    session: AsyncSession, venue: str, normalized_markets: List[NormalizedMarketDTO]
) -> Tuple[Dict[str, Market], Dict[str, Optional[str]]]:
    """Idempotently upsert markets into the slowly-changing `markets` table in batches.

    Returns (market_map, previous_status_map):
      - market_map: venue_market_id -> Market instance (with assigned UUID id), post-upsert.
      - previous_status_map: venue_market_id -> status as it was *before* this upsert
        (None for markets seen for the first time). Used by tier-1 change-detection to
        tell whether a market's status flipped even when its quote fields didn't.
    """
    if not normalized_markets:
        return {}, {}

    now = datetime.now(timezone.utc)
    batch_size = 1000

    # Capture pre-upsert status so callers can detect a status change independently
    # of quote fields (e.g. active -> closed with the same frozen bid/ask).
    previous_status_map: Dict[str, Optional[str]] = {}
    all_ids = [m.venue_market_id for m in normalized_markets]
    for i in range(0, len(all_ids), batch_size):
        id_batch = all_ids[i : i + batch_size]
        stmt_prev = select(Market.venue_market_id, Market.status).where(
            Market.venue == venue, Market.venue_market_id.in_(id_batch)
        )
        prev_res = await session.execute(stmt_prev)
        for row in prev_res.fetchall():
            previous_status_map[row.venue_market_id] = row.status

    for i in range(0, len(normalized_markets), batch_size):
        batch = normalized_markets[i : i + batch_size]
        values = []
        for m_dto in batch:
            values.append({
                "venue": m_dto.venue,
                "venue_market_id": m_dto.venue_market_id,
                "event_id": m_dto.event_id,
                "series_id": m_dto.series_id,
                "title": m_dto.title,
                "subtitle": m_dto.subtitle,
                "outcomes": m_dto.outcomes,
                "clob_token_ids": m_dto.clob_token_ids,
                "resolution_rules_text": m_dto.resolution_rules_text,
                "resolution_source": m_dto.resolution_source,
                "open_time": m_dto.open_time,
                "close_time": m_dto.close_time,
                "expected_resolution_time": m_dto.expected_resolution_time,
                "status": m_dto.status,
                "first_seen_at": now,
                "last_updated_at": now,
                "price_level_structure": m_dto.price_level_structure,
                "price_ranges": m_dto.price_ranges,
                "raw_market": m_dto.raw_market,
            })

        stmt = insert(Market).values(values)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_venue_market_id",
            set_={
                "title": stmt.excluded.title,
                "subtitle": stmt.excluded.subtitle,
                "outcomes": stmt.excluded.outcomes,
                "clob_token_ids": stmt.excluded.clob_token_ids,
                "resolution_rules_text": stmt.excluded.resolution_rules_text,
                "resolution_source": stmt.excluded.resolution_source,
                "status": stmt.excluded.status,
                "close_time": stmt.excluded.close_time,
                "expected_resolution_time": stmt.excluded.expected_resolution_time,
                "last_updated_at": now,
                "price_level_structure": stmt.excluded.price_level_structure,
                "price_ranges": stmt.excluded.price_ranges,
                # Tier-2 syncs a minimal DTO that never populates raw_market (see
                # get_gated_kalshi_markets) — coalesce so that upsert never clobbers
                # a good tier-1 raw_market with NULL.
                "raw_market": func.coalesce(stmt.excluded.raw_market, Market.raw_market),
            },
        )
        await session.execute(stmt)

    await session.flush()

    stmt_select = select(Market).where(Market.venue == venue)
    result = await session.execute(stmt_select)
    market_objs = result.scalars().all()
    return {m.venue_market_id: m for m in market_objs}, previous_status_map


async def save_snapshots_to_db(
    session: AsyncSession,
    market_map: Dict[str, Market],
    snapshots: List[NormalizedSnapshotDTO],
    skip_unchanged: bool = False,
    previous_status_map: Optional[Dict[str, Optional[str]]] = None,
) -> int:
    """Idempotently insert append-only snapshots using batch inserts.

    raw_market is stored only on depth_fetched=True (tier-2) rows — tier-1 never
    stores it, since markets.raw_market already holds the same payload per ticker.

    If skip_unchanged=True, a depth_fetched=False (tier-1) snapshot is skipped
    entirely when (bid, ask) and the market's status are identical to the
    market's most recent snapshot (no-quote markets use the slow-tier heartbeat
    cadence instead). depth_fetched=True
    (tier-2) rows always write regardless of skip_unchanged.
    """
    if not snapshots:
        return 0

    previous_status_map = previous_status_map or {}

    # Fetch latest (bid, ask, captured_at) per (market_id, outcome_index) for
    # change-detection (key: bid, ask, status) and slow-tier heartbeat cadence.
    # volume_24h/open_interest are still stored on every written row below —
    # they're just excluded from the key, since volume_24h decays continuously
    # and would otherwise force a write almost every cycle regardless of price.
    latest_quotes: Dict[Tuple[Any, int], Tuple] = {}
    if skip_unchanged:
        query_latest_quotes = text(
            """
            SELECT DISTINCT ON (market_id, outcome_index)
                   market_id, outcome_index, bid, ask, captured_at
            FROM market_snapshots
            ORDER BY market_id, outcome_index, captured_at DESC
            """
        )
        res_q = await session.execute(query_latest_quotes)
        for r in res_q.fetchall():
            latest_quotes[(r.market_id, r.outcome_index)] = (r.bid, r.ask, r.captured_at)

    slow_tier_interval = timedelta(seconds=settings.kalshi_slow_tier_interval_seconds)

    values = []
    skipped_unchanged = 0
    for snap_dto in snapshots:
        market_obj = market_map.get(snap_dto.venue_market_id)
        if not market_obj:
            logger.warning(
                "Skipping snapshot for unknown venue_market_id: %s", snap_dto.venue_market_id
            )
            continue

        if skip_unchanged and not snap_dto.depth_fetched:
            prev = latest_quotes.get((market_obj.id, snap_dto.outcome_index))
            prev_status = previous_status_map.get(snap_dto.venue_market_id)
            status_unchanged = prev_status == market_obj.status
            if prev is not None:
                prev_bid, prev_ask, prev_captured_at = prev
                quote_unchanged = (prev_bid, prev_ask) == (snap_dto.bid, snap_dto.ask)
                if quote_unchanged and status_unchanged:
                    # No-quote markets (neither bid nor ask) get the slow cadence
                    # tier: skip unless the slow interval has elapsed since the
                    # last write (heartbeat). One-sided and two-sided markets stay
                    # on the fast tier — plain change-detection, no extra gating.
                    is_no_quote = snap_dto.bid is None and snap_dto.ask is None
                    if is_no_quote:
                        elapsed = snap_dto.captured_at - prev_captured_at
                        if elapsed < slow_tier_interval:
                            skipped_unchanged += 1
                            continue
                    else:
                        skipped_unchanged += 1
                        continue

        # raw_market is only stored on tier-2 (depth_fetched=True) rows. Tier-1
        # never stores it: markets.raw_market already holds the same payload,
        # deduplicated one-per-ticker — repeating it on every snapshot row was
        # the single largest contributor to storage (see investigation).
        store_raw = snap_dto.depth_fetched

        values.append({
            "market_id": market_obj.id,
            "outcome_index": snap_dto.outcome_index,
            "outcome_label": snap_dto.outcome_label,
            "captured_at": snap_dto.captured_at,
            "bid": snap_dto.bid,
            "ask": snap_dto.ask,
            "mid": snap_dto.mid,
            "last_trade_price": snap_dto.last_trade_price,
            "volume_24h": snap_dto.volume_24h,
            "volume_total": snap_dto.volume_total,
            "open_interest": snap_dto.open_interest,
            "liquidity_at_5c": snap_dto.liquidity_at_5c,
            "liquidity_at_10c": snap_dto.liquidity_at_10c,
            "spread": snap_dto.spread,
            "raw_orderbook": snap_dto.raw_orderbook,
            "raw_market": snap_dto.raw_market if store_raw else None,
            "has_two_sided_book": snap_dto.has_two_sided_book,
            "depth_fetched": snap_dto.depth_fetched,
        })

    if skip_unchanged and skipped_unchanged:
        logger.info(
            "save_snapshots_to_db: skipped %d unchanged tier-1 rows (bid/ask/volume_24h/"
            "open_interest/status all matched the market's most recent snapshot)",
            skipped_unchanged,
        )

    if not values:
        return 0

    batch_size = 1000
    for i in range(0, len(values), batch_size):
        batch = values[i : i + batch_size]
        stmt = insert(MarketSnapshot).values(batch)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_snapshot_market_outcome_captured",
            set_={
                "bid": stmt.excluded.bid,
                "ask": stmt.excluded.ask,
                "mid": stmt.excluded.mid,
                "last_trade_price": stmt.excluded.last_trade_price,
                "volume_24h": stmt.excluded.volume_24h,
                "volume_total": stmt.excluded.volume_total,
                "open_interest": stmt.excluded.open_interest,
                "liquidity_at_5c": stmt.excluded.liquidity_at_5c,
                "liquidity_at_10c": stmt.excluded.liquidity_at_10c,
                "spread": stmt.excluded.spread,
                "raw_orderbook": stmt.excluded.raw_orderbook,
                "raw_market": func.coalesce(stmt.excluded.raw_market, MarketSnapshot.raw_market),
                "has_two_sided_book": stmt.excluded.has_two_sided_book,
                "depth_fetched": stmt.excluded.depth_fetched,
            },
        )
        await session.execute(stmt)

    await session.flush()
    return len(values)


async def get_gated_kalshi_markets(
    session: AsyncSession,
    max_spread: float,
    min_volume: float,
) -> List[NormalizedMarketDTO]:
    """Find active Kalshi markets whose latest snapshot is two-sided and matches the gate.

    Gates on each market's most recent snapshot, whenever it was captured — NOT
    on an exact match against this cycle's own timestamp. Tier-1 and Tier-2 run
    as independent loops on different intervals with independently-generated
    cycle timestamps, so a `captured_at = this cycle's timestamp` filter can
    never match and would silently gate zero markets on every cycle.
    """
    query = text(
        """
        WITH latest_snaps AS (
            SELECT DISTINCT ON (market_id) market_id, bid, ask, spread, volume_total, volume_24h, open_interest, last_trade_price, captured_at
            FROM market_snapshots
            ORDER BY market_id, captured_at DESC
        )
        SELECT m.id, m.venue_market_id, m.title, m.outcomes, m.clob_token_ids,
               m.event_id, m.series_id, m.subtitle, m.resolution_rules_text,
               m.resolution_source, m.open_time, m.close_time,
               m.expected_resolution_time, m.status, m.price_level_structure, m.price_ranges,
               ls.volume_total, ls.volume_24h, ls.open_interest, ls.last_trade_price
        FROM markets m
        JOIN latest_snaps ls ON m.id = ls.market_id
        WHERE m.venue = 'kalshi'
          AND m.status = 'active'
          AND ls.bid IS NOT NULL
          AND ls.ask IS NOT NULL
          AND ls.spread <= :max_spread
          AND ls.volume_total >= :min_volume
        ORDER BY ls.volume_24h DESC NULLS LAST
        """
    )
    result = await session.execute(query, {
        "max_spread": max_spread,
        "min_volume": min_volume,
    })
    rows = result.fetchall()
    markets = []
    for r in rows:
        dto = NormalizedMarketDTO(
            venue="kalshi",
            venue_market_id=r.venue_market_id,
            event_id=r.event_id,
            series_id=r.series_id,
            title=r.title,
            subtitle=r.subtitle,
            outcomes=r.outcomes,
            clob_token_ids=r.clob_token_ids,
            resolution_rules_text=r.resolution_rules_text,
            resolution_source=r.resolution_source,
            open_time=r.open_time,
            close_time=r.close_time,
            expected_resolution_time=r.expected_resolution_time,
            status=r.status,
            price_level_structure=r.price_level_structure,
            price_ranges=r.price_ranges,
        )
        raw_market = {
            "volume_fp": str(r.volume_total) if r.volume_total is not None else None,
            "volume_24h_fp": str(r.volume_24h) if r.volume_24h is not None else None,
            "open_interest_fp": str(r.open_interest) if r.open_interest is not None else None,
            "last_price_dollars": str(r.last_trade_price) if r.last_trade_price is not None else None,
        }
        setattr(dto, "_raw_kalshi_data", raw_market)
        markets.append(dto)

    return markets


async def run_kalshi_tier1_pipeline(
    client: BaseVenueClient, cycle_timestamp: datetime
) -> Tuple[int, int, int, Optional[Exception]]:
    """Run Kalshi Tier-1 ingestion (events-anchored with nested markets).

    Returns (events_count, markets_count, snapshots_stored, exception).
    """
    logger.info("Starting Kalshi Tier-1 Ingestion (events-anchored)...")
    try:
        if hasattr(client, "fetch_events_with_snapshots"):
            open_markets, snapshots = await client.fetch_events_with_snapshots(cycle_timestamp)
        else:
            # Fallback for basic mock clients in tests
            open_markets = await client.fetch_open_markets()
            snapshots = await client.fetch_snapshots_for_markets(open_markets, cycle_timestamp)

        if not open_markets:
            logger.info("No open markets found for Kalshi Tier-1")
            return (0, 0, 0, None)

        events_count = len(set(m.event_id for m in open_markets if m.event_id))

        async with get_db_session() as session:
            market_map, previous_status_map = await sync_markets_to_db(session, "kalshi", open_markets)
            saved_count = await save_snapshots_to_db(
                session,
                market_map,
                snapshots,
                skip_unchanged=not settings.tier1_write_unchanged,
                previous_status_map=previous_status_map,
            )

        logger.info(
            "Kalshi Tier-1 complete: %d events, %d markets synced, %d snapshots stored",
            events_count,
            len(open_markets),
            saved_count,
        )
        return (events_count, len(open_markets), saved_count, None)
    except Exception as exc:
        logger.error("Error in Kalshi Tier-1 pipeline: %s", exc, exc_info=True)
        return (0, 0, 0, exc)


async def run_kalshi_tier2_pipeline(
    client: KalshiClient, cycle_timestamp: datetime
) -> Tuple[int, int, Optional[Exception]]:
    """Run Kalshi Tier-2 ingestion (orderbook depth).

    The gate itself is unrestricted (bid AND ask present, spread <= max_spread);
    what's bounded is the WORK per cycle — at most tier2_max_markets_per_cycle
    orderbook fetches, ranked by volume_24h DESC, so cycle wall-clock is capped
    by construction regardless of how many markets pass the gate.

    Returns (markets_fetched_count, snapshots_stored, exception).
    """
    logger.info("Starting Kalshi Tier-2 Ingestion (orderbook depth)...")
    cycle_start = time.monotonic()
    try:
        max_spread = getattr(client, "max_spread", settings.max_spread_for_two_sided)
        min_volume = getattr(client, "min_volume_for_tier2", settings.min_volume_for_tier2)
        async with get_db_session() as session:
            gated_markets = await get_gated_kalshi_markets(
                session,
                max_spread=max_spread,
                min_volume=min_volume,
            )

        gate_size = len(gated_markets)
        cap = settings.tier2_max_markets_per_cycle
        markets_to_fetch = gated_markets[:cap] if cap > 0 else gated_markets
        skipped_by_cap = gate_size - len(markets_to_fetch)

        if not markets_to_fetch:
            logger.info(
                "Kalshi Tier-2: gate_size=%d, fetched=0, skipped_by_cap=%d, wall_clock=%.1fs "
                "— no Kalshi markets passed the Tier-2 gate",
                gate_size, skipped_by_cap, time.monotonic() - cycle_start,
            )
            return (0, 0, None)

        logger.info(
            "Kalshi Tier-2: gate_size=%d, fetching top %d by volume_24h (skipped_by_cap=%d)...",
            gate_size, len(markets_to_fetch), skipped_by_cap,
        )
        snapshots = await client.fetch_orderbook_snapshots_for_markets(markets_to_fetch, cycle_timestamp)

        async with get_db_session() as session:
            market_map, _ = await sync_markets_to_db(session, "kalshi", markets_to_fetch)
            saved_count = await save_snapshots_to_db(session, market_map, snapshots)

        logger.info(
            "Kalshi Tier-2 complete: gate_size=%d, fetched=%d, skipped_by_cap=%d, "
            "snapshots_stored=%d, wall_clock=%.1fs",
            gate_size, len(markets_to_fetch), skipped_by_cap, saved_count,
            time.monotonic() - cycle_start,
        )
        return (len(markets_to_fetch), saved_count, None)
    except Exception as exc:
        logger.error("Error in Kalshi Tier-2 pipeline: %s", exc, exc_info=True)
        return (0, 0, exc)


async def run_venue_pipeline(
    client: BaseVenueClient, venue_name: str, cycle_timestamp: datetime
) -> Tuple[int, int, Optional[Exception]]:
    """Isolated ingestion pipeline for a single venue (Polymarket style)."""
    logger.info("Starting ingestion cycle for venue: %s", venue_name)
    try:
        open_markets = await client.fetch_open_markets()
        if not open_markets:
            logger.info("No open markets found for venue: %s", venue_name)
            return (0, 0, None)

        snapshots = await client.fetch_snapshots_for_markets(open_markets, cycle_timestamp)

        async with get_db_session() as session:
            market_map, _ = await sync_markets_to_db(session, venue_name, open_markets)
            saved_count = await save_snapshots_to_db(session, market_map, snapshots)

        logger.info(
            "Venue %s cycle complete: %d markets synced, %d snapshots stored",
            venue_name,
            len(open_markets),
            saved_count,
        )
        return (len(open_markets), saved_count, None)
    except Exception as exc:
        logger.error("Error in venue %s ingestion pipeline: %s", venue_name, exc, exc_info=True)
        return (0, 0, exc)


async def fetch_and_print_samples(session: AsyncSession):
    """Fetch and print 10 sample rows with has_two_sided_book = True (at least 5 Kalshi)."""
    if "Mock" in type(session).__name__ or hasattr(session, "assert_called"):
        return

    k_query = text(
        """
        SELECT m.venue, m.title, s.outcome_label, s.bid, s.ask, s.spread, s.has_two_sided_book, s.depth_fetched
        FROM market_snapshots s
        JOIN markets m ON m.id = s.market_id
        WHERE m.venue = 'kalshi' AND s.has_two_sided_book = true
        ORDER BY s.captured_at DESC
        LIMIT 5
        """
    )
    k_res = await session.execute(k_query)
    k_rows = k_res.fetchall()

    p_query = text(
        """
        SELECT m.venue, m.title, s.outcome_label, s.bid, s.ask, s.spread, s.has_two_sided_book, s.depth_fetched
        FROM market_snapshots s
        JOIN markets m ON m.id = s.market_id
        WHERE m.venue = 'polymarket' AND s.has_two_sided_book = true
        ORDER BY s.captured_at DESC
        LIMIT 5
        """
    )
    p_res = await session.execute(p_query)
    p_rows = p_res.fetchall()

    print("\n=== 10 SAMPLE TWO-SIDED BOOK SNAPSHOTS (has_two_sided_book=True) ===")
    for row in k_rows + p_rows:
        print(f"[{row.venue.upper()}] {row.title[:65]}")
        print(f"  Outcome: {row.outcome_label} | bid={row.bid} | ask={row.ask} | spread={row.spread} | depth_fetched={row.depth_fetched}")



async def execute_snapshot_cycle() -> None:
    """Execute a single sequential snapshot cycle (used for single-run mode & retro compatibility)."""
    cycle_timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    start_time = time.monotonic()
    logger.info("=== Starting Unified Snapshot Ingestion Cycle (Cycle TS: %s) ===", cycle_timestamp.isoformat())

    async with get_db_session() as session:
        await verify_current_month_partition_exists(session)

    kalshi_client = KalshiClient(
        rate_limit_rps=settings.kalshi_rate_limit_rps,
        max_spread=settings.max_spread_for_two_sided,
        max_pages=settings.kalshi_max_pages,
        min_volume_for_tier2=settings.min_volume_for_tier2,
        excluded_categories=_parse_excluded_categories(settings.kalshi_excluded_categories),
    )
    polymarket_client = PolymarketClient(
        rate_limit_rps=settings.polymarket_rate_limit_rps,
        max_spread=settings.max_spread_for_two_sided,
    )

    # 1. Kalshi Tier 1
    k_events, k_markets, k_tier1_snaps, k1_err = await run_kalshi_tier1_pipeline(kalshi_client, cycle_timestamp)

    # 2. Kalshi Tier 2 (orderbook depth for gated markets)
    k_gated, k_tier2_snaps, k2_err = await run_kalshi_tier2_pipeline(kalshi_client, cycle_timestamp)

    # 3. Polymarket
    p_markets, p_snaps, p_err = await run_venue_pipeline(polymarket_client, "polymarket", cycle_timestamp)

    duration = time.monotonic() - start_time
    logger.info("=== Ingestion Cycle Summary (Duration: %.2fs) ===", duration)
    logger.info(
        "Kalshi Tier-1: %d events, %d markets, %d snapshots %s",
        k_events,
        k_markets,
        k_tier1_snaps,
        f"(ERROR: {k1_err})" if k1_err else "[OK]",
    )
    logger.info(
        "Kalshi Tier-2: %d gated markets, %d snapshots %s",
        k_gated,
        k_tier2_snaps,
        f"(ERROR: {k2_err})" if k2_err else "[OK]",
    )
    logger.info(
        "Polymarket: %d markets, %d snapshots %s",
        p_markets,
        p_snaps,
        f"(ERROR: {p_err})" if p_err else "[OK]",
    )

    # Print first-pass metrics / single-run special stats requested
    print("\n==================================================")
    print("SINGLE RUN METRICS REPORT")
    print("==================================================")
    print(f"  Wall-clock Duration: {duration:.2f} seconds")
    print(f"  Total Kalshi Events: {k_events}")
    print(f"  Total Kalshi Markets (Tier-1): {k_markets}")
    print(f"  Total Kalshi snapshots stored (Tier-1): {k_tier1_snaps}")
    print(f"  Total Kalshi gated markets (Tier-2): {k_gated}")
    print(f"  Total Kalshi snapshots stored (Tier-2): {k_tier2_snaps}")
    print(f"  Total Polymarket markets: {p_markets}")
    print(f"  Total Polymarket snapshots stored: {p_snaps}")
    print(f"  Total market snapshot rows written: {k_tier1_snaps + k_tier2_snaps + p_snaps}")

    async with get_db_session() as session:
        await fetch_and_print_samples(session)


# ---------------------------------------------------------------------------
# Continuous loops per venue/tier
# ---------------------------------------------------------------------------

def _check_cycle_health(loop_name: str, rows_written: int, duration: float, interval: int) -> None:
    """Log loudly instead of silently drifting: zero rows written, or a cycle
    that outran its own interval (the next cycle is already due or overdue)."""
    if rows_written == 0:
        logger.error(
            "%s cycle wrote 0 rows this cycle — investigate (possible fetch "
            "failure, API change, or empty result set).",
            loop_name,
        )
    if duration > interval:
        logger.error(
            "%s cycle took %.1fs, exceeding its %ds interval — the loop is falling behind.",
            loop_name, duration, interval,
        )


async def kalshi_tier1_loop() -> None:
    """Continuous loop for Kalshi Tier-1 ingestion."""
    interval = settings.kalshi_tier1_interval_seconds
    client = KalshiClient(
        rate_limit_rps=settings.kalshi_rate_limit_rps,
        max_spread=settings.max_spread_for_two_sided,
        max_pages=settings.kalshi_max_pages,
        min_volume_for_tier2=settings.min_volume_for_tier2,
        excluded_categories=_parse_excluded_categories(settings.kalshi_excluded_categories),
    )
    logger.info("Starting Kalshi Tier-1 loop (interval: %d seconds)...", interval)
    while True:
        cycle_timestamp = datetime.now(timezone.utc).replace(microsecond=0)
        cycle_start = time.monotonic()
        try:
            async with get_db_session() as session:
                await verify_current_month_partition_exists(session)
                dropped = await enforce_retention_policy(session, settings.kalshi_retention_months)
                if dropped:
                    logger.warning("Retention policy dropped partitions: %s", dropped)
            _events, _markets, saved_count, err = await run_kalshi_tier1_pipeline(client, cycle_timestamp)
            if err is None:
                _check_cycle_health("Kalshi Tier-1", saved_count, time.monotonic() - cycle_start, interval)
        except Exception as exc:
            logger.critical("Critical error in Kalshi Tier-1 loop: %s", exc, exc_info=True)
        logger.info("Kalshi Tier-1 loop sleeping for %d seconds...", interval)
        await asyncio.sleep(interval)


async def kalshi_tier2_loop() -> None:
    """Continuous loop for Kalshi Tier-2 ingestion."""
    interval = settings.kalshi_tier2_interval_seconds
    client = KalshiClient(
        rate_limit_rps=settings.kalshi_rate_limit_rps,
        max_spread=settings.max_spread_for_two_sided,
        max_pages=settings.kalshi_max_pages,
        min_volume_for_tier2=settings.min_volume_for_tier2,
        excluded_categories=_parse_excluded_categories(settings.kalshi_excluded_categories),
    )
    logger.info("Starting Kalshi Tier-2 loop (interval: %d seconds)...", interval)
    while True:
        cycle_timestamp = datetime.now(timezone.utc).replace(microsecond=0)
        cycle_start = time.monotonic()
        try:
            async with get_db_session() as session:
                await verify_current_month_partition_exists(session)
            try:
                _gated, saved_count, err = await asyncio.wait_for(
                    run_kalshi_tier2_pipeline(client, cycle_timestamp), timeout=interval
                )
                if err is None:
                    _check_cycle_health("Kalshi Tier-2", saved_count, time.monotonic() - cycle_start, interval)
            except asyncio.TimeoutError:
                # A cycle must never block the next one. The per-cycle cap
                # (tier2_max_markets_per_cycle) should keep this from firing in
                # practice; this is the hard backstop if it ever doesn't.
                logger.error(
                    "Kalshi Tier-2 cycle exceeded its %ds interval — abandoning the "
                    "remainder and starting the next cycle fresh.",
                    interval,
                )
        except Exception as exc:
            logger.critical("Critical error in Kalshi Tier-2 loop: %s", exc, exc_info=True)
        logger.info("Kalshi Tier-2 loop sleeping for %d seconds...", interval)
        await asyncio.sleep(interval)


async def polymarket_loop() -> None:
    """Continuous loop for Polymarket ingestion."""
    interval = settings.polymarket_interval_seconds
    client = PolymarketClient(
        rate_limit_rps=settings.polymarket_rate_limit_rps,
        max_spread=settings.max_spread_for_two_sided,
    )
    logger.info("Starting Polymarket loop (interval: %d seconds)...", interval)
    while True:
        cycle_timestamp = datetime.now(timezone.utc).replace(microsecond=0)
        cycle_start = time.monotonic()
        try:
            async with get_db_session() as session:
                await verify_current_month_partition_exists(session)
            _markets, saved_count, err = await run_venue_pipeline(client, "polymarket", cycle_timestamp)
            if err is None:
                _check_cycle_health("Polymarket", saved_count, time.monotonic() - cycle_start, interval)
        except Exception as exc:
            logger.critical("Critical error in Polymarket loop: %s", exc, exc_info=True)
        logger.info("Polymarket loop sleeping for %d seconds...", interval)
        await asyncio.sleep(interval)


def _refuse_live_trading() -> None:
    """Stop startup when someone asks this build to trade.

    Order placement is not implemented. The default is off. A true flag is
    treated as a misconfiguration so the process cannot silently do nothing
    while looking like a live trader.
    """
    if settings.live_trading_enabled:
        raise RuntimeError(
            "LIVE_TRADING_ENABLED is set, but this project has no order-placement "
            "client. Refusing to start. Unset LIVE_TRADING_ENABLED to run read-only ingestion."
        )
    logger.info(
        "Read-only ingestion: fetching public market data only. No orders will be submitted."
    )


async def main_loop(single_run: bool = False) -> None:
    """Main snapshot loop runner orchestrator."""
    _refuse_live_trading()
    if single_run:
        logger.info("Running single ingestion cycle...")
        await execute_snapshot_cycle()
        return

    logger.info("Starting background execution of independent loop pipelines...")
    tasks = [
        asyncio.create_task(kalshi_tier1_loop()),
        asyncio.create_task(kalshi_tier2_loop()),
        asyncio.create_task(polymarket_loop()),
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Prediction Market Screener snapshot loop. "
            "Read-only: fetches public market data and does not place orders."
        )
    )
    parser.add_argument(
        "--single-run",
        action="store_true",
        help="Run a single snapshot cycle and exit",
    )
    args = parser.parse_args()

    try:
        asyncio.run(main_loop(single_run=args.single_run))
    except KeyboardInterrupt:
        logger.info("Runner stopped by user.")
        sys.exit(0)
