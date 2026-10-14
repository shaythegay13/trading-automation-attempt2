"""Configuration settings powered by pydantic-settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings read from environment variables or .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database DSN (SQLAlchemy asyncpg format).
    # The default matches docker-compose.yml (local Postgres only).
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/pm_screener"

    # Safety: this build only reads public market data. There is no order client.
    # When true, the runner refuses to start instead of attempting to trade.
    live_trading_enabled: bool = False

    # Kalshi tier-1: event-anchored ingestion interval (market-level quotes, no orderbook)
    kalshi_tier1_interval_seconds: int = 900  # 15 min

    # Kalshi tier-2: orderbook depth fetch for gate-passing markets
    kalshi_tier2_interval_seconds: int = 300  # 5 min

    # Polymarket snapshot interval (unchanged)
    polymarket_interval_seconds: int = 300  # 5 min

    # Legacy: kept for backward compatibility; not used in the new loop
    poll_interval_seconds: int = 300

    # Rate limits in requests per second
    # Kalshi: 2 RPS — safely under the stated API ceiling; validated empirically at 560+ pages.
    kalshi_rate_limit_rps: float = 2.0
    polymarket_rate_limit_rps: float = 5.0

    # Maximum bid-ask spread (in probability units [0,1]) for has_two_sided_book=True.
    # A snapshot is only flagged two-sided if both bid and ask exist AND (ask - bid) <= this value.
    # Default 0.10 (10 cents) — tune via MAX_SPREAD_FOR_TWO_SIDED env var.
    max_spread_for_two_sided: float = 0.10

    # Minimum volume (volume_fp) for a market to qualify for tier-2 orderbook fetch.
    # Default 0.0 — the per-cycle cap (tier2_max_markets_per_cycle) bounds the
    # work instead, so a volume floor isn't needed and would exclude the
    # pre-liquidity history of thin/forming books.
    min_volume_for_tier2: float = 0.0

    # Hard cap on how many gated markets Tier-2 fetches orderbook depth for in
    # a single cycle, ranked by volume_24h DESC. Bounds cycle wall-clock by
    # construction regardless of how many markets pass the gate. At 2 RPS,
    # 250 markets ≈ 125s against a 300s interval.
    tier2_max_markets_per_cycle: int = 250

    # Hard page cap on every cursor loop. If hit, log ERROR and abort — never run unbounded.
    kalshi_max_pages: int = 1000

    # Kalshi Tier-1 only: when False (default), a snapshot row is skipped if
    # (bid, ask, market status) are unchanged vs. the market's most recent
    # snapshot. volume_24h/open_interest are excluded from this key — volume_24h
    # decays continuously and would force a write almost every cycle regardless
    # of price. Set True to write every tier-1 snapshot unconditionally (old
    # behavior). Tier-2 depth rows always write regardless.
    tier1_write_unchanged: bool = False

    # Slow-tier heartbeat cadence (seconds) for markets with no quote on either
    # side (bid AND ask both absent). Such a market still gets at least one
    # snapshot per this interval even when nothing has changed, so its
    # last-known state doesn't go silently stale. Two-sided and one-sided
    # markets are unaffected — they stay on the tier-1 interval.
    kalshi_slow_tier_interval_seconds: int = 21600  # 6 hours

    # Kalshi event categories to exclude from tier-1 ingestion entirely, comma-
    # separated. Matched against the event's `category` field (authoritative,
    # from Kalshi — there is no reliable series_ticker prefix for this: sports
    # series tickers share no distinguishing substring beyond the universal
    # "KX" prefix). Esports (e.g. KXCS2MAP) is filed under "Sports" too, so
    # excluding "Sports" covers both. Empty string disables filtering.
    kalshi_excluded_categories: str = "Sports"

    # Retention: drop market_snapshots partitions older than this many months,
    # by calendar age alone — NOT gated on whether markets in that partition
    # have resolved. 0 (default) disables retention entirely; everything is kept.
    kalshi_retention_months: int = 0

    # Logging
    log_level: str = "INFO"


# Global settings singleton instance
settings = Settings()
