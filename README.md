# Prediction Market Screener

Read-only ingestion and archival for [Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com) order books. This is Shay Bouchles's second attempt at prediction-market trading automation. This attempt is the data layer: it watches public markets and stores snapshots in Postgres. It does not trade.

> **Disclaimer.** This is an experimental project. It is not financial advice, not a broker, and not an offer to buy or sell any contract. Nothing here is a recommendation to enter a position. Market data can be missing, delayed, or wrong. The liquidity figures can be economically misleading (see below). If you trade on anything derived from this archive, you can lose money, including the entire amount you commit.

The process is read-only by construction. Venue clients issue HTTP GET requests against public market-data endpoints and write rows to Postgres. There is no wallet, no API key, and no order, cancel, or amend path. `LIVE_TRADING_ENABLED` defaults to off. If it is set to true, the runner refuses to start, because this build has no execution client and should not pretend otherwise.

Coming back after a break? [`PROJECT_STATUS.md`](PROJECT_STATUS.md) is the session log (decisions and open questions). This file is the architecture and setup reference. [`DEPLOY.md`](DEPLOY.md) is an undeployed Railway plan.

## What it does

Three independent async loops share one Postgres database:

| Loop | Default cadence | What it stores |
|---|---|---|
| Kalshi Tier-1 | 15 min | Top-of-book quotes for non-excluded events, via `/events?with_nested_markets=true` |
| Kalshi Tier-2 | 5 min | Full order-book depth for the top markets that pass a two-sided-book gate |
| Polymarket | 5 min | Gamma metadata plus a CLOB book snapshot per outcome |

A separate matcher (`src/matching/baseline.py`) can score Kalshi and Polymarket titles with `rapidfuzz` and store candidate pairs. The snapshot runner does not call it.

Nothing is sent back to either venue.

## Architecture

```
src/
  config.py            Settings (environment / .env), including the live-trading guard
  runner.py            Snapshot loops: partition check, fetch, upsert
  clients/
    base.py            NormalizedMarketDTO, NormalizedSnapshotDTO, BaseVenueClient
    kalshi.py          Kalshi Trade API v2 client (public GET, tiered, rate-limited)
    polymarket.py      Polymarket Gamma + CLOB client (public GET)
    rate_limiter.py    Async token bucket
  analytics/
    liquidity.py       Mid price and notional depth near mid
  matching/
    baseline.py        Cross-venue title similarity
  db/
    models.py          SQLAlchemy 2.0: Market, MarketSnapshot, CrossVenueCandidate
    session.py         Async session factory
alembic/               PostgreSQL migrations
tests/                 pytest-asyncio + respx unit tests
```

```
Kalshi /events ──► Tier-1 quotes ──► markets + market_snapshots
                         │
                         ▼
              gate (two-sided, spread, volume)
                         │
Kalshi /orderbook ─► Tier-2 depth (capped) ──► market_snapshots
Polymarket Gamma + CLOB ──────────────────────► markets + market_snapshots
```

Snapshots are append-only and partitioned by month on `captured_at`. Migrations pre-create partitions from 2026-07 through 2027-07. The runner aborts if the current month's partition is missing, so later months need a new migration before they start.

## Setup

Requires Python 3.11+ and Docker (for local Postgres).

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"    # runtime + pytest + ruff

docker compose up -d
alembic upgrade head

# One cycle, then exit
python -m src.runner --single-run

# Three loops until Ctrl-C
python -m src.runner
```

`pip install .` or `pip install -r requirements.txt` installs the worker without test tools. `requirements.txt` is the runtime list; `pyproject.toml` loads it.

The compose file publishes Postgres on `localhost:5432` with the database `pm_screener`. That matches the default `DATABASE_URL`. It is a local development database, not a hosted credential.

## Configuration

Settings come from the environment or a `.env` file (`src/config.py`, pydantic-settings, case-insensitive). `.env.example` lists names only. Copy it to `.env` and uncomment a line only when you want to override the default. Do not commit `.env`.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://postgres:postgres@localhost:5432/pm_screener` | Async SQLAlchemy DSN. Must use the `postgresql+asyncpg://` scheme. |
| `LIVE_TRADING_ENABLED` | `false` | Must stay false. `true` makes the runner exit. There is no order client. |
| `KALSHI_TIER1_INTERVAL_SECONDS` | `900` | Kalshi Tier-1 loop interval (15 min). |
| `KALSHI_TIER2_INTERVAL_SECONDS` | `300` | Kalshi Tier-2 loop interval (5 min). |
| `POLYMARKET_INTERVAL_SECONDS` | `300` | Polymarket loop interval (5 min). |
| `POLL_INTERVAL_SECONDS` | `300` | Legacy. Not used by the current loops. |
| `KALSHI_RATE_LIMIT_RPS` | `2.0` | Kalshi request cap. |
| `POLYMARKET_RATE_LIMIT_RPS` | `5.0` | Polymarket request cap. |
| `MAX_SPREAD_FOR_TWO_SIDED` | `0.10` | Max `(ask − bid)` in probability units for `has_two_sided_book`. |
| `MIN_VOLUME_FOR_TIER2` | `0.0` | Volume floor for Tier-2. The per-cycle cap is the real bound. |
| `TIER2_MAX_MARKETS_PER_CYCLE` | `250` | Order-book fetches per Tier-2 cycle, ranked by `volume_24h` descending. |
| `TIER1_WRITE_UNCHANGED` | `false` | When false, skip a Tier-1 row whose `(bid, ask, status)` match the last snapshot. |
| `KALSHI_SLOW_TIER_INTERVAL_SECONDS` | `21600` | Heartbeat (6 h) for markets with no quote on either side. |
| `KALSHI_EXCLUDED_CATEGORIES` | `Sports` | Comma-separated Kalshi event categories dropped before Tier-1. Empty string disables the filter. Esports is filed under `Sports`. |
| `KALSHI_RETENTION_MONTHS` | `0` | Drop `market_snapshots` partitions older than N calendar months. `0` keeps everything. |
| `KALSHI_MAX_PAGES` | `1000` | Cursor-pagination kill switch. |
| `LOG_LEVEL` | `INFO` | App log level. `sqlalchemy.engine` stays at `WARNING`. |

## Ingestion strategy

### Kalshi

Ingestion is event-anchored. The flat `/markets` stream is not the discovery path: it is dominated by combinatorial parlay legs that have no `/events` entry and no tradeable book.

- **Tier-1** paginates `/events?with_nested_markets=true` and stores top-of-book quotes with `depth_fetched = false`. `raw_market` is kept once on the `markets` row, not repeated on every Tier-1 snapshot. Unchanged `(bid, ask, status)` rows are skipped, except markets with no quote on either side, which get the slow heartbeat instead.
- **Tier-2** selects active markets whose latest snapshot is two-sided and narrow (`spread <= MAX_SPREAD_FOR_TWO_SIDED`), ranked by `volume_24h`, capped at `TIER2_MAX_MARKETS_PER_CYCLE`. It fetches `/orderbook`, computes `liquidity_at_5c` and `liquidity_at_10c`, and stores those rows with `depth_fetched = true`. A cycle that exceeds its interval is abandoned rather than blocking the next one.

### Polymarket

Every interval, the client loads open markets from the Gamma API and fetches a CLOB book for each outcome. Successful book fetches set `depth_fetched = true`.

### `has_two_sided_book`

A snapshot is two-sided only when both `bid` and `ask` are present and `(ask − bid) <= MAX_SPREAD_FOR_TWO_SIDED`. The raw `spread` is always stored, so a tighter cap can be applied later in SQL without re-fetching.

## Tests and lint

```bash
pytest -v
ruff check .
```

GitHub Actions (`.github/workflows/ci.yml`) installs the `dev` extra, runs Ruff, then runs the unit tests on Python 3.12. The tests mock HTTP (respx) and the database. CI does not start Postgres and does not call Kalshi or Polymarket.

## Known limitations

- **`liquidity_at_5c` / `liquidity_at_10c` overstates tail markets.** The metric sums notional within a few cents of mid. A contract priced near 0 or 1 can show a huge number that is not practically hittable. Filter with `has_two_sided_book` and a mid in a range such as `0.10` to `0.90` before aggregating.
- **Retention is implemented and defaults off.** `KALSHI_RETENTION_MONTHS=0` is the only value that has been run. Turning it on drops partitions by calendar age, including markets that have not resolved.
- **Partitions stop at 2027-07.** After that, ingestion refuses to start until a migration adds the new month.
- **A sleeping laptop pauses every loop.** Continuous collection needs a host that does not suspend. See [`DEPLOY.md`](DEPLOY.md).
- **No authenticated Kalshi API, and no WebSocket delta feed.** Depth is polled. Signed order entry was never built, on purpose.
- **This has not been deployed.** [`DEPLOY.md`](DEPLOY.md) is planning notes only.

The longer decision log is in [`PROJECT_STATUS.md`](PROJECT_STATUS.md).
