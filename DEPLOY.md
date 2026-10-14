# Deploying to Railway (worker, not yet done — planning notes only)

This documents what it would take to run the continuous ingestion loop
(`python -m src.runner`) as a persistent background worker on Railway. Nothing
here has been deployed; this is a plan to execute later.

## Service type: background worker, not a web service

This app has no HTTP server — it's a long-running `while True` loop. On
Railway that means:

- Create it as a Railway service from this repo, but **do not let Railway's
  default HTTP health check apply** — there's no port to bind. Disable the
  health check (or set it to a TCP/none check) in the service settings, or the
  deploy will be marked unhealthy and restarted in a loop.
- Start command: `python -m src.runner` (continuous mode — no `--single-run`).
- Leave `LIVE_TRADING_ENABLED` unset. The worker is read-only public-data
  ingestion. If that variable is true, the process exits on startup because
  this repo has no order-placement client.

## Dependency install

Runtime dependencies are declared in `requirements.txt`. `pyproject.toml`
loads that file, so either of these installs the worker:

```bash
pip install .
pip install -r requirements.txt
```

Nixpacks can build from either file. Test and lint tools (`pytest`,
`pytest-asyncio`, `respx`, `ruff`) are the `dev` extra and are not required
on the worker:

```bash
pip install -e ".[dev]"
```

## What's still missing before this can deploy

The dependency manifest is no longer the blocker. These gaps remain:

- No release command runs `alembic upgrade head` before the worker starts.
- `DATABASE_URL` from Railway's Postgres plugin uses `postgresql://`, and this
  app needs `postgresql+asyncpg://`. See the table below.
- Monthly `market_snapshots` partitions are pre-created only through 2027-07.
  Ingestion aborts when the current month's partition is missing.
- Nothing in this repo has been deployed. The notes below are still a plan.

## Environment variables

Everything is read via `src/config.py` (pydantic-settings), case-insensitive,
with sane defaults — only the ones below need explicit values on Railway:

| Variable | Why it needs setting on Railway |
|---|---|
| `DATABASE_URL` | **Must be rewritten.** Railway's Postgres plugin injects a plain `postgresql://...` URL as `DATABASE_URL`. This app needs the asyncpg driver in the scheme: `postgresql+asyncpg://...`. Either reference Railway's `DATABASE_URL` with the scheme edited in a Railway variable (`postgresql+asyncpg://${{Postgres.PGUSER}}:${{Postgres.PGPASSWORD}}@${{Postgres.PGHOST}}:${{Postgres.PGPORT}}/${{Postgres.PGDATABASE}}`), or add a one-line startup shim that swaps `postgresql://` → `postgresql+asyncpg://` if the raw Railway variable is reused directly. |
| `KALSHI_EXCLUDED_CATEGORIES` | Only if you want a different exclusion set than the `Sports` default — otherwise the default already applies. |
| `KALSHI_RETENTION_MONTHS` | Still `0` (disabled) by design — set explicitly if/when retention is turned on. |
| `LOG_LEVEL` | Defaults to `INFO`; set explicitly if you want `DEBUG` for a deploy (note: `sqlalchemy.engine` is pinned to `WARNING` regardless, so this won't flood logs with SQL). |

Everything else (`KALSHI_TIER1_INTERVAL_SECONDS`, `KALSHI_TIER2_INTERVAL_SECONDS`,
`POLYMARKET_INTERVAL_SECONDS`, `KALSHI_RATE_LIMIT_RPS`, `POLYMARKET_RATE_LIMIT_RPS`,
`MAX_SPREAD_FOR_TWO_SIDED`, `MIN_VOLUME_FOR_TIER2`, `TIER2_MAX_MARKETS_PER_CYCLE`,
`TIER1_WRITE_UNCHANGED`, `KALSHI_SLOW_TIER_INTERVAL_SECONDS`, `KALSHI_MAX_PAGES`)
already has the values this session validated — only override if you
deliberately want different behavior than what's running now.

`.env` itself is irrelevant on Railway — it only exists for local dev
(`python-dotenv`/pydantic-settings reads it if present). Railway injects
env vars directly into the process; no `.env` file is used or needed there.

## Things in the current setup that assume a local machine

- **`docker-compose.yml`** spins up a local Postgres container — not used on
  Railway, which provides Postgres as a separate managed plugin. This file
  stays for local dev only.
- **Migrations aren't wired into a deploy step.** Locally, `alembic upgrade
  head` is run by hand. On Railway this needs to happen either as a release
  command (run once before the worker starts) or manually via `railway run
  alembic upgrade head` after the first deploy — there's currently no
  automation for it.
- **Partition headroom is a manual, calendar-driven maintenance task,
  independent of hosting.** `market_snapshots` partitions currently exist
  through 2027-07 (13 months out from today). `verify_current_month_partition_exists()`
  raises and aborts ingestion if the current month's partition is missing, so
  someone needs to keep creating future partitions (via alembic migration)
  before they run out, on whatever host this runs on.
- **The Postgres auth used throughout local debugging (`sudo -u postgres
  psql`, peer auth) is a local-machine artifact of this dev box, not
  something the app itself relies on** — the app always connects via
  `DATABASE_URL` over TCP with a password, which is exactly how Railway's
  managed Postgres works. No code assumes peer auth.
- **No process manager / restart policy defined.** Locally this session ran
  the worker via a bare `nohup python -m src.runner &`, with manual restarts.
  Railway restarts a crashed service automatically, but the loop's own
  internal `try/except` around each cycle (see `README.md`'s health-check
  behavior) already prevents a single bad cycle from crashing the whole
  process — a full-process restart should only be needed for a config change,
  not routine operation.
- **Suspend/sleep is a local-machine-only risk** (see README "Known risk: host
  suspend on a local machine") — Railway's compute doesn't suspend the way a
  laptop or Crostini container does, so this specific failure mode goes away
  on a real deploy.
