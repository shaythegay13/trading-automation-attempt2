# Project Status

Last touched: 2026-07-25, with a packaging pass on 2026-10-14. This file is the
"what was I doing" recap — read this before you read code. `README.md` is the
architecture/config reference; `DEPLOY.md` is Railway deployment planning
(not executed).

The 2026-10-14 pass added `requirements.txt`, `pyproject.toml`, GitHub Actions
CI, and an explicit read-only startup guard (`LIVE_TRADING_ENABLED` defaults
off and refuses to start if set, because there is still no order client).
Ingestion behavior was not changed.

## Is anything running right now?

**No.** The continuous loop (`python -m src.runner`) was explicitly stopped at
the end of the last session. Nothing is ingesting. To resume:

```bash
docker-compose up -d        # or: use your local Postgres
alembic upgrade head         # only if there are new migrations since you last ran
python -m src.runner         # continuous loop, all three venues/tiers
```

Data already collected is sitting in Postgres from earlier sessions — check
`SELECT max(captured_at) FROM market_snapshots;` to see how stale it is before
assuming it's current.

## The shape of the system, in one paragraph

Two venues, three independent async loops. Kalshi is tiered: **Tier-1** polls
`/events` every 15 min for top-of-book quotes across (as of this session)
~40,000 non-Sports markets; **Tier-2** polls `/orderbook` every 5 min for
depth, but only for the top 250 (by `volume_24h`) of whatever passes the
two-sided gate, because the gate alone returns 20,000-38,000 markets and
orderbook fetches are one HTTP request per market at 2 requests/sec — trying
to fetch all of them takes ~3 hours, not 5 minutes. Polymarket runs a simpler
single-tier 5-min loop (Gamma metadata + CLOB book per market, no tiering).

## Decisions made this session, and why (read this before changing config)

These aren't arbitrary defaults — each one came out of an investigation this
session, usually because the original assumption turned out wrong. If you're
tempted to "clean up" one of these, re-derive whether the reason still holds
first.

- **`resolution_source` comes from the event's `settlement_sources` array, not
  the market object.** Kalshi's nested market objects have no source/URL
  field at all; the source lives one level up, on the parent event, as
  `[{"name": ..., "url": ...}]` (sometimes one entry, sometimes 20+). We join
  them into one string. `resolution_rules_text` was already correctly pulling
  from `rules_primary`/`rules_secondary` — that part was never broken.

- **Change-detection key is `(bid, ask, status)` — deliberately NOT
  `volume_24h` or `open_interest`.** The original instinct was that
  `volume_24h`'s continuous decay was forcing near-every-cycle writes.
  Measured: it was a real but *secondary* contributor (~15% of the noise).
  The dominant driver (~83% of writes, pre-fix) is genuine price movement —
  a large fraction of Kalshi's market universe is live sports/esports
  contracts repricing every 15 minutes. `volume_24h`/`open_interest` are
  still stored on every written row; they're just excluded from the
  comparison that decides *whether* to write.

- **Cadence tiering only slows down "no quote on either side" markets (6h
  heartbeat), not one-sided ones.** One-sided markets (bid XOR ask — a book
  that's forming but not yet crossed) stay on the full 15-min cadence
  on purpose: "that's the pre-liquidity history I want." Don't fold
  one-sided into the slow tier without checking whether that's still true.

- **`raw_market` is dropped entirely from Tier-1 snapshot rows — this was the
  actual storage lever, not the change-detection key.** Measured before/after
  on the real table: 319MB → 52MB, avg tier-1 row 1,540 → 156 bytes. The
  `markets` table already stores the same payload once per ticker
  (deduplicated); storing it again on every 15-minute snapshot bought
  nothing. Tier-2 rows still keep `raw_market` + `raw_orderbook` — that IS
  genuinely per-capture data.

- **SQLAlchemy JSONB columns need `none_as_null=True`, or a Python `None`
  silently becomes the JSON scalar `'null'` (4 bytes), not a real SQL
  `NULL`.** This bit twice — once as ~34 historical rows nobody had
  noticed, once again at full scale the moment the `raw_market=None` change
  above went live, because the model didn't have the flag set. Fixed on all
  four nullable JSONB columns (`Market.price_ranges`, `Market.raw_market`,
  `MarketSnapshot.raw_orderbook`, `MarketSnapshot.raw_market`). If you add a
  new nullable JSONB column later, set this flag on it too or you'll
  reproduce the exact same bug.

- **`Sports` is excluded from Tier-1 by default, matched on `event.category`
  — not a ticker prefix.** There is no clean prefix: sports series tickers
  (`KXNFLWINS`, `KXMLBHRR`, `KXCS2MAP`, ...) share nothing but the universal
  `KX` every category uses. Esports is filed under `Sports` too (confirmed:
  `KXCS2MAP` shows up in the Sports category list), so excluding `Sports`
  covers both without a separate esports rule. This excludes ~45% of markets
  and ~83% of pre-fix write volume — deliberately, because it's sports
  repricing that isn't going to be traded. `Entertainment` (awards,
  box-office — "exactly the kind of thing with a modelable base rate") was
  explicitly **not** excluded; don't add it without asking.

- **Tier-2's gate query used to always return zero markets in continuous
  mode.** It filtered on `captured_at = this cycle's own timestamp`, which
  can never match once Tier-1 and Tier-2 run as independent loops with
  independently-generated timestamps (it only ever worked in
  `--single-run`, where both tiers share one timestamp by construction).
  This is why Tier-2 had ~564 rows total before this session, from
  single-run test invocations only. Fixed by gating on each market's most
  recent snapshot, regardless of when it was captured. This is also why the
  cap below exists — fixing this bug immediately revealed 20,000+ gated
  markets that the bug had been hiding.

- **`MIN_VOLUME_FOR_TIER2` is `0` (no floor), and the real bound is
  `TIER2_MAX_MARKETS_PER_CYCLE` (250, ranked by `volume_24h` DESC).**
  Deliberate: "shrinking the gate is treating a bounded-resource problem as
  a selectivity problem, and it recurs the moment more markets develop
  books." A volume floor would permanently exclude thin/forming markets;
  the cap bounds cycle wall-clock without excluding anything on principle.
  Tier-2 also has a hard per-cycle timeout (`asyncio.wait_for`) as a
  backstop — a cycle that somehow still runs long gets abandoned rather
  than blocking the next one.

- **Storage: measured, not assumed.** Real per-row size was checked directly
  via `pg_column_size`, not estimated — the original "~200 bytes/row"
  assumption was off by ~9x before the `raw_market` fix. Projected 30-day
  storage at the current config (key `(bid,ask,status)`, cadence tiering,
  Sports excluded, `raw_market` off Tier-1): roughly **19-30GB/month**,
  depending on real market activity — re-measure before trusting an old
  number, market count and volatility both drift over time.

## Known-safe non-issues (don't re-investigate these)

- **The loop can go silent for 10+ minutes with zero log activity, all three
  sub-loops resuming in lockstep, no errors.** This is the host (a
  Linux-on-Chromebook/Crostini container, in this session) suspending —
  confirmed via `journalctl` (`maitred: Received request to prepare to
  suspend`). Not a code bug. See README's "Known risk: host suspend."
  Deploying somewhere that doesn't suspend (see `DEPLOY.md`) makes this
  moot.

## Explicitly NOT done — don't assume these exist

- **A dependency manifest now exists** (`requirements.txt`, loaded by
  `pyproject.toml`). It was missing for the whole July session and blocked
  any real deploy. Installing is `pip install .` (or `pip install -e ".[dev]"`
  for tests and lint). What is still not wired is a Railway release command
  for migrations — see `DEPLOY.md`.
- **Retention (`KALSHI_RETENTION_MONTHS`) is implemented but has never been
  turned on or tested against a real old partition.** It's wired to drop
  partitions by calendar age, unconditional on market resolution status —
  correct by design, but `0` (disabled) is still the only value that's ever
  actually run.
- **Kalshi's WebSocket orderbook-delta API was researched, not built.** It's
  the real fix for Tier-2's scaling problem (persistent subscription instead
  of polling thousands of tickers one at a time), and the access path looks
  open (self-serve API key, no tier gate found in the docs) — but this
  project currently makes zero authenticated Kalshi requests, so it needs an
  actual API key plus RSA request-signing implemented from scratch, plus a
  genuinely different ingestion architecture (persistent connection,
  sequence-gap/reconnect handling). Treat it as a separate project, not a
  quick swap.
- **Nothing has actually been deployed anywhere.** `DEPLOY.md` is planning
  notes for Railway only.

## If something looks wrong when you come back

- **Row counts look way off (too high or zero):** check the Tier-2 log lines
  first (`gate_size` / `fetched` / `skipped_by_cap`) — they tell you exactly
  what the gate and cap are doing each cycle. For Tier-1, check the
  `save_snapshots_to_db: skipped N unchanged tier-1 rows` line.
- **Storage growing faster than expected:** confirm `raw_market IS NULL` for
  `depth_fetched = false` rows in `market_snapshots` — if it's coming back
  populated, something reintroduced the per-row duplication this session
  removed.
- **A cycle seems stuck / interval blown past:** the health check logs an
  `ERROR` for zero-row cycles and over-interval cycles, and Tier-2 has a hard
  timeout backstop — check the log for those before assuming something's
  hung.
