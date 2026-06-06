# Migrating mull v1 → v2

mull v2 adds **multi-source indexing**: one binary can index N contracts
across N chains into one SQLite database. The v2 schema reshapes the three
core tables (`events`, `checkpoint`, `block_hashes`) so each row carries a
`source` column, and the config grows a top-level `sources:` list.

This document covers everything that changes between v1 and v2, and how to
upgrade an existing deployment.

## TL;DR

1. **Back up `mull.db`** (and any typed-event tables produced by
   `mull codegen`).
2. Run `mull migrate --config mull.yaml` — rewrites the schema in place,
   stamps every existing row with `source = "default"`.
3. Decide on your config shape:
   - **Stay single-source:** leave the existing `rpc_url:` / `contract:` /
     `topics:` / `start_block:` keys in `mull.yaml`. The legacy shim wraps
     them as a synthetic `default` source at boot and emits a one-time INFO
     log. Nothing else changes.
   - **Go multi-source:** replace the legacy top-level keys with a
     `sources:` list (see "Config shape" below). Each entry is its own
     source; the indexer spins up one goroutine per entry under a shared
     `errgroup`.
4. **If you use `mull codegen`:** drop your existing `internal/gen/`
   typed-event tables and re-run `mull codegen`, then re-index from your
   desired start block (or reset the per-source checkpoint to a known
   earlier block to re-populate). The migration intentionally does NOT
   touch codegen output — that schema is operator-controlled.
5. **`mull serve` clients:** `/checkpoint` now always returns
   `{"checkpoints": {<src>: <n>, …}}`. The v1 `{"checkpoint": <n>}` shape
   is gone. Clients should read `body.checkpoints[<source_name>]`.

## Schema delta

The schema bump is `user_version` 0 → 2. `mull migrate` enforces this; an
unmigrated v1 database opened by `mull index` / `mull serve` fails with:

```
database "./mull.db" is on schema v1; run 'mull migrate --config mull.yaml' to upgrade to v2
```

The three tables change as follows:

| Table | v1 PK | v2 PK | Notes |
|---|---|---|---|
| `events` | `(tx_hash, log_index)` | `(source, tx_hash, log_index)` | `source TEXT NOT NULL` column added; `idx_events_block` replaced by `idx_events_source_block (source, block_number)` |
| `checkpoint` | `id INTEGER PRIMARY KEY CHECK (id = 1)` | `source TEXT PRIMARY KEY` | one row per source |
| `block_hashes` | `block_number INTEGER PRIMARY KEY` | `(source, block_number)` | reorg detection scoped per source |

All existing rows are stamped `source = 'default'` during migration —
matches the legacy-config shim so a single-source `mull.yaml` keeps
working unchanged after migrate.

The migration runs inside a single `BEGIN IMMEDIATE … COMMIT` transaction.
Failure mid-sequence rolls back automatically; the on-disk file is
unchanged. `mull migrate` is idempotent — re-running on an already-v2
database is a no-op.

## Config shape

### Single-source (legacy — still works via shim)

```yaml
rpc_url: https://cloudflare-eth.com
contract: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"  # USDC
topics: ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]
start_block: 19000000
db_path: ./mull.db
```

This loads as one source named `default`. At boot you'll see:

```
INFO legacy single-source config detected; wrapped as source name="default". See MIGRATION.md for the multi-source schema.
```

### Multi-source (recommended)

```yaml
db_path: ./mull.db
poll_interval: 5s
concurrency: 2      # global — applies to each source's catch-up pool
reorg_depth: 64

sources:
  - name: usdc_mainnet
    rpc_url: https://eth-mainnet.example
    contract: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    topics: ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]
    start_block: 19000000

  - name: usdc_arbitrum
    rpc_url: https://arb-mainnet.example
    contract: "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
    topics: ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]
    start_block: 200000000
```

Rules:

- **`name` is required.** Charset is `[a-z0-9_-]{1,64}` — narrow on purpose
  because the name appears in log structured fields, cursor payloads, and
  may end up as a table-name segment later.
- **Duplicate names are rejected** (`duplicate source name "main"`).
- **Mixed shape is rejected** — you can't have BOTH `sources:` and any
  legacy top-level fields (`rpc_url`, `contract`, …). Pick one.
- Per-source fields: `name`, `rpc_url`, `ws_rpc_url`, `contract`,
  `topics`, `start_block`, `chunk_size` (default 1000), `abi_path`,
  `head_source` (default `auto`), `head_source_fallback_after`
  (default 30s).
- Global fields stay at the top level: `db_path`, `poll_interval`,
  `rpc_retry_*`, `concurrency`, `reorg_depth`.

### High RPC pressure warning

If `len(sources) * concurrency > 16`, `validate()` emits a one-time WARN
log at boot:

```
WARN high aggregate RPC pressure: len(sources) * concurrency exceeds 16; consider lowering concurrency sources=5 concurrency=4 product=20
```

`concurrency` stays global in v2 (per-source concurrency is a follow-up).
Operators tune it down when running many sources.

## `mull serve` API changes

### `/checkpoint` — breaking shape change

v1:

```json
{ "checkpoint": 19000050 }
```

v2 (always — regardless of `?source=`):

```json
{ "checkpoints": { "usdc_mainnet": 19000050, "usdc_arbitrum": 200000123 } }
```

`/checkpoint?source=usdc_mainnet` returns the same shape with a single key:

```json
{ "checkpoints": { "usdc_mainnet": 19000050 } }
```

Migrate clients with a one-line read change: `body.checkpoint` →
`body.checkpoints[source_name]`.

### `/events` — new `?source=` filter

`GET /events?source=usdc_arbitrum` scopes the query to one source. Omit to
page across every source — `/events` (no source) returns events from all
sources in deterministic `(block_number, log_index, source)` order.

### `/events` cursor — backward compatible, source-aware

The cursor payload is base64-of-JSON. v1 payloads are `{"b": <n>, "l": <n>}`;
v2 adds an `s` (source) field: `{"b": <n>, "l": <n>, "s": "<src>"}`.

A v2 server accepts v1 cursors gracefully — `s` decodes as `""` and sorts
strictly before any real source name. Paging from a legacy cursor
completes deterministically; **one event may re-emit at the transition
boundary** when both pre- and post-upgrade clients page through the same
data. Refresh your cursor to v2 shape after the upgrade if exact-once
semantics matter for the transition page.

## Codegen and typed-event tables

Codegen output (`internal/gen/`) lives outside the migrator's knowledge —
operators control the per-event schema by re-running `mull codegen`. The
v2 codegen templates emit a `source TEXT NOT NULL` column on every typed
table with PK `(source, tx_hash, log_index)`.

**If you have existing populated typed tables:**

1. Drop them: `DROP TABLE events_<alias>_<event>;` for each generated
   table you have.
2. Re-run `mull codegen` against your ABI — the regenerated `gen/`
   package emits the v2 shape.
3. Re-index from your desired start block — either let mull re-discover
   the full history, or reset the per-source `checkpoint` to a known
   earlier block to re-populate from there.

For a mainnet contract with months of history, this is a non-trivial
operational cost. The alternative — leaving the v1-shaped typed tables in
place — would mean the source column is absent from typed reads and
cross-chain queries from typed tables would be ambiguous.

## Store interface changes (for Go consumers)

`store.Store` adds a `source string` parameter to every method that
touches per-source state: `SaveEvents`, `Checkpoint`, `SetCheckpoint`,
`Checkpoints` (new), `RecordBlockHash`, `RecentBlockHashes`,
`BlockHashAt`, `RewindTo`. `Query` does not gain a bare param — instead
`QueryFilter.Source *string` follows the existing pointer-tri-state
convention. `Event` gains `Source string`. `EventSink.RewindTo` now takes
a `source string` parameter.

mull is a single-binary repo with no external Go consumers (`internal/`
enforces this at the language level), so this is a documentation note
rather than a breaking change for the world.
