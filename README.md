# mull

[![CI](https://github.com/NFhbar/mull/actions/workflows/ci.yml/badge.svg)](https://github.com/NFhbar/mull/actions/workflows/ci.yml)

A lightweight EVM log indexer. Polls an Ethereum-compatible JSON-RPC endpoint
for contract event logs and persists them to SQLite, resuming from a
checkpoint on restart.

Inspired by [Ponder](https://github.com/ponder-sh/ponder), scoped down to a
single Go binary.

## Quick start

```sh
go build -o mull .
cp mull.example.yaml mull.yaml   # edit RPC URL, contract, topics, start_block
./mull index --config mull.yaml
```

Stop with `Ctrl-C`; the next run resumes from the last persisted block.

Typed event decoding (per-event SQLite tables + Go structs) is opt-in via
`mull codegen` — see [Codegen (optional)](#codegen-optional) below.

## Codegen (optional)

`mull codegen` reads a contract ABI and emits, for each event, a typed Go
struct, a SQLite `CREATE TABLE` with typed columns, a decoder, and an
`EventSink` implementation. The indexer wires the sinks alongside the raw
`events` table — raw storage is preserved, typed tables are written in
addition.

**Lifecycle.** `abi_path` is a codegen *input*, not a runtime switch. The
indexer never consults `abi_path`; whether typed indexing is active is
determined entirely by the contents of the committed `internal/gen/`
package at build time. Workflow:

1. Set `abi_path:` in `mull.yaml` to your ABI JSON file.
2. Run `mull codegen` — overwrites files under `internal/gen/`.
3. Commit the regenerated files.
4. Next `go build && mull index` picks up typed sinks automatically.

**Invocation:**

```sh
./mull codegen --config mull.yaml --out internal/gen
./mull codegen --config mull.yaml --out internal/gen --alias myproject
```

`--out` defaults to `internal/gen`, resolved against the current working
directory.

`--alias` namespaces the generated SQL tables — `events_<alias>_<event>`.
Defaults to the ABI filename stem (`abi/foo.json` → `events_foo_<event>`).
Override when ingesting multiple contracts that share an event name, e.g.
two ERC-20s would both emit `events_<alias>_transfer` and collide without
distinct aliases.

**Caveats:**

- *v1 type coverage* — `address`, `bool`, `uintN/intN` (N ≤ 256), `bytes`,
  `bytesN` (1..32), `string`. Tuples and arrays are not yet supported;
  ABIs containing unsupported types fail at codegen with a clear error.
- *Schema regeneration* — `ApplySchema` runs `CREATE TABLE IF NOT EXISTS`
  on every `mull index` startup, which is correct for first-run and for
  *adding* new event tables to an existing deployment. It silently no-ops
  on shape changes, so if you regenerate an event with a different field
  set (e.g. ABI gains an indexed `nonce` arg) the typed table on disk
  keeps the old columns and the next matching event aborts the indexer
  with `no such column: <name>`. mull has no migration tool by design;
  drop or migrate the affected `events_<alias>_<event>` table manually
  before resuming, then `mull index` rebuilds it via codegen-emitted DDL.
- *Atomicity* — the committer goroutine writes raw events, runs each sink,
  then advances the checkpoint in separate transactions. If `mull index`
  crashes mid-chunk, the raw `events` row, the per-event typed rows, and
  the checkpoint can advance independently. On restart the chunk is
  replayed; every generated sink uses `INSERT OR IGNORE` on
  `(tx_hash, log_index)` so the final state converges, but a snapshot
  taken mid-crash may show transiently incomplete typed rows for that
  chunk. Decoders are pure functions of the input log, so replay
  reproduces the same rows exactly.
- *Per-event sink writes* — raw `events` rows for a chunk are saved in one
  batched transaction; typed-table inserts run one `INSERT OR IGNORE` per
  event per matching sink. On a high-volume contract with many indexed
  events per chunk, this is the dominant write cost vs. raw-only indexing.
  Acceptable for typical single-contract indexing; a `HandleBatch` interface
  or per-chunk `BEGIN/COMMIT` around dispatch would close the gap and is
  tracked for a follow-up.

## Configuration

`mull.yaml`:

```yaml
rpc_url: "https://ethereum-rpc.publicnode.com"
contract: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48" # USDC
topics:
  - "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" # Transfer
start_block: 19000000
chunk_size: 500 # blocks per eth_getLogs request
poll_interval: 5s # delay between head polls once caught up
db_path: "./mull.db"

# Optional — retry knobs for transient RPC errors (5xx, 429, network blips).
rpc_retry_base: 500ms # initial backoff before the first retry
rpc_retry_max_delay: 30s # cap on a single backoff window
rpc_retry_max_attempts: 5 # total attempts (including the original call)

# Optional — bounded worker pool for catch-up. Default 1 preserves sequential
# behaviour; 4 is a sane shared-RPC default; ceiling is 8.
concurrency: 1

# Optional — reorg detection horizon. The indexer keeps the last `reorg_depth`
# canonical block headers and walks parent-hash on each head reconcile to
# detect divergence. Once the cursor is within `reorg_depth` of head, a reorg
# shallower than this value is detected and rewound automatically. A reorg
# deeper than `reorg_depth` (detected when the cursor is within `reorg_depth`
# of head but the parent-hash walk can't find a common ancestor in that many
# steps) is logged and the indexer exits — raise this value and restart if you
# observe one. If the indexer was offline long enough that head outruns the
# stored block hashes by more than `reorg_depth`, the indexer silently
# re-anchors on the canonical head (logged as `re-anchoring on head`) —
# events indexed prior to the offline window are trusted; raise `reorg_depth`
# proportional to your expected downtime to keep this window narrow.
reorg_depth: 64
```

`chunk_size`, `poll_interval`, the `rpc_retry_*` keys, `concurrency`, and
`reorg_depth` have defaults (1000, 5s, 500ms, 30s, 5, 1, 64). The rest are
required.

`concurrency` interacts with the `rpc_retry_*` knobs: higher concurrency
multiplies the number of in-flight `eth_getLogs` calls hitting the RPC at
once. Public endpoints rate-limit aggressively, so the retry layer (with
`Retry-After` honoring) absorbs the resulting 429s — but if you raise
`concurrency` you should also size `rpc_retry_max_attempts` and
`rpc_retry_max_delay` to match. A sane default for a shared/public RPC is
`concurrency: 4`; a dedicated provider key tolerates the full ceiling of 8.

A poll cycle's effective wall-clock is now `poll_interval +
worst_case_retry_budget`. With the defaults above (`rpc_retry_max_attempts=5`,
`rpc_retry_max_delay=30s`), an unlucky chain of failures can stall a single
`eth_getLogs` call for up to ~2 minutes before the loop continues — operators
who need a tight head-to-tail cadence should size `rpc_retry_max_attempts` and
`rpc_retry_max_delay` accordingly. Servers returning a `Retry-After` header on
429 are honored (clamped to `rpc_retry_max_delay`) so the indexer respects
provider rate limits rather than retrying on a fixed schedule.

Public RPC endpoints rate-limit aggressively; `cloudflare-eth.com`,
`ethereum-rpc.publicnode.com`, and `rpc.ankr.com/eth` are all reasonable
free starting points. For sustained indexing use a provider key.

## Logging

Two persistent flags control log output:

```sh
./mull index --log-level=debug                  # debug|info|warn|error (default: info)
./mull index --log-format=json                  # text|json              (default: text)
```

`AddSource` (file:line) is auto-enabled at `debug` level. Logs go to stderr.

Each indexed chunk emits a line carrying `contract`, `from`, `to`, `events`,
`fetch_ms` (worker-side `eth_getLogs` time for this chunk), `commit_lag_ms`
(time the chunk waited in the committer for earlier chunks to land — always
0 at `concurrency: 1`, can be nonzero with parallel fetches), and
`lag_blocks` (head − to), so you can watch progress and catch-up rate:

```
time=… level=INFO msg="indexed range" contract=0xA0b8… from=19000000 to=19000499 events=4420 fetch_ms=17361 commit_lag_ms=0 lag_blocks=2134567
```

The `contract` field is bound once via `slog.Logger.With` rather than
re-passed at each call site.

## Inspecting the database

Quick sanity check from the shell:

```sh
sqlite3 mull.db "SELECT COUNT(*), MIN(block_number), MAX(block_number) FROM events"
sqlite3 mull.db "SELECT block_number FROM checkpoint"
```

For interactive exploration, any SQLite-aware GUI (TablePlus, DBeaver,
DB Browser for SQLite) can open `mull.db` directly — no driver setup.
Useful queries:

```sql
-- Busiest blocks
SELECT block_number, COUNT(*) AS n
FROM events
GROUP BY block_number
ORDER BY n DESC
LIMIT 20;

-- Latest events (raw)
SELECT block_number, tx_hash, log_index, topics, data
FROM events
ORDER BY block_number DESC, log_index DESC
LIMIT 50;
```

For ERC-20 `Transfer(address,address,uint256)` the raw layout is:
`topics[0]` = event signature hash, `topics[1]` / `topics[2]` =
from/to addresses (32-byte left-padded), `data` = amount (raw uint256 hex).

## Architecture

```
cmd/                   cobra commands (root, index) + logging setup
internal/
  config/              YAML load + validate + defaults
  rpc/                 JSON-RPC client (eth_blockNumber, eth_getLogs)
  store/               Store interface + SQLite impl (modernc.org/sqlite, pure Go)
  indexer/             Poll loop, chunked catch-up, checkpoint advance
```

The indexer is wired against `rpc.Client` and `store.Store` interfaces, so
both the network and the database are swappable in tests (see
`internal/indexer/indexer_test.go`).

The checkpoint stored in SQLite is **the next block to index**, so a
successful chunk `[from, to]` advances it to `to + 1`. Writes within a chunk
are transactional and use `INSERT OR IGNORE` on `(tx_hash, log_index)`, so
re-indexing the same range is idempotent.

## Development

```sh
go test ./...
go build ./...
```

## Roadmap

1. **Reorg handling.** _(correctness near head)_
   The indexer trusts the chain. Track recent block hashes and, when a
   parent hash mismatches at head, rewind the checkpoint and re-index
   the affected range. Historical backfills are unaffected; this only
   matters once the indexer is caught up and ingesting live blocks.

2. **Concurrent chunk fetches.** _(throughput)_
   At ~35 ms/event sequentially, backfilling years of mainnet history
   takes a long time. A bounded `errgroup` worker pool fetching N
   chunks in parallel with ordered checkpoint commits is the obvious
   perf lever — single biggest speedup available without changing the
   storage layer.

3. **ABI decoding.** _(data usability)_
   Logs are stored as raw topics + hex data. Either generic decoding
   driven by an ABI file in config, or codegen producing typed event
   structs per contract. Codegen is more idiomatic Go and gives type
   safety in any downstream consumer.

4. **`mull serve` query API.** _(completes the Ponder analogy)_
   A small HTTP/JSON endpoint exposing events with block-range and
   topic filters. Pairs naturally with ABI decoding once that lands.

5. **WebSocket subscriptions.** _(head latency)_
   `eth_subscribe` over WSS would replace head polling and cut latency
   from `poll_interval` down to sub-second. Lower priority — 5s polling
   is acceptable for most use cases, and getting subscription lifecycle
   - reconnect right adds nontrivial complexity.

6. **Multi-contract / multi-chain.** _(scope expansion)_
   Multiple indexer instances driven by a sources list in config, each
   with its own checkpoint. Requires reworking the config schema, the
   store layout (checkpoint per source), and the CLI to coordinate
   shutdown across goroutines.
