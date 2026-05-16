# mull

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
```

`chunk_size` and `poll_interval` have defaults (1000, 5s). The rest are required.

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
`took_ms`, and `lag_blocks` (head − to), so you can watch progress and
catch-up rate:

```
time=… level=INFO msg="indexed range" contract=0xA0b8… from=19000000 to=19000499 events=4420 took_ms=17361 lag_blocks=2134567
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

1. **Retries with exponential backoff.** _(blocks unattended use)_
   A single transient RPC error (5xx, rate-limit, network blip) currently
   terminates the run. A `retry` helper in `internal/rpc` with
   context-aware exponential backoff and a bounded attempt budget is
   the highest-leverage addition: small change, eliminates the most
   common failure mode.

2. **Reorg handling.** _(correctness near head)_
   The indexer trusts the chain. Track recent block hashes and, when a
   parent hash mismatches at head, rewind the checkpoint and re-index
   the affected range. Historical backfills are unaffected; this only
   matters once the indexer is caught up and ingesting live blocks.

3. **Concurrent chunk fetches.** _(throughput)_
   At ~35 ms/event sequentially, backfilling years of mainnet history
   takes a long time. A bounded `errgroup` worker pool fetching N
   chunks in parallel with ordered checkpoint commits is the obvious
   perf lever — single biggest speedup available without changing the
   storage layer.

4. **ABI decoding.** _(data usability)_
   Logs are stored as raw topics + hex data. Either generic decoding
   driven by an ABI file in config, or codegen producing typed event
   structs per contract. Codegen is more idiomatic Go and gives type
   safety in any downstream consumer.

5. **`mull serve` query API.** _(completes the Ponder analogy)_
   A small HTTP/JSON endpoint exposing events with block-range and
   topic filters. Pairs naturally with ABI decoding once that lands.

6. **WebSocket subscriptions.** _(head latency)_
   `eth_subscribe` over WSS would replace head polling and cut latency
   from `poll_interval` down to sub-second. Lower priority — 5s polling
   is acceptable for most use cases, and getting subscription lifecycle
   - reconnect right adds nontrivial complexity.

7. **Multi-contract / multi-chain.** _(scope expansion)_
   Multiple indexer instances driven by a sources list in config, each
   with its own checkpoint. Requires reworking the config schema, the
   store layout (checkpoint per source), and the CLI to coordinate
   shutdown across goroutines.
