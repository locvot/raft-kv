# mini-kv (RaftKV)

A distributed, fault-tolerant key-value store written in Go — a
from-scratch Raft consensus implementation on top of an LSM-style storage engine. This is a personal project.

## Current status

| Milestone | Scope | Status |
|---|---|---|
| M0 | Project skeleton, module layout, `go.work` | ✅ |
| M1 | Concurrency-safe in-memory engine | ✅ |
| M2 | Durable storage layer (WAL + minimal LSM) | ✅ |
| M3 | Core Raft consensus | ✅ election + log replication + persistence + snapshot/log compaction (`raft/`) |
| M4 | 3-node cluster wiring over gRPC | ✅ real gRPC transport, leader redirect, `cmd/server`/`cmd/client` (`transport/`) |
| M5 | Fault-tolerance verification | ⬜ |
| M6 | Minimal observability (metrics/logging) | ⬜ |
| M7 | Real benchmarks & write-up | ⬜ |
| M8 | Finished README, architecture, design doc | ⬜ |

## Project layout

```
mini-kv/
├── engine/        # M1 — in-memory KV engine
├── storage/       # M2 — WAL + LSM
├── raft/          # M3 — Raft consensus
├── transport/     # M4 — gRPC service wrapping raft/storage
├── cmd/
│   ├── server/     # server process: flags -peers/-me/-dir
│   ├── client/     # client CLI: get/put/delete against a cluster
│   ├── enginecli/  # manual debug REPL for engine.Engine
│   └── storagecli/ # manual debug REPL for storage.Store
└── simharness/    # MIT 6.824/6.5840-style test rig (fake network, fake
                   # persister, N-peer config) — its own Go module, used
                   # via go.work, its go.mod is not modified
```

## Modules

- **Engine (M1)** — `engine.Engine` is a minimal `Get/Put/Delete`
  interface with explicit copy semantics. Five implementations
  (`MutexMap`, `ShardedMap`, `RWMutexMap`, `SyncMap`, `RCUShardedMap`)
  compare different concurrency-control strategies over the same
  interface.
- **Storage (M2)** — `storage.Store` is a durable, LSM-style key-value
  engine: a group-commit WAL, a skip-list memtable, immutable checksummed
  SSTables, an append-only manifest, and background size-tiered
  compaction.
- **Raft (M3)** — a from-scratch Raft implementation covering leader
  election, log replication with the Figure 2 conflict-backtracking
  optimization, persistence, and snapshot/log compaction with
  `InstallSnapshot`. Tested through `simharness`, an in-process fake
  network/persister test rig.
- **Transport (M4)** — wires `raft.Raft` and `storage.Store` into a real
  multi-process cluster over gRPC (`raftkvpb/`), with leader redirect on
  the client side. Peer membership is static, set at startup via `-peers`.
  Known gap: Raft's own persistent state still uses the in-memory
  `simharness` persister, so it does not survive a real process restart —
  only `storage.Store`'s data is durable across restarts today.

## Build, test, run

```bash
go build ./...
go vet ./...
go test ./... -race
```

Run a local 3-node cluster (three terminals):

```bash
go run ./cmd/server -me=0
go run ./cmd/server -me=1
go run ./cmd/server -me=2
```

```bash
go run ./cmd/client
> put hello world
OK
> get hello
"world"
```
