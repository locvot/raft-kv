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
| M5 | Fault-tolerance verification | ✅ leader-kill + node-isolation demo against real processes (`scripts/fault_tolerance_demo.sh`), `transport/server_test.go` |
| M6 | Minimal observability (metrics/logging) | ✅ Prometheus metrics endpoint per node, structured `log/slog` logging (`metrics/`, `transport/`, `cmd/server`) |
| M7 | Real benchmarks & write-up | ⬜ |
| M8 | Finished README, architecture, design doc | ⬜ |

## Project layout

```
mini-kv/
├── engine/        # M1 — in-memory KV engine
├── storage/       # M2 — WAL + LSM
├── raft/          # M3 — Raft consensus
├── transport/     # M4 — gRPC service wrapping raft/storage
├── metrics/       # M6 — Prometheus collectors, one private registry per node
├── cmd/
│   ├── server/         # server process: flags -peers/-me/-dir/-metrics-addr
│   ├── client/         # client CLI: get/put/delete against a cluster
│   ├── clusterstatus/  # M5: probes every peer's leader/follower state
│   ├── enginecli/      # manual debug REPL for engine.Engine
│   └── storagecli/     # manual debug REPL for storage.Store
├── scripts/
│   └── fault_tolerance_demo.sh # M5: kill-leader + isolate-node demo
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
- **Fault tolerance (M5)** — `transport/server_test.go` has two
  fault-injection tests: killing the leader mid-write, and isolating one
  follower via `transport.Server.SetPartitioned`, a network-partition
  simulation built entirely at the application layer (no root/iptables
  needed): it disables that node's own outbound Raft RPC ends and makes
  its inbound RaftInternal handlers refuse, in both directions, while its
  election/heartbeat ticker keeps running underneath.
  `scripts/fault_tolerance_demo.sh` runs both scenarios against three real
  `cmd/server` processes over real loopback gRPC and prints a transcript
  proving the cluster elects a new leader and keeps accepting writes, and
  that an isolated minority-of-one never declares itself leader.
- **Observability (M6)** — each node serves its own Prometheus metrics at
  `-metrics-addr` (default `localhost:9101+me`) `/metrics`: a
  `raftkv_request_duration_seconds` histogram and a
  `raftkv_requests_total{op,result}` counter for every Get/Put/Delete
  (`result` is `ok`, `not_leader`, or `error` — enough for p50/p99
  latency, throughput, and error rate via PromQL), plus a `raftkv_keys`
  gauge for the number of live keys currently stored. `metrics.Metrics`
  registers on a private `prometheus.Registry` per node rather than the
  global one, so multiple `transport.Server`s can coexist in one process
  (as the in-process test clusters in `transport/server_test.go` do)
  without colliding. Logging across `cmd/server` and `transport/` uses
  structured `log/slog`, tagged with the node index.

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

Scrape a node's metrics (node 0's default `-metrics-addr` is
`localhost:9101`):

```bash
curl localhost:9101/metrics
```

Watch fault tolerance in action — kill the leader mid-write, isolate a
node, and confirm the cluster keeps working — against three real
processes (no manual setup needed, the script starts and tears down its
own clusters):

```bash
scripts/fault_tolerance_demo.sh
```
