# mini-kv (RaftKV)

A distributed, fault-tolerant key-value store written in Go — a
from-scratch Raft consensus implementation on top of an LSM-style storage engine. This is a personal
project. 

## Current status

| Milestone | Scope | Status |
|---|---|---|
| M0 | Project skeleton, module layout, `go.work` | ✅ |
| M1 | Concurrency-safe in-memory engine | ✅ |
| M2 | Durable storage layer (WAL + minimal LSM) | ⬜ not started (`storage/`) |
| M3 | Core Raft consensus | ⬜ not started (`raft/`) |
| M4 | 3-node cluster wiring over gRPC | ⬜ not started (`transport/`, `cmd/server`, `cmd/client`) |
| M5 | Fault-tolerance verification | ⬜ |
| M6 | Minimal observability (metrics/logging) | ⬜ |
| M7 | Real benchmarks & write-up | ⬜ |
| M8 | Finished README, architecture, design doc | ⬜ |

## Project layout

```
mini-kv/
├── engine/        # M1 — in-memory KV engine (done, see below)
├── storage/       # M2 — WAL + LSM (not implemented yet)
├── raft/          # M3 — Raft consensus (not implemented yet)
├── transport/     # M4 — gRPC service wrapping raft/engine/storage (not implemented yet)
├── cmd/
│   ├── server/    # M4 — server process (placeholder)
│   ├── client/    # M4 — client CLI (placeholder)
│   └── enginecli/ # manual debug REPL for engine.Engine, not part of the M0–M8 chain
├── simharness/    # MIT 6.824/6.5840-style test rig (fake network, fake
│                  # persister, N-peer config) — its own Go module, used
│                  # via go.work, its go.mod is not modified
└── doc/raftkv.plan.md  # detailed technical plan per milestone (Vietnamese)
```

## Engine (M1)

`engine.Engine` is a minimal `Get/Put/Delete` interface with explicit
copy semantics (Put copies its input, Get returns a copy — a caller
mutating either slice can never touch internal state or race with it).

Three implementations to compare the contention/complexity trade-off:

- **`MutexMap`** — baseline: a single `sync.Mutex` guarding one
  `map[string][]byte`.
- **`ShardedMap`** — splits the keyspace into 128 shards by default,
  each with its own mutex, routed by FNV-1a hash — cuts lock contention
  under concurrent writes.
- **`SyncMap`** — wraps `sync.Map`. Fast on read-heavy/write-rare
  workloads (exactly the pattern `sync.Map` is optimized for), but
  slower than `ShardedMap` as the fraction of overwrites to existing
  keys grows (every overwrite falls into the internal mutex-guarded
  `dirty` map, losing the lock-free fast path).

### Running tests

```bash
go test ./engine/... -race
```

### Benchmarks

`benchmarkEngine` spreads iterations across GOMAXPROCS goroutines
hammering a small shared keyspace so reads/writes actually collide on
the same keys/shards, instead of comparing implementations on disjoint
keys (where they'd all look equally fast).

```bash
go test ./engine/... -bench=. -run=^$ -benchmem
```

Flags to tune the workload:

| Flag | Default | Meaning |
|---|---|---|
| `-keyspace` | 1000 | number of distinct keys used in the benchmark |
| `-valuesize` | 1 | size in bytes of the value written on every Put |
| `-writepct` | 10 | % of operations that are Put (the rest are Get) |

Example: write-heavy workload (50/50 Put/Get) with a larger keyspace:

```bash
go test ./engine/... -bench=. -run=^$ -benchmem -keyspace=100000 -valuesize=256 -writepct=50
```

**Measured numbers** (Intel i7-11700, default `writepct=10`, keyspace=1000):

| Engine | ns/op | B/op | allocs/op |
|---|---|---|---|
| MutexMap | 142.5 | 14 | 1 |
| ShardedMap | 18.74 | 14 | 1 |
| SyncMap | 18.95 | 23 | 2 |

Same configuration but `writepct=50`:

| Engine | ns/op | B/op | allocs/op |
|---|---|---|---|
| MutexMap | 178.9 | 17 | 2 |
| ShardedMap | 22.87 | 17 | 2 |
| SyncMap | 33.03 | 62 | 3 |

Takeaway: sharding clearly beats a single mutex at every workload;
`sync.Map` only competes when reads dominate writes, and does
noticeably worse once overwrites of existing keys become frequent —
which matches how a real KV store is actually used.

### Profiling

The benchmark numbers above say *what* is faster; profiling says *why*.

**CPU profile** — where time is actually spent:
```bash
go test ./engine/... -bench=BenchmarkMutexMap -run=^$ -cpuprofile=cpu.out
go tool pprof -top cpu.out
```
For `MutexMap` this shows most time inside `sync.(*Mutex).Lock` /
`runtime.lock2` — direct evidence for why it's ~8x slower than
`ShardedMap`, instead of just assuming "probably contention".

**Memory profile** — where allocations come from:
```bash
go test ./engine/... -bench=BenchmarkSyncMap -run=^$ -memprofile=mem.out
go tool pprof -alloc_objects -top mem.out
```
Pinpoints exactly which line allocates — e.g. the mandatory
`append([]byte(nil), value...)` copy, plus `sync.Map`'s extra boxing of
`[]byte` into `interface{}`, explaining its higher allocs/op.

**Mutex/block profile** — lock contention measured directly, isolating
time spent *waiting* for a lock from time spent *running*:
```bash
go test ./engine/... -bench=BenchmarkMutexMap -run=^$ -mutexprofile=mutex.out -blockprofile=block.out
go tool pprof -top mutex.out
```

**GC trace** — whether the mandatory copy-on-every-Put/Get semantics
create meaningful GC pressure under load:
```bash
GODEBUG=gctrace=1 go test ./engine/... -bench=. -run=^$ -benchtime=3s 2>&1 | grep gc
```

Other measurements worth knowing about:

- **Escape analysis** (`go build -gcflags="-m" ./engine/...`) — shows
  which values are forced onto the heap at compile time, without
  running anything. Useful for spotting the interface-boxing cost in
  `SyncMap` before it even shows up in a memory profile.
- **`go tool trace`** — visualizes the scheduler timeline: goroutine
  run/block/wait states, GC STW pauses, syscalls. More useful once
  there's a real client/server (M4+) and p50/p99 tail latency matters
  (M7) — a CPU profile shows aggregate hot spots but not *when* a GC
  pause or scheduling delay spiked one particular request's latency.
  ```bash
  go test ./engine/... -bench=. -run=^$ -trace=trace.out
  go tool trace trace.out
  ```
- **`benchstat`** — statistical comparison between two benchmark runs,
  so a claimed improvement isn't just run-to-run noise:
  ```bash
  go test ./engine/... -bench=. -run=^$ -count=10 > old.txt
  # ... make a change ...
  go test ./engine/... -bench=. -run=^$ -count=10 > new.txt
  go run golang.org/x/perf/cmd/benchstat@latest old.txt new.txt
  ```
- **`pprof -http`** — same profiles as above but as an interactive
  flame graph / call graph in the browser instead of a text table:
  ```bash
  go tool pprof -http=:8080 cpu.out
  ```

## Verifying the build

```bash
go build ./...
go vet ./...
go test ./... -race
```

Once M3 (Raft) is underway, run tests repeatedly to catch
timing-dependent bugs:

```bash
go test ./raft/... -race -count=20
```
