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

Five implementations to compare the contention/complexity trade-off:

- **`MutexMap`** — baseline: a single `sync.Mutex` guarding one
  `map[string][]byte`.
- **`ShardedMap`** — splits the keyspace into 128 shards by default,
  each with its own mutex, routed by FNV-1a hash — cuts lock contention
  under concurrent writes.
- **`RWMutexMap`** — a single `sync.RWMutex` guarding one map; readers
  take `RLock`, writers take `Lock`. Beats a plain `Mutex` (concurrent
  readers no longer serialize), but every `RLock`/`RUnlock` still does an
  atomic increment/decrement on one shared reader counter — under many
  cores that counter itself becomes a cache-line-bouncing bottleneck, so
  it loses badly to `ShardedMap` at every workload tested, including
  pure reads (see benchmark numbers below).
- **`SyncMap`** — wraps `sync.Map`. Fast on read-heavy/write-rare
  workloads (exactly the pattern `sync.Map` is optimized for), but
  slower than `ShardedMap` as the fraction of overwrites to existing
  keys grows (every overwrite falls into the internal mutex-guarded
  `dirty` map, losing the lock-free fast path).
- **`RCUShardedMap`** — read-copy-update per shard: readers do a plain
  `atomic.Pointer` load (no lock, no shared reader counter to contend
  on at all, unlike `RWMutexMap`); writers serialize per-shard and pay
  the cost of copying the *entire shard's map* on every Put/Delete. Ties
  `ShardedMap` on pure reads (genuinely zero read-side contention), but
  loses badly as the write fraction grows — the O(shard size) copy per
  write makes it the slowest of all five once writes dominate.

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

**Measured numbers** (Intel i7-11700, 16 cores, `-count=8` + `benchstat`,
keyspace=1000):

| Engine | writepct=0 | writepct=10 | writepct=50 | writepct=90 |
|---|---|---|---|---|
| MutexMap | 105.6n | 135.6n | 173.1n | 210.0n |
| ShardedMap | 17.71n | 24.62n | 29.15n | 31.59n |
| RWMutexMap | 42.16n | 117.9n | 126.8n | 150.9n |
| RCUShardedMap | 17.86n | 35.05n | 129.6n | 287.8n |
| SyncMap | 18.44n | 25.75n | 44.47n | 65.16n |

Takeaway: sharding clearly beats a single mutex at every workload.
`RWMutexMap` beats a plain mutex but never catches `ShardedMap` — a CPU
profile at writepct=0 shows **67.7%** of total CPU time inside
`sync/atomic.(*Int32).Add`, i.e. the shared reader-count increment on
every `RLock`/`RUnlock`, which bounces across all 16 cores' caches even
though nothing is actually being written. `RCUShardedMap` (per-shard
`atomic.Pointer` copy-on-write) is the mirror image: it *ties*
`ShardedMap` on pure reads — a CPU profile at writepct=0 shows no
lock/atomic hotspot at all, just the benchmark's own key-formatting cost
— because a reader is a single pointer load with no shared mutable state
to contend on. But every write copies the entire shard's map, so it gets
worse than `MutexMap` past ~50% writes. `sync.Map` only competes when
reads dominate writes, and does noticeably worse once overwrites of
existing keys become frequent — which matches how a real KV store is
actually used. See [`doc/knowledge.md`](doc/knowledge.md) (Vietnamese)
for the full methodology, more workloads (keyspace/value-size sweeps),
and the profiling evidence behind each conclusion.

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
