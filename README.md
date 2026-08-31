# mini-kv (RaftKV)

A distributed, fault-tolerant key-value store written in Go — a
from-scratch Raft consensus implementation on top of an LSM-style storage engine. This is a personal project. 

## Current status

| Milestone | Scope | Status |
|---|---|---|
| M0 | Project skeleton, module layout, `go.work` | ✅ |
| M1 | Concurrency-safe in-memory engine | ✅ |
| M2 | Durable storage layer (WAL + minimal LSM) | ✅ |
| M3 | Core Raft consensus | 🚧 in progress — leader election done (`raft/`) |
| M4 | 3-node cluster wiring over gRPC | ⬜ not started (`transport/`, `cmd/server`, `cmd/client`) |
| M5 | Fault-tolerance verification | ⬜ |
| M6 | Minimal observability (metrics/logging) | ⬜ |
| M7 | Real benchmarks & write-up | ⬜ |
| M8 | Finished README, architecture, design doc | ⬜ |

## Project layout

```
mini-kv/
├── engine/        # M1 — in-memory KV engine (done, see below)
├── storage/       # M2 — WAL + LSM (done, see below)
├── raft/          # M3 — Raft consensus: leader election done, log
│                  #      replication/persistence/snapshot pending
├── transport/     # M4 — gRPC service wrapping raft/engine/storage (not implemented yet)
├── cmd/
│   ├── server/     # M4 — server process (placeholder)
│   ├── client/     # M4 — client CLI (placeholder)
│   ├── enginecli/  # manual debug REPL for engine.Engine, not part of the M0–M8 chain
│   └── storagecli/ # manual debug REPL for storage.Store, not part of the M0–M8 chain
└── simharness/    # MIT 6.824/6.5840-style test rig (fake network, fake
                   # persister, N-peer config) — its own Go module, used
                   # via go.work, its go.mod is not modified
```

## Engine (M1)

`engine.Engine` is a minimal `Get/Put/Delete` interface with explicit
copy semantics (Put copies its input, Get returns a copy — a caller
mutating either slice can never touch internal state or race with it).

Five implementations, comparing different concurrency-control strategies
over the same interface:

- **`MutexMap`** — a single `sync.Mutex` guarding one `map[string][]byte`.
- **`ShardedMap`** — splits the keyspace into 128 shards by default, each
  with its own mutex, routed by FNV-1a hash.
- **`RWMutexMap`** — a single `sync.RWMutex` guarding one map; readers
  take `RLock`, writers take `Lock`.
- **`SyncMap`** — wraps `sync.Map`.
- **`RCUShardedMap`** — read-copy-update per shard: readers do a plain
  `atomic.Pointer` load; writers copy-modify-store the shard's map.

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

Statistical comparison across runs (so a claimed improvement isn't just
run-to-run noise):

```bash
go test ./engine/... -bench=. -run=^$ -benchmem -count=8 -writepct=<0|10|50|90> > out.txt
go run golang.org/x/perf/cmd/benchstat@latest out.txt
```

### Profiling

```bash
# CPU profile — where time is actually spent
go test ./engine/... -bench=BenchmarkMutexMap -run=^$ -cpuprofile=cpu.out
go tool pprof -top cpu.out

# Memory profile — where allocations come from
go test ./engine/... -bench=BenchmarkSyncMap -run=^$ -memprofile=mem.out
go tool pprof -alloc_objects -top mem.out

# Mutex/block profile — time spent waiting for a lock vs. running
go test ./engine/... -bench=BenchmarkMutexMap -run=^$ -mutexprofile=mutex.out -blockprofile=block.out
go tool pprof -top mutex.out

# GC trace
GODEBUG=gctrace=1 go test ./engine/... -bench=. -run=^$ -benchtime=3s 2>&1 | grep gc
```

Other measurements worth knowing about:

- **Escape analysis**: `go build -gcflags="-m" ./engine/...` — shows
  which values are forced onto the heap at compile time, without running
  anything.
- **`go tool trace`**: visualizes the scheduler timeline (goroutine
  run/block/wait states, GC STW pauses, syscalls).
  ```bash
  go test ./engine/... -bench=. -run=^$ -trace=trace.out
  go tool trace trace.out
  ```
- **`pprof -http`**: same profiles as above but as an interactive flame
  graph / call graph in the browser.
  ```bash
  go tool pprof -http=:8080 cpu.out
  ```

## Storage (M2)

`storage.Store` is a durable, LSM-style key-value engine, independent of
`engine.Engine` and of Raft's `simharness/persister` — it stands on its own
under `storage/`, backed by a real on-disk write path:

- **WAL** (`wal.go`) — every `Put`/`Delete` is appended and fsynced before
  `Store` touches the memtable or returns to the caller, so an
  acknowledged write survives a crash even before it's ever flushed. Uses
  group commit: concurrent callers batch into one shared `fsync` instead
  of each paying for their own.
- **Memtable** (`memtable_skiplist.go`) — a mutex-guarded skip list is
  what `Store` uses; a sorted-slice-plus-binary-search alternative
  (`memtable.go`) also lives in the tree as a measured comparison point.
- **SSTable** (`sstable.go`) — an immutable, checksummed on-disk format:
  `[block]* + index-block + footer`, each block prefixed with a CRC32
  checksum. A corrupted block is reported as `storage.ErrCorruptBlock`,
  never silently treated as a miss or a wrong value.
- **Manifest** (`manifest.go`) — an append-only ADD/DEL log of which
  SSTable files are currently live, so a crash mid-compaction leaves an
  unambiguous, recoverable trail instead of a directory listing to guess
  from.
- **Compaction** (`compaction.go`) — size-tiered, running on its own
  background goroutine so it never blocks the write path.
- **`Store`** (`store.go`) — ties it together: `Open`, `Get`, `Put`,
  `Delete`, `Close`, plus crash recovery.

### Manual REPL (`cmd/storagecli`)

A throwaway REPL for poking at a real `storage.Store` by hand — same idea
as `cmd/enginecli` for M1, except data here is **real, disk-backed, and
survives between runs** (that's the whole point of M2):

```bash
go run ./cmd/storagecli
> put hello world
OK
> get hello
"world"
> quit
```

Run it again (same default `-dir`, or pass your own) with no `put` at
all — `get hello` still returns `"world"`, recovered from the WAL/SSTable
files the previous run left on disk. Kill it with Ctrl+C instead of
`quit` to simulate a real crash (nothing is lost either way — every
`Put`/`Delete` is WAL-fsynced before it returns). Pass `-flushthreshold=1`
to force a flush after every single `put`/`delete` and watch
`sst-*.sst`/`MANIFEST` files appear live in `-dir` as you type.

### Running tests

```bash
go build ./...
go vet ./...
go test ./storage/... -race
```

Verbose, so each test name/scenario is visible:

```bash
go test ./storage/... -race -v
```

Repeat many times to catch timing-dependent bugs in the background
flush/compaction goroutines:

```bash
go test ./storage/... -race -count=15
```

Run just one scenario group by name:

```bash
go test ./storage/... -race -run TestStoreCrashRecovery -v   # crash mid-write, restart, WAL replay
go test ./storage/... -race -run TestStoreCompaction -v      # size-tiered merge + tombstone GC
go test ./storage/... -race -run TestSSTableCorruptBlock -v  # checksum corruption is reported, not swallowed
```

| Test file | Covers |
|---|---|
| `wal_test.go` | Append/replay round trip, missing file, a truncated ("torn") final record, concurrent group commit |
| `memtable_test.go` | Basic ops, ascending iteration order, size accounting, concurrent access — run against both `Memtable` and `SkipListMemtable` |
| `sstable_test.go` | Multi-block round trip, tombstones, enforced ascending key order, `ErrCorruptBlock` on a flipped byte |
| `store_test.go` | End-to-end: basic Get/Put/Delete, tombstone surviving a flush, crash recovery (clean and torn-WAL), compaction + tombstone GC, concurrent Get/Put/Delete |

### Benchmarks

**Important**: these benchmarks deliberately do **not** use `b.TempDir()`.
That resolves under `os.TempDir()`, which on many machines is a `tmpfs`
mount — an in-RAM filesystem where `fsync` is nearly free, which would
silently produce meaningless, far-too-fast numbers for exactly the thing
being measured. They write into the project's own `tmp/` instead
(gitignored, see `benchDir` in `store_bench_test.go`) — confirm it's on a
real disk before trusting any number out of these:

```bash
df -T tmp/
```

Raw WAL append throughput under concurrent callers (sweep GOMAXPROCS with
`-cpu` since `testing.B` doesn't vary parallelism within one run):

```bash
go test ./storage/... -bench=BenchmarkWALAppendParallel -run=^$ -benchtime=1000x -cpu=1,2,4,8,16
```

End-to-end `Store.Put` (same WAL cost, plus the memtable insert). Uses a
huge flush threshold so no flush runs during the benchmark; **always pass
a fixed `-benchtime=Nx`** here, never the default time-based
auto-scaling — with flush disabled the memtable keeps growing for the
whole run, and letting the auto-scaler pick a huge iteration count on its
own makes the run take far longer than intended:

```bash
go test ./storage/... -bench=BenchmarkStorePutParallel -run=^$ -benchtime=1000x -cpu=1,2,4,8,16
```

Tune value size same as the engine benchmarks:

```bash
go test ./storage/... -bench=. -run=^$ -benchtime=1000x -valuesize=256 -cpu=1,16
```

Memtable, sorted slice (`Memtable`) vs. skip list (`SkipListMemtable`) —
same shared-keyspace-under-`b.RunParallel` methodology, swept by
`-mem-keyspace` (memtable size) and `-mem-writepct`:

```bash
go test ./storage/... -bench=Memtable -run=^$ -benchmem -mem-writepct=50
go test ./storage/... -bench=Memtable -run=^$ -benchmem -mem-keyspace=200000 -mem-writepct=50
```

## Raft (M3)

A from-scratch Raft consensus implementation, tested through
`simharness` (an in-process fake network/persister/N-peer test rig —
its own Go module, see Project layout above). Peers only ever talk
through a `Call(svcMeth, args, reply) bool`-shaped interface, so the same
`raft.Raft` logic runs unmodified against the fake network in tests and
(once M4 wires it up) a real gRPC transport in production.

Current scope is **leader election** only: `RequestVote`, heartbeat-only
`AppendEntries`, randomized election timeouts, and the term-based checks
that keep two servers from ever both claiming leadership in the same
term. Log replication, persistence (`ps.Save`), and snapshotting are the
next steps of M3 — `Start`/`Snapshot` currently exist only to satisfy the
harness's peer interface and don't do anything beyond that yet.

- **`raft.go`** — the `Raft` struct's state (mirrors the paper's Figure 2:
  persistent state on all servers, volatile state on all servers, volatile
  state on leaders), `Make` (matches `harness.RaftMaker`), the election
  ticker with a randomized deadline, and the candidate/leader transitions.
- **`rpc.go`** — `RequestVote`/`AppendEntries` request/reply structs and
  handlers.
- **`election_test.go`** — `TestInitialElection`, `TestReElection`.

### Running tests

```bash
go test ./raft/... -race
```

Repeat many times to catch timing-dependent election bugs:

```bash
go test ./raft/... -race -count=20
```

Run one scenario at a time:

```bash
go test ./raft/... -race -run TestInitialElection -v
go test ./raft/... -race -run TestReElection -v
```

| Test file | Covers |
|---|---|
| `election_test.go` | Initial leader election; leader disconnect → re-election; old leader rejoining without disrupting the new one; dropping to a minority (no leader can emerge); quorum restored |

## Verifying the build

```bash
go build ./...
go vet ./...
go test ./... -race
```
