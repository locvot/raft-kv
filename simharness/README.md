# simharness

An MIT-6.824/6.5840-style test rig for a from-scratch Go Raft implementation:
an in-process fake network (`simnet`) that can drop, delay, reorder, and
partition RPCs on demand, a fake disk (`persister`) for crash/restart tests,
and a `harness.Config` that wires N peers together and fails the test the
instant two servers disagree about what got committed.

Verified: `go build ./...`, `go vet ./...`, and `go test ./... -race` all
pass, including a repeated run (`-count=10`) of the harness package to rule
out flakiness in the harness itself. The `harness` package's tests currently
run against `toyPeer` (in `toy_test.go`) — a deliberately non-Raft stand-in
used only to prove the plumbing works. Delete or ignore `toy_test.go` once
you're testing your real implementation; it's there as a working reference,
not as part of your test suite.

## Wiring in your real Raft

Your `raft.Raft` needs to satisfy `harness.RaftPeer`:

```go
type RaftPeer interface {
    Start(command interface{}) (index int, term int, isLeader bool)
    GetState() (term int, isLeader bool)
    Snapshot(index int, snapshot []byte)
    Kill()
}
```

and your constructor needs to match `harness.RaftMaker`:

```go
func Make(peers []*simnet.ClientEnd, me int, ps *persister.Persister, applyCh chan harness.ApplyMsg) harness.RaftPeer
```

Your RPC handlers (`RequestVote`, `AppendEntries`, `InstallSnapshot`, ...)
must be exported methods shaped like:

```go
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply)
```

(no error return — `simnet` reports transport failure through `Call`'s bool
return, the same as `end.Call(...)` inside your own code). Call them from
inside `raft.go` via `peers[j].Call("Raft.RequestVote", args, &reply)`.

Then a test just does:

```go
cfg := harness.NewConfig(t, 3, false, raft.Make)
defer cfg.Cleanup()
```

## Suggested test categories (mirrors 2A/2B/2C/2D)

Write these as you build each piece of Raft, not all at the end:

- **Election** — `TestInitialElection`, `TestReElection`: bring up 3-5
  peers, `cfg.CheckOneLeader()`, disconnect the leader, confirm a new one
  is elected, reconnect the old one, confirm it doesn't disrupt things.
- **Log agreement** — `TestBasicAgree`, `TestFailAgree`, `TestFailNoAgree`,
  `TestConcurrentStarts`, `TestRejoin`, `TestBackup`: use `cfg.One(cmd, n,
  retry)` and `cfg.NCommitted(index)`; disconnect a minority and confirm
  writes still go through; disconnect a majority and confirm they *don't*.
- **Persistence** — `TestPersist1/2/3`, `TestFigure8`: `cfg.Crash1(i)` then
  `cfg.Start1(i)` then `cfg.Connect(i)`; confirm committed entries survive
  and an uncommitted entry from an old term never gets committed by a later
  leader (this is Leader Completeness — the classic subtle bug).
- **Unreliable network** — same tests again with
  `cfg.SetUnreliable(true)` and `cfg.SetLongReordering(true)`.

Run everything with the race detector and repeatedly to catch timing-
dependent bugs before they catch you:

```
go test ./... -race -run 2A -count 20
go test ./... -race -run 2B -count 20
```

## What this harness checks for you automatically

Every peer's `applyCh` is drained by a background "applier" inside
`Config` that cross-checks commits across all peers. If two servers ever
apply different commands at the same log index, the test fails immediately
with both values named — that's State Machine Safety, enforced for you.
`CheckOneLeader` similarly fails the test outright if it ever observes two
leaders in the same term (Election Safety). Everything else — Leader
Append-Only, Log Matching, Leader Completeness — has to hold up under the
partition/crash/unreliable scenarios above; the harness gives you the tools
to provoke them, not a separate checker.
