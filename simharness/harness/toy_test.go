package harness

// toyPeer is NOT Raft. It's the smallest possible stand-in — server 0 is
// always "leader" and broadcasts every command by direct RPC — used only to
// prove that Config/simnet's plumbing (start/crash/restart, connect/
// disconnect, RPC dispatch through reflection+gob, and the applier safety
// check) actually works before you wire in your real raft.Raft via
// RaftMaker. Do not use this as a reference for how Raft itself works.

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"simharness/persister"
	"simharness/simnet"
)

type toyPeer struct {
	mu      sync.Mutex
	me      int
	peers   []*simnet.ClientEnd
	ps      *persister.Persister
	applyCh chan ApplyMsg
	killed  bool
	nextIdx int
}

type ReplicateArgs struct {
	Index   int
	Command interface{}
}
type ReplicateReply struct{ OK bool }

func newToyPeer(peers []*simnet.ClientEnd, me int, ps *persister.Persister, applyCh chan ApplyMsg) RaftPeer {
	tp := &toyPeer{me: me, peers: peers, ps: ps, applyCh: applyCh}
	if data := ps.ReadRaftState(); len(data) > 0 {
		fmt.Sscanf(string(data), "%d", &tp.nextIdx)
	}
	return tp
}

func (tp *toyPeer) Start(command interface{}) (int, int, bool) {
	if tp.me != 0 {
		return -1, 1, false
	}
	tp.mu.Lock()
	if tp.killed {
		tp.mu.Unlock()
		return -1, 1, false
	}
	tp.nextIdx++
	idx := tp.nextIdx
	tp.ps.Save([]byte(fmt.Sprintf("%d", tp.nextIdx)), nil)
	peers := tp.peers
	tp.mu.Unlock()

	tp.applyCh <- ApplyMsg{CommandValid: true, Command: command, CommandIndex: idx}

	for j, end := range peers {
		if j == tp.me {
			continue
		}
		go func(end *simnet.ClientEnd) {
			var reply ReplicateReply
			end.Call("toyPeer.Replicate", ReplicateArgs{Index: idx, Command: command}, &reply)
		}(end)
	}
	return idx, 1, true
}

func (tp *toyPeer) Replicate(args ReplicateArgs, reply *ReplicateReply) {
	tp.mu.Lock()
	killed := tp.killed
	tp.mu.Unlock()
	if killed {
		return
	}
	tp.applyCh <- ApplyMsg{CommandValid: true, Command: args.Command, CommandIndex: args.Index}
	reply.OK = true
}

func (tp *toyPeer) GetState() (int, bool) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	return 1, tp.me == 0 && !tp.killed
}

func (tp *toyPeer) Snapshot(index int, snapshot []byte) {}

func (tp *toyPeer) Kill() {
	tp.mu.Lock()
	tp.killed = true
	tp.mu.Unlock()
}

// ---------------------------------------------------------------------
// Smoke tests: these validate the HARNESS, not a real Raft. Once you swap
// newToyPeer for your own raft.Make, layer your real 2A/2B/2C/2D-style
// tests on top of these same Config calls.
// ---------------------------------------------------------------------

func TestHarnessBasicAgreement(t *testing.T) {
	cfg := NewConfig(t, 3, false, newToyPeer)
	defer cfg.Cleanup()

	idx := cfg.One("hello", 3, false)
	if n, v := cfg.NCommitted(idx); n != 3 || v != "hello" {
		t.Fatalf("NCommitted(%d) = (%d, %v), want (3, hello)", idx, n, v)
	}
}

func TestHarnessPartitionExcludesDisconnected(t *testing.T) {
	cfg := NewConfig(t, 3, false, newToyPeer)
	defer cfg.Cleanup()

	cfg.Disconnect(2)
	idx := cfg.One("partitioned-write", 2, false)

	time.Sleep(200 * time.Millisecond) // give any (incorrect) delivery a chance to land
	if n, _ := cfg.NCommitted(idx); n != 2 {
		t.Fatalf("NCommitted(%d) = %d while server 2 was disconnected, want exactly 2", idx, n)
	}

	cfg.Connect(2)
}

func TestHarnessCrashRestartPreservesState(t *testing.T) {
	cfg := NewConfig(t, 3, false, newToyPeer)
	defer cfg.Cleanup()

	cfg.One("before-crash", 3, false)

	cfg.Crash1(0)
	cfg.Start1(0)
	cfg.Connect(0)
	time.Sleep(100 * time.Millisecond)

	idx := cfg.One("after-restart", 3, false)
	if idx != 2 {
		t.Fatalf("index after restart = %d, want 2 (persister should have remembered nextIdx=1 across the crash)", idx)
	}
}

func TestHarnessRPCCountIncreases(t *testing.T) {
	cfg := NewConfig(t, 3, false, newToyPeer)
	defer cfg.Cleanup()

	before := cfg.RPCCount()
	cfg.One("count-me", 3, false)
	after := cfg.RPCCount()
	if after <= before {
		t.Fatalf("RPCCount did not increase: before=%d after=%d", before, after)
	}
}
