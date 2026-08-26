// Package harness is the MIT-6.824-style test rig: it wires N of your Raft
// peers together over the simnet fake network, gives you Connect/Disconnect
// (partitions) and Crash/Start (crash-restart via the persister package),
// and — this is the important part — an applier goroutine per server that
// watches every commit and immediately fails the test if two
// servers ever apply different commands at the same log index. That single
// check is what actually enforces Raft's State Machine Safety property; the
// rest of this file just gives you plumbing to provoke violations of it.
package harness

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"simharness/persister"
	"simharness/simnet"
)

// ApplyMsg is what your Raft sends on applyCh for each committed entry (and,
// optionally, each installed snapshot). Shape it however your raft.go
// needs — this harness only reads CommandValid/Command/CommandIndex.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// RaftPeer is the shape your raft.Raft must satisfy to plug into this
// harness — Start/GetState/Snapshot/Kill, the same four operations any
// client of Raft needs regardless of test infrastructure.
type RaftPeer interface {
	Start(command interface{}) (index int, term int, isLeader bool)
	GetState() (term int, isLeader bool)
	Snapshot(index int, snapshot []byte)
	Kill()
}

// RaftMaker constructs one peer. peers[j] is how this peer calls peer j
// (peers[me] is unused, matching the real lab's convention).
type RaftMaker func(peers []*simnet.ClientEnd, me int, ps *persister.Persister, applyCh chan ApplyMsg) RaftPeer

type Config struct {
	mu        sync.Mutex
	t         *testing.T
	net       *simnet.Network
	n         int
	maker     RaftMaker
	rafts     []RaftPeer
	connected []bool
	saved     []*persister.Persister
	endnames  [][]string
	applyCh   []chan ApplyMsg
	gen       []int
	logs      []map[int]interface{}
}

func NewConfig(t *testing.T, n int, unreliable bool, maker RaftMaker) *Config {
	cfg := &Config{
		t:         t,
		net:       simnet.MakeNetwork(),
		n:         n,
		maker:     maker,
		rafts:     make([]RaftPeer, n),
		connected: make([]bool, n),
		saved:     make([]*persister.Persister, n),
		endnames:  make([][]string, n),
		applyCh:   make([]chan ApplyMsg, n),
		gen:       make([]int, n),
		logs:      make([]map[int]interface{}, n),
	}
	cfg.net.Reliable(!unreliable)
	for i := 0; i < n; i++ {
		cfg.logs[i] = map[int]interface{}{}
		cfg.endnames[i] = make([]string, n)
	}
	for i := 0; i < n; i++ {
		cfg.start1(i)
	}
	for i := 0; i < n; i++ {
		cfg.connect(i)
	}
	return cfg
}

func (cfg *Config) serverName(i int) string { return fmt.Sprintf("server-%d", i) }

// start1 boots (or reboots) peer i. Calling it on a live peer first tears
// the old instance down, so this is also how you simulate a crash-restart:
// the new instance gets a *copy* of the same persister, exactly like a real
// process reading its state back off disk after a reboot.
func (cfg *Config) start1(i int) {
	cfg.mu.Lock()
	oldRaft := cfg.rafts[i]
	cfg.mu.Unlock()
	if oldRaft != nil {
		oldRaft.Kill()
		cfg.net.DeleteServer(cfg.serverName(i))
	}

	cfg.mu.Lock()
	if cfg.saved[i] == nil {
		cfg.saved[i] = persister.MakePersister()
	} else {
		cfg.saved[i] = cfg.saved[i].Copy()
	}
	cfg.gen[i]++
	gen := cfg.gen[i]
	ps := cfg.saved[i]
	wasConnected := cfg.connected[i]
	cfg.mu.Unlock()

	ends := make([]*simnet.ClientEnd, cfg.n)
	endNames := make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		name := fmt.Sprintf("%d.g%d->%d", i, gen, j)
		endNames[j] = name
		ends[j] = cfg.net.MakeEnd(name)
		cfg.net.Connect(name, cfg.serverName(j))
		cfg.net.Enable(name, wasConnected)
	}

	applyCh := make(chan ApplyMsg)

	cfg.mu.Lock()
	cfg.endnames[i] = endNames
	cfg.applyCh[i] = applyCh
	cfg.mu.Unlock()

	rf := cfg.maker(ends, i, ps, applyCh)

	cfg.mu.Lock()
	cfg.rafts[i] = rf
	cfg.mu.Unlock()

	srv := simnet.MakeServer()
	srv.AddService(simnet.MakeService(rf))
	cfg.net.AddServer(cfg.serverName(i), srv)

	go cfg.applier(i, applyCh)
}

// crash1 kills peer i without restarting it. Its persister is preserved so
// a later start1(i) resumes as if the process had rebooted.
func (cfg *Config) crash1(i int) {
	cfg.disconnect(i)
	cfg.net.DeleteServer(cfg.serverName(i))

	cfg.mu.Lock()
	r := cfg.rafts[i]
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}
	cfg.mu.Unlock()
	if r != nil {
		r.Kill()
	}
}

// connect / disconnect simulate a partition: they disable the link in BOTH
// directions between i and every other currently-connected peer.
func (cfg *Config) connect(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.connected[i] = true
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			cfg.net.Enable(cfg.endnames[i][j], true)
			cfg.net.Enable(cfg.endnames[j][i], true)
		}
	}
}

func (cfg *Config) disconnect(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.connected[i] = false
	for j := 0; j < cfg.n; j++ {
		if len(cfg.endnames[i]) > 0 {
			cfg.net.Enable(cfg.endnames[i][j], false)
		}
		if len(cfg.endnames[j]) > 0 {
			cfg.net.Enable(cfg.endnames[j][i], false)
		}
	}
}

// applier is the safety net: every committed entry any peer reports gets
// cross-checked against what every OTHER peer already committed at that
// same index. A mismatch is State Machine Safety broken, and fails the
// test immediately with both conflicting values rather than corrupting
// silently.
func (cfg *Config) applier(i int, applyCh chan ApplyMsg) {
	for m := range applyCh {
		if !m.CommandValid {
			continue
		}
		cfg.mu.Lock()
		for j, l := range cfg.logs {
			if j == i {
				continue
			}
			if old, ok := l[m.CommandIndex]; ok && fmt.Sprintf("%v", old) != fmt.Sprintf("%v", m.Command) {
				cfg.mu.Unlock()
				cfg.t.Fatalf(
					"SAFETY VIOLATION (State Machine Safety) at index %d: server %d applied %v, server %d already applied %v",
					m.CommandIndex, i, m.Command, j, old)
				return
			}
		}
		cfg.logs[i][m.CommandIndex] = m.Command
		cfg.mu.Unlock()
	}
}

// CheckOneLeader polls until exactly one connected server claims leadership
// in the highest term seen, and fails the test outright (Election Safety)
// if it ever observes two leaders in the same term.
func (cfg *Config) CheckOneLeader() int {
	for iters := 0; iters < 12; iters++ {
		time.Sleep(400 * time.Millisecond)
		leadersByTerm := map[int][]int{}
		for i := 0; i < cfg.n; i++ {
			cfg.mu.Lock()
			connected := cfg.connected[i]
			rf := cfg.rafts[i]
			cfg.mu.Unlock()
			if connected && rf != nil {
				if term, isLeader := rf.GetState(); isLeader {
					leadersByTerm[term] = append(leadersByTerm[term], i)
				}
			}
		}
		best := -1
		for term, ls := range leadersByTerm {
			if len(ls) > 1 {
				cfg.t.Fatalf("SAFETY VIOLATION (Election Safety): servers %v all claim leadership in term %d", ls, term)
			}
			if term > best {
				best = term
			}
		}
		if best != -1 {
			return leadersByTerm[best][0]
		}
	}
	cfg.t.Fatalf("no leader elected within timeout")
	return -1
}

// CheckNoLeader asserts that no *connected* server believes it is leader —
// use it after isolating a minority partition.
func (cfg *Config) CheckNoLeader() {
	for i := 0; i < cfg.n; i++ {
		cfg.mu.Lock()
		connected := cfg.connected[i]
		rf := cfg.rafts[i]
		cfg.mu.Unlock()
		if connected && rf != nil {
			if _, isLeader := rf.GetState(); isLeader {
				cfg.t.Fatalf("server %d claims leadership but should not (isolated minority)", i)
			}
		}
	}
}

// NCommitted reports how many servers have applied a command at index, and
// what that command was. It fails the test if servers disagree.
func (cfg *Config) NCommitted(index int) (int, interface{}) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	count := 0
	var cmd interface{}
	for i := 0; i < cfg.n; i++ {
		if v, ok := cfg.logs[i][index]; ok {
			if count > 0 && fmt.Sprintf("%v", cmd) != fmt.Sprintf("%v", v) {
				cfg.t.Fatalf("SAFETY VIOLATION: committed values differ at index %d", index)
			}
			cmd = v
			count++
		}
	}
	return count, cmd
}

// One submits cmd through whichever peer currently claims leadership and
// waits (with light retrying) until it is committed on at least
// expectedServers peers. It returns the log index it landed at.
func (cfg *Config) One(cmd interface{}, expectedServers int, retry bool) int {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		index := -1
		for i := 0; i < cfg.n; i++ {
			cfg.mu.Lock()
			connected := cfg.connected[i]
			rf := cfg.rafts[i]
			cfg.mu.Unlock()
			if connected && rf != nil {
				if idx, _, isLeader := rf.Start(cmd); isLeader {
					index = idx
					break
				}
			}
		}
		if index != -1 {
			subDeadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(subDeadline) {
				n, v := cfg.NCommitted(index)
				if n >= expectedServers && fmt.Sprintf("%v", v) == fmt.Sprintf("%v", cmd) {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		if !retry {
			break
		}
	}
	cfg.t.Fatalf("One(%v): not committed on %d servers within timeout", cmd, expectedServers)
	return -1
}

// Crash1 / Start1 / Connect / Disconnect are the public entry points your
// TestXxx functions call — mirroring cfg.crash1/start1/connect/disconnect
// in the real lab's config.go.
func (cfg *Config) Crash1(i int)             { cfg.crash1(i) }
func (cfg *Config) Start1(i int)             { cfg.start1(i) }
func (cfg *Config) Connect(i int)            { cfg.connect(i) }
func (cfg *Config) Disconnect(i int)         { cfg.disconnect(i) }
func (cfg *Config) RPCCount() int            { return cfg.net.RPCCount() }
func (cfg *Config) SetUnreliable(u bool)     { cfg.net.Reliable(!u) }
func (cfg *Config) SetLongReordering(v bool) { cfg.net.LongReordering(v) }

func (cfg *Config) Cleanup() {
	for i := 0; i < cfg.n; i++ {
		cfg.mu.Lock()
		rf := cfg.rafts[i]
		cfg.mu.Unlock()
		if rf != nil {
			rf.Kill()
		}
	}
	cfg.net.Cleanup()
}
