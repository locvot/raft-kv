package raft

import (
	"sync"
	"testing"
	"time"

	"simharness/harness"
)

// harness.Config deliberately doesn't expose the raw *Raft peers to test
// code (only Start/GetState indirectly, through One/CheckOneLeader), so
// these tests drive log replication through cfg.One/NCommitted/Connect/
// Disconnect rather than calling rf.Start directly the way the original
// MIT 6.824 2B tests do. Same properties under test, adapted to the API
// this harness actually gives us.

func TestBasicAgree(t *testing.T) {
	n := 3
	cfg := harness.NewConfig(t, n, false, Make)
	defer cfg.Cleanup()

	cfg.CheckOneLeader() // let the initial election settle before Start(retry=false)

	for i := 1; i <= 3; i++ {
		if nc, _ := cfg.NCommitted(i); nc > 0 {
			t.Fatalf("index %d already committed before Start", i)
		}
		index := cfg.One(100+i, n, false)
		if index != i {
			t.Fatalf("expected command to land at index %d, got %d", i, index)
		}
	}
}

func TestFailAgree(t *testing.T) {
	n := 3
	cfg := harness.NewConfig(t, n, false, Make)
	defer cfg.Cleanup()

	cfg.One(101, n, true)

	// A minority (one follower) disconnects — the remaining majority must
	// keep committing.
	leader := cfg.CheckOneLeader()
	follower := (leader + 1) % n
	cfg.Disconnect(follower)

	cfg.One(102, n-1, true)
	cfg.One(103, n-1, true)
	cfg.One(104, n-1, true)

	// Follower rejoins and must catch up to full agreement.
	cfg.Connect(follower)
	cfg.One(105, n, true)
}

func TestFailNoAgree(t *testing.T) {
	n := 5
	cfg := harness.NewConfig(t, n, false, Make)
	defer cfg.Cleanup()

	cfg.One(201, n, true)

	// Isolate the leader together with enough followers that the
	// *remaining* connected set (a minority of followers, not including
	// the old leader) can never elect anyone: no connected server ever
	// believes it's leader, so nothing can accept writes.
	leader := cfg.CheckOneLeader()
	cfg.Disconnect(leader)
	cfg.Disconnect((leader + 1) % n)
	cfg.Disconnect((leader + 2) % n)

	time.Sleep(2 * time.Second)
	cfg.CheckNoLeader()

	if nc, _ := cfg.NCommitted(2); nc > 0 {
		t.Fatalf("index 2 committed with no majority available")
	}

	// Restore quorum — progress must resume.
	cfg.Connect(leader)
	cfg.Connect((leader + 1) % n)
	cfg.Connect((leader + 2) % n)
	cfg.One(202, n, true)
}

func TestConcurrentStarts(t *testing.T) {
	n := 3
	cfg := harness.NewConfig(t, n, false, Make)
	defer cfg.Cleanup()

	cfg.CheckOneLeader()

	const concurrent = 5
	indices := make([]int, concurrent)
	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			indices[i] = cfg.One(1000+i, n, true)
		}(i)
	}
	wg.Wait()

	seen := map[int]bool{}
	for _, idx := range indices {
		if seen[idx] {
			t.Fatalf("two concurrent Start calls landed on the same index %d", idx)
		}
		seen[idx] = true
	}
}

func TestRejoin(t *testing.T) {
	n := 3
	cfg := harness.NewConfig(t, n, false, Make)
	defer cfg.Cleanup()

	cfg.One(101, n, true)

	// Leader partitioned away; the remaining majority elects a new one and
	// keeps committing without it.
	leader1 := cfg.CheckOneLeader()
	cfg.Disconnect(leader1)
	cfg.One(102, n-1, true)

	// New leader also partitioned; old leader rejoins as a lagging
	// follower with a stale/divergent log tail it must reconcile.
	leader2 := cfg.CheckOneLeader()
	cfg.Disconnect(leader2)
	cfg.Connect(leader1)
	cfg.One(103, n-1, true)

	// Everyone reconnects — full agreement must be reachable again,
	// including the entries committed while leader1 was cut off.
	cfg.Connect(leader2)
	cfg.One(104, n, true)
}

func TestBackup(t *testing.T) {
	n := 5
	cfg := harness.NewConfig(t, n, false, Make)
	defer cfg.Cleanup()

	cfg.One(0, n, true)

	// Two followers fall behind by many entries while still connected to
	// nobody; the leader keeps committing on the remaining 3-of-5
	// majority.
	leader := cfg.CheckOneLeader()
	lag1 := (leader + 1) % n
	lag2 := (leader + 2) % n
	cfg.Disconnect(lag1)
	cfg.Disconnect(lag2)

	const many = 30
	for i := 1; i <= many; i++ {
		cfg.One(i, n-2, true)
	}

	// Reconnecting both should make the leader backtrack nextIndex across
	// the whole gap using ConflictTerm/ConflictIndex, not one entry at a
	// time, and land on full agreement quickly.
	cfg.Connect(lag1)
	cfg.Connect(lag2)
	cfg.One(many+1, n, true)
}
