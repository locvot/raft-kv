package raft

import (
	"math/rand"
	"testing"
	"time"

	"simharness/harness"
)

// Ported from the MIT 6.5840 lab's TestManyElections3A: repeatedly strip a
// minority of servers out of a 7-node cluster and put them back, and check
// Election Safety holds (exactly one leader) at every step, not just once
// at the start.
func TestManyElections(t *testing.T) {
	n := 7
	cfg := harness.NewConfig(t, n, false, Make)
	defer cfg.Cleanup()

	cfg.CheckOneLeader()

	for iter := 0; iter < 10; iter++ {
		i1 := rand.Intn(n)
		i2 := rand.Intn(n)
		i3 := rand.Intn(n)
		cfg.Disconnect(i1)
		cfg.Disconnect(i2)
		cfg.Disconnect(i3)

		// Either the old leader survived (it wasn't one of the three), or
		// the remaining four elect a new one.
		cfg.CheckOneLeader()

		cfg.Connect(i1)
		cfg.Connect(i2)
		cfg.Connect(i3)
	}
	cfg.CheckOneLeader()
}

// Ported from the MIT 6.5840 lab's TestFigure8Unreliable3C, dropping the
// crash/restart half of the original (this Raft doesn't persist state yet,
// so a restart would just lose it — not a bug this test is meant to probe).
// What's left is still the sharpest tool available for the nextIndex
// regression flagged in code review: every iteration submits a command to
// every server concurrently (not just through whichever one cfg.One()
// happens to find), so multiple AppendEntries rounds to the same peer are
// routinely in flight together, and after iteration 200 the network starts
// reordering their replies by a long, randomized delay. If a stale reply
// can regress a peer's replication progress, this is the workload that
// gives it the most chances to happen.
func TestFigure8Unreliable(t *testing.T) {
	n := 5
	cfg := harness.NewConfig(t, n, true, Make)
	defer cfg.Cleanup()

	cfg.One(rand.Intn(10000), 1, true)

	nup := n
	for iter := 0; iter < 1000; iter++ {
		if iter == 200 {
			cfg.SetLongReordering(true)
		}

		leader := -1
		for i := 0; i < n; i++ {
			cmd := rand.Intn(10000)
			if _, _, ok := cfg.StartOn(i, cmd); ok && cfg.IsConnected(i) {
				leader = i
			}
		}

		if rand.Intn(1000) < 100 {
			ms := rand.Int63n(int64(electionTimeoutMax) / 2 / int64(time.Millisecond))
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := rand.Int63n(13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && rand.Intn(1000) < int(electionTimeoutMax/time.Millisecond)/2 {
			cfg.Disconnect(leader)
			nup--
		}

		if nup < 3 {
			s := rand.Intn(n)
			if !cfg.IsConnected(s) {
				cfg.Connect(s)
				nup++
			}
		}
	}

	for i := 0; i < n; i++ {
		if !cfg.IsConnected(i) {
			cfg.Connect(i)
		}
	}

	cfg.One(rand.Intn(10000), n, true)
}
