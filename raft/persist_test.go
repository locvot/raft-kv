package raft

import (
	"math/rand"
	"testing"
	"time"

	"simharness/harness"
)

// Ported from the MIT 6.5840 lab's TestPersist12C: crash-restart every
// server in turn (sometimes the whole cluster, sometimes just the current
// leader, sometimes a follower isolated first) and check agreement still
// reaches everyone afterward. Each cfg.Start1 tears down the live peer and
// rebuilds it from a *copy* of its persister, exactly like a process
// reading currentTerm/votedFor/log back off disk after a real reboot.
func TestPersist1(t *testing.T) {
	n := 3
	cfg := harness.NewConfig(t, n, false, Make)
	defer cfg.Cleanup()

	cfg.One(11, n, true)

	// Crash and restart every server at once.
	for i := 0; i < n; i++ {
		cfg.Start1(i)
	}
	for i := 0; i < n; i++ {
		cfg.Disconnect(i)
		cfg.Connect(i)
	}
	cfg.One(12, n, true)

	// Restart just the leader.
	leader1 := cfg.CheckOneLeader()
	cfg.Disconnect(leader1)
	cfg.Start1(leader1)
	cfg.Connect(leader1)
	cfg.One(13, n, true)

	// Restart the (possibly new) leader while it's isolated, having missed
	// an entry the rest of the cluster committed without it.
	leader2 := cfg.CheckOneLeader()
	cfg.Disconnect(leader2)
	cfg.One(14, n-1, true)
	cfg.Start1(leader2)
	cfg.Connect(leader2)
	cfg.One(15, n, true) // wait for leader2's restart to fully rejoin before touching another peer

	// Restart a follower after isolating it the same way.
	follower := (cfg.CheckOneLeader() + 1) % n
	cfg.Disconnect(follower)
	cfg.One(16, n-1, true)
	cfg.Start1(follower)
	cfg.Connect(follower)
	cfg.One(17, n, true)
}

// Ported from the MIT 6.5840 lab's TestPersist22C: strand a 2-of-5 minority,
// crash-restart it while still isolated (so it comes back with only the
// stale log it had before losing contact with the majority), then bring it
// back and confirm it reconciles rather than corrupting the log the
// majority kept extending.
func TestPersist2(t *testing.T) {
	n := 5
	cfg := harness.NewConfig(t, n, false, Make)
	defer cfg.Cleanup()

	cfg.One(101, n, true)

	leader := cfg.CheckOneLeader()
	minorityA := (leader + 1) % n
	minorityB := (leader + 2) % n
	cfg.Disconnect(minorityA)
	cfg.Disconnect(minorityB)

	cfg.One(102, n-2, true)

	// Now isolate everyone else too, so the two minority peers can restart
	// with nothing but their own stale, unreconciled state.
	cfg.Disconnect(leader)
	majorityC := (leader + 3) % n
	majorityD := (leader + 4) % n
	cfg.Disconnect(majorityC)
	cfg.Disconnect(majorityD)

	cfg.Start1(minorityA)
	cfg.Start1(minorityB)
	cfg.Connect(minorityA)
	cfg.Connect(minorityB)

	time.Sleep(electionTimeoutMax)

	cfg.Start1(majorityC)
	cfg.Connect(majorityC)

	cfg.One(103, n-2, true)

	cfg.Connect(majorityD)
	cfg.Connect(leader)
}

// Ported from the MIT 6.5840 lab's TestPersist32C: same crash-restart shape
// as TestPersist2 but over an unreliable network, so RPC drops/delays land
// in between the persisted writes too.
func TestPersist3(t *testing.T) {
	n := 3
	cfg := harness.NewConfig(t, n, true, Make)
	defer cfg.Cleanup()

	cfg.One(rand.Intn(10000), n, true)

	leader := cfg.CheckOneLeader()
	cfg.Disconnect((leader + 1) % n)
	cfg.One(rand.Intn(10000), n-1, true)

	cfg.Disconnect(leader)
	cfg.Disconnect((leader + 2) % n)
	cfg.Start1((leader + 1) % n)
	cfg.Connect((leader + 1) % n)
	cfg.Connect((leader + 2) % n)
	cfg.Connect(leader)

	cfg.One(rand.Intn(10000), n, true)
}

// Ported from the MIT 6.5840 lab's TestFigure82C: the paper's Figure 8
// scenario made adversarial. Every round, every connected server tries
// Start(), then the round's leader (if any) gets crashed and later
// restarted from its persisted state — repeatedly, for 1000 rounds, so an
// old-term entry has every opportunity to be incorrectly committed the way
// Figure 8 describes (reaching a majority without ever being safely
// committed by a leader that saw it in its own term) if persistence or
// commit-index logic is even slightly wrong. cfg.IsConnected stands in for
// the upstream test's `cfg.rafts[i] != nil` check — this harness doesn't
// nil out a crashed peer's slot, but crash1 always disconnects first and
// only Start1+Connect ever reconnects, so "connected" is an equally
// reliable proxy for "has a live, non-crashed Raft instance right now."
func TestFigure8(t *testing.T) {
	n := 5
	cfg := harness.NewConfig(t, n, false, Make)
	defer cfg.Cleanup()

	cfg.One(rand.Intn(10000), 1, true)

	for iter := 0; iter < 1000; iter++ {
		leader := -1
		for i := 0; i < n; i++ {
			if cfg.IsConnected(i) {
				if _, _, ok := cfg.StartOn(i, rand.Intn(10000)); ok {
					leader = i
				}
			}
		}

		if rand.Intn(1000) < 100 {
			ms := rand.Int63n(int64(electionTimeoutMax/time.Millisecond)/2 + 1)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.Int63n(13)) * time.Millisecond)
		}

		if leader != -1 {
			cfg.Crash1(leader)
		}

		nup := 0
		for i := 0; i < n; i++ {
			if cfg.IsConnected(i) {
				nup++
			}
		}
		if nup < 3 {
			s := rand.Intn(n)
			if !cfg.IsConnected(s) {
				cfg.Start1(s)
				cfg.Connect(s)
			}
		}
	}

	for i := 0; i < n; i++ {
		if !cfg.IsConnected(i) {
			cfg.Start1(i)
			cfg.Connect(i)
		}
	}

	cfg.One(rand.Intn(10000), n, true)
}
