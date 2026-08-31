package raft

import (
	"testing"

	"simharness/harness"
)

// Same election/agreement properties as election_test.go and
// agreement_test.go, but with the network dropping, delaying, and
// reordering RPCs — Leader Append-Only and Log Matching have to hold up
// under that too, not just on a reliable network.

func TestUnreliableAgree(t *testing.T) {
	n := 3
	cfg := harness.NewConfig(t, n, true, Make)
	defer cfg.Cleanup()

	cfg.CheckOneLeader()
	for i := 1; i <= 20; i++ {
		cfg.One(i, n, true)
	}
}

func TestUnreliableFailAgree(t *testing.T) {
	n := 3
	cfg := harness.NewConfig(t, n, true, Make)
	defer cfg.Cleanup()

	cfg.One(101, n, true)

	leader := cfg.CheckOneLeader()
	follower := (leader + 1) % n
	cfg.Disconnect(follower)

	cfg.One(102, n-1, true)
	cfg.One(103, n-1, true)

	cfg.Connect(follower)
	cfg.One(104, n, true)
}

func TestUnreliableLongReordering(t *testing.T) {
	n := 3
	cfg := harness.NewConfig(t, n, true, Make)
	defer cfg.Cleanup()
	cfg.SetLongReordering(true)

	cfg.CheckOneLeader()
	for i := 1; i <= 20; i++ {
		cfg.One(i, n, true)
	}
}
