package raft

import (
	"testing"
	"time"

	"simharness/harness"
)

func TestInitialElection(t *testing.T) {
	cfg := harness.NewConfig(t, 3, false, Make)
	defer cfg.Cleanup()

	cfg.CheckOneLeader()
}

func TestReElection(t *testing.T) {
	cfg := harness.NewConfig(t, 3, false, Make)
	defer cfg.Cleanup()

	leader1 := cfg.CheckOneLeader()

	// Leader disconnected -> remaining majority must elect a new one.
	cfg.Disconnect(leader1)
	cfg.CheckOneLeader()

	// Old leader rejoins -> it must step down (higher term seen from the
	// current leader) instead of disrupting the cluster.
	cfg.Connect(leader1)
	leader2 := cfg.CheckOneLeader()

	// Drop to a minority (leader plus one follower disconnected, one lone
	// follower left) -> no quorum, so no leader should ever emerge even
	// after several election timeouts.
	cfg.Disconnect(leader2)
	cfg.Disconnect((leader2 + 1) % 3)
	time.Sleep(2 * time.Second)
	cfg.CheckNoLeader()

	// Quorum restored (2 of 3) -> a leader is elected again.
	cfg.Connect((leader2 + 1) % 3)
	cfg.CheckOneLeader()

	// Last node rejoining must not disrupt the elected leader.
	cfg.Connect(leader2)
	cfg.CheckOneLeader()
}
