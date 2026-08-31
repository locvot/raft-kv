package raft

import (
	"fmt"
	"testing"

	"simharness/harness"
)

// This harness has no maxraftstate knob (unlike the MIT 6.5840 lab's 2D
// config), so these tests drive snapshotting explicitly via
// cfg.SnapshotOn — standing in for a service layer that would otherwise
// decide on its own when it has captured enough state to compact the log.

// TestSnapshotBasic checks that snapshotting every peer as it goes still
// leaves normal replication working, and that RaftStateSize doesn't just
// grow forever now that entries can be discarded.
func TestSnapshotBasic(t *testing.T) {
	n := 3
	cfg := harness.NewConfig(t, n, false, Make)
	defer cfg.Cleanup()

	sizeBeforeCompaction := -1
	const commands = 50
	for i := 1; i <= commands; i++ {
		index := cfg.One(i, n, true)
		if i == 10 {
			sizeBeforeCompaction = cfg.RaftStateSize(0)
		}
		if i%10 == 0 {
			for s := 0; s < n; s++ {
				cfg.SnapshotOn(s, index, []byte(fmt.Sprintf("snap-%d", index)))
			}
		}
	}

	if sizeBeforeCompaction <= 0 {
		t.Fatalf("failed to sample RaftStateSize before compaction")
	}
	for s := 0; s < n; s++ {
		if size := cfg.RaftStateSize(s); size >= sizeBeforeCompaction {
			t.Fatalf("server %d: RaftStateSize %d did not shrink below pre-compaction sample %d after repeated snapshotting", s, size, sizeBeforeCompaction)
		}
	}

	// Compaction must not have broken ordinary replication.
	cfg.One(commands+1, n, true)
}

// TestSnapshotInstall forces the InstallSnapshot path: a follower falls far
// enough behind (while disconnected) that by the time it reconnects, the
// leader has already compacted away the entries it would need for a normal
// AppendEntries catch-up, leaving InstallSnapshot as the only way for it to
// rejoin.
func TestSnapshotInstall(t *testing.T) {
	n := 3
	cfg := harness.NewConfig(t, n, false, Make)
	defer cfg.Cleanup()

	cfg.One(0, n, true)

	leader := cfg.CheckOneLeader()
	lagging := (leader + 1) % n
	cfg.Disconnect(lagging)

	const commands = 30
	var lastIndex int
	for i := 1; i <= commands; i++ {
		lastIndex = cfg.One(i, n-1, true)
	}

	// Compact away everything the lagging peer will need replayed.
	for s := 0; s < n; s++ {
		if s != lagging {
			cfg.SnapshotOn(s, lastIndex, []byte("snap"))
		}
	}

	cfg.Connect(lagging)
	cfg.One(commands+1, n, true)
}

// TestSnapshotInstallCrash combines InstallSnapshot with a crash-restart:
// the victim comes back from scratch (empty log, whatever the persister
// had saved) and must catch up purely from a leader-sent snapshot plus
// whatever AppendEntries follow it.
func TestSnapshotInstallCrash(t *testing.T) {
	n := 3
	cfg := harness.NewConfig(t, n, false, Make)
	defer cfg.Cleanup()

	cfg.One(0, n, true)

	leader := cfg.CheckOneLeader()
	victim := (leader + 1) % n
	cfg.Crash1(victim)

	const commands = 30
	var lastIndex int
	for i := 1; i <= commands; i++ {
		lastIndex = cfg.One(i, n-1, true)
	}

	for s := 0; s < n; s++ {
		if s != victim {
			cfg.SnapshotOn(s, lastIndex, []byte("snap"))
		}
	}

	cfg.Start1(victim)
	cfg.Connect(victim)
	cfg.One(commands+1, n, true)
}
