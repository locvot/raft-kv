// Package persister is a fake stable-storage layer: it plays the role of
// disk. A crashed-and-restarted peer in the test harness reads its Raft
// state (and snapshot) back from here, the same way it would read a real
// file after a real reboot.
package persister

import "sync"

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = append([]byte{}, ps.raftstate...)
	np.snapshot = append([]byte{}, ps.snapshot...)
	return np
}

// ReadRaftState returns the last state saved via Save, or nil if none.
func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return append([]byte{}, ps.raftstate...)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save persists Raft's durable state. Call it every time term, votedFor, or
// the log changes — before replying to the RPC that caused the change.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = append([]byte{}, raftstate...)
	ps.snapshot = append([]byte{}, snapshot...)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return append([]byte{}, ps.snapshot...)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
