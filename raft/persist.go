package raft

import (
	"bytes"
	"encoding/gob"
)

// persistedState is the durable subset of a Raft peer's state — exactly
// what Figure 2 (plus Figure 13's snapshot fields) requires survive a
// crash. commitIndex/lastApplied are volatile: on restart they're
// reinitialized to lastIncludedIndex, since anything at or before it is
// covered by the snapshot the service layer is expected to load on its
// own from persister.ReadSnapshot().
type persistedState struct {
	CurrentTerm       int
	VotedFor          int
	Log               []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

// persist saves currentTerm, votedFor, and log to stable storage, keeping
// whatever snapshot is already on disk untouched. Must be called with
// rf.mu held, and — per Figure 2 — every time one of those fields changes,
// before any RPC reply triggered by that change goes out. Otherwise a
// crash between the reply and the save could leave this peer having
// promised something (a vote, an accepted entry) it forgets on restart.
func (rf *Raft) persist() {
	rf.persistStateAndSnapshot(rf.persister.ReadSnapshot())
}

// persistStateAndSnapshot saves currentTerm/votedFor/log together with a
// new snapshot, atomically from the persister's point of view. Used by
// Snapshot and InstallSnapshot, the two places the snapshot itself
// changes; everywhere else just calls persist and leaves it alone.
func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if err := e.Encode(persistedState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		Log:               rf.log,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}); err != nil {
		panic("raft: persist: " + err.Error())
	}
	rf.persister.Save(w.Bytes(), snapshot)
}

// readPersist restores state saved by a previous instance of this peer.
// data is nil (or empty) on a genuinely first boot, in which case rf keeps
// the zero-value defaults Make already set. Called once from Make, before
// the background loops start, so no lock is needed.
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var ps persistedState
	if err := d.Decode(&ps); err != nil {
		panic("raft: readPersist: " + err.Error())
	}
	rf.currentTerm = ps.CurrentTerm
	rf.votedFor = ps.VotedFor
	rf.log = ps.Log
	rf.lastIncludedIndex = ps.LastIncludedIndex
	rf.lastIncludedTerm = ps.LastIncludedTerm
}
