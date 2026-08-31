package raft

import (
	"bytes"
	"encoding/gob"
)

// persistedState is the durable subset of a Raft peer's state — exactly the
// three fields Figure 2 requires survive a crash. commitIndex/lastApplied
// are volatile: on restart they rebuild from zero as committed entries get
// re-delivered (or, once snapshotting lands, from the snapshot's index).
type persistedState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

// persist saves currentTerm, votedFor, and log to stable storage. Must be
// called with rf.mu held, and — per Figure 2 — every time one of those
// three fields changes, before any RPC reply triggered by that change goes
// out. Otherwise a crash between the reply and the save could leave this
// peer having promised something (a vote, an accepted entry) it forgets on
// restart.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if err := e.Encode(persistedState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	}); err != nil {
		panic("raft: persist: " + err.Error())
	}
	rf.persister.Save(w.Bytes(), rf.persister.ReadSnapshot())
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
}
