package raft

// Snapshotting shifts the log's "index 0" forward: rf.log[0] stops being a
// fixed sentinel for global index 0 and becomes a movable placeholder for
// whatever index the snapshot last included — its Term field is kept equal
// to lastIncludedTerm, so termAt needs no special case for that boundary.
// rf.log[k] for k >= 1 holds the entry at global index
// lastIncludedIndex+k. nextIndex/matchIndex/commitIndex/lastApplied and
// every *Args/*Reply field always carry global indices; only rf.log
// indexing itself needs translating, via sliceIndex.

// pendingSnapshot is a snapshot InstallSnapshot has applied to rf's log
// but that applier hasn't yet handed to the state machine over applyCh.
// Needed because delivering it can block on the consumer, and nothing
// holding rf.mu can afford to block (see applier's doc comment).
type pendingSnapshot struct {
	index int
	term  int
	data  []byte
}

// lastLogIndex returns the global index of the last entry in rf.log.
func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

// sliceIndex converts a global log index into the physical offset into
// rf.log. Callers must ensure index >= rf.lastIncludedIndex.
func (rf *Raft) sliceIndex(index int) int {
	return index - rf.lastIncludedIndex
}

// termAt returns the term of the entry at global index, including
// rf.lastIncludedIndex itself (rf.log[0].Term is kept in sync with
// lastIncludedTerm, so no special case is needed here).
func (rf *Raft) termAt(index int) int {
	return rf.log[rf.sliceIndex(index)].Term
}

// Snapshot is called by the service layer above Raft (not implemented in
// this milestone) once it has durably captured its own state through
// index, telling Raft it may discard log entries up to and including it.
// index must already be applied — Raft can't discard entries the state
// machine hasn't actually incorporated into snapshot yet.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.lastApplied {
		return
	}

	newLastIncludedTerm := rf.termAt(index)
	rf.log = append([]LogEntry{{Term: newLastIncludedTerm}}, rf.log[rf.sliceIndex(index)+1:]...)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = newLastIncludedTerm

	rf.persistStateAndSnapshot(snapshot)
}

// sendInstallSnapshot ships the leader's current snapshot to peer i when
// its nextIndex has fallen behind what the leader still keeps logged —
// the entry it would need for a normal AppendEntries was already
// compacted away. Must be called with rf.mu held; it releases the lock
// itself and re-acquires it before returning, mirroring replicateTo.
func (rf *Raft) sendInstallSnapshot(i int, term int) {
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	seq := rf.dispatchSeq[i]
	rf.dispatchSeq[i]++
	rf.mu.Unlock()

	var reply InstallSnapshotReply
	if !rf.peers[i].Call("Raft.InstallSnapshot", args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if rf.state != leader || rf.currentTerm != term {
		return
	}
	// Same staleness guard as replicateTo: dispatchSeq/acceptedSeq are
	// shared per-peer across both RPC kinds, since either one advancing
	// nextIndex/matchIndex[i] makes an older in-flight reply of the other
	// kind stale.
	if seq <= rf.acceptedSeq[i] {
		return
	}
	rf.acceptedSeq[i] = seq

	if args.LastIncludedIndex+1 > rf.nextIndex[i] {
		rf.nextIndex[i] = args.LastIncludedIndex + 1
	}
	if args.LastIncludedIndex > rf.matchIndex[i] {
		rf.matchIndex[i] = args.LastIncludedIndex
	}
}
