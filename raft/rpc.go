package raft

// RequestVoteArgs / RequestVoteReply and AppendEntriesArgs / AppendEntriesReply
// are the wire structs for Raft's internal RPCs (Figure 2 of the paper).
// Handlers below take args by value and reply by pointer, with no error
// return, so they can be dispatched by both simnet (reflection) and a real
// gRPC adapter without changing raft.go.

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// ConflictTerm/ConflictIndex let the leader skip back over an entire
	// conflicting term in one round trip instead of retrying with
	// PrevLogIndex-1 each time (Figure 2's suggested optimization).
	// ConflictTerm is -1 when the follower's log is simply too short.
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.termAt(lastLogIndex)
	candidateUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateUpToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.resetElectionDeadline()
		return
	}
	reply.VoteGranted = false
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// A valid heartbeat/append from the current term's leader: recognize
	// it even as a candidate (Raft Figure 2's "convert to follower" rule)
	// and postpone our own next election.
	rf.state = follower
	rf.resetElectionDeadline()

	lastLogIndex := rf.lastLogIndex()
	if args.PrevLogIndex > lastLogIndex {
		reply.Success = false
		reply.ConflictIndex = lastLogIndex + 1
		return
	}
	if args.PrevLogIndex < rf.lastIncludedIndex {
		// Leader thinks we still need entries at or before our own
		// snapshot boundary — everything through lastIncludedIndex is
		// already compacted (and therefore already committed), so just
		// report how far along we actually are and let the leader retry
		// with an up-to-date PrevLogIndex, or fall back to InstallSnapshot.
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}
	if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.termAt(args.PrevLogIndex)
		idx := args.PrevLogIndex
		for idx > rf.lastIncludedIndex && rf.termAt(idx-1) == reply.ConflictTerm {
			idx--
		}
		reply.ConflictIndex = idx
		return
	}

	// Append, but only truncate the suffix if it actually conflicts —
	// an out-of-order/duplicate RPC carrying entries we already have must
	// not discard anything already agreed on past them.
	logChanged := false
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx > lastLogIndex {
			rf.log = append(rf.log, args.Entries[i:]...)
			logChanged = true
			break
		}
		if rf.termAt(idx) != e.Term {
			rf.log = append(rf.log[:rf.sliceIndex(idx)], args.Entries[i:]...)
			logChanged = true
			break
		}
	}
	if logChanged {
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(lastNew, args.LeaderCommit)
		rf.applyCond.Broadcast()
	}

	reply.Success = true
}

// InstallSnapshotArgs / InstallSnapshotReply are the wire structs for
// Figure 13's InstallSnapshot RPC. Unlike the paper this ships the whole
// snapshot in one RPC (no Offset/Done chunking) — a deliberate scope cut
// for this project, not a correctness gap: chunking only matters once a
// single snapshot is too large to hold in memory or send in one message.
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.state = follower
	rf.resetElectionDeadline()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return // stale: we already have an equal-or-newer snapshot
	}

	// Figure 13 step 6: if our log already has this exact entry, keep
	// whatever follows it — our own log/applier will catch up to it
	// through the normal per-command path, no need to force-feed the
	// state machine a whole snapshot it's already past.
	matched := args.LastIncludedIndex <= rf.lastLogIndex() &&
		rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm

	if matched {
		rf.log = append([]LogEntry{{Term: args.LastIncludedTerm}}, rf.log[rf.sliceIndex(args.LastIncludedIndex)+1:]...)
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
		if rf.lastApplied < args.LastIncludedIndex {
			rf.lastApplied = args.LastIncludedIndex
			rf.pendingSnapshot = &pendingSnapshot{index: args.LastIncludedIndex, term: args.LastIncludedTerm, data: args.Data}
		}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	rf.persistStateAndSnapshot(args.Data)
	rf.applyCond.Broadcast()
}
