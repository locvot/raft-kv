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

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
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

	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.ConflictIndex = len(rf.log)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		idx := args.PrevLogIndex
		for idx > 0 && rf.log[idx-1].Term == reply.ConflictTerm {
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
		if idx >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
			logChanged = true
			break
		}
		if rf.log[idx].Term != e.Term {
			rf.log = append(rf.log[:idx], args.Entries[i:]...)
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
