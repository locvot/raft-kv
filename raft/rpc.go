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

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// A valid heartbeat/append from the current term's leader: recognize
	// it even as a candidate (Raft Figure 2's "convert to follower" rule)
	// and postpone our own next election.
	rf.state = follower
	rf.resetElectionDeadline()

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// Log replication (append/truncate on conflict, advance commitIndex)
	// lands in the next milestone; entries are always empty for now.
	reply.Success = true
}
