package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"simharness/harness"
	"simharness/persister"
	"simharness/simnet"
)

// state is a server's role in the current term.
type state int

const (
	follower state = iota
	candidate
	leader
)

const (
	tickInterval       = 10 * time.Millisecond
	heartbeatInterval  = 100 * time.Millisecond
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 600 * time.Millisecond
)

// LogEntry is one slot in the replicated log. log[0] is a sentinel with
// Term 0 so PrevLogIndex/PrevLogTerm checks never need a special case for
// an empty log.
type LogEntry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex
	peers     []*simnet.ClientEnd
	persister *persister.Persister
	me        int
	applyCh   chan harness.ApplyMsg
	dead      int32 // atomic, set by Kill

	// persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	state            state
	electionDeadline time.Time

	// volatile state
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// Make constructs a Raft peer and starts its background election ticker.
// It matches harness.RaftMaker so it can be passed straight to
// harness.NewConfig.
func Make(peers []*simnet.ClientEnd, me int, ps *persister.Persister, applyCh chan harness.ApplyMsg) harness.RaftPeer {
	rf := &Raft{
		peers:       peers,
		persister:   ps,
		me:          me,
		applyCh:     applyCh,
		currentTerm: 0,
		votedFor:    -1,
		log:         []LogEntry{{Term: 0}},
		state:       follower,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
	}
	rf.resetElectionDeadline()
	go rf.ticker()
	return rf
}

// killed reports whether Kill has been called, so ticker and
// leaderHeartbeatLoop can stop themselves instead of looping forever after
// this peer is torn down (test crash/restart, or harness cleanup).
func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// Kill is a one-way flag, not a real teardown: nothing in the process can
// force-stop another goroutine, so background loops must poll killed() and
// exit on their own. atomic (not rf.mu) so it's cheap to check every tick
// without contending with the rest of the state.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

// Start appends command to the leader's own log. Replication to followers
// and commit/apply are wired up in the next milestone (log replication) —
// for leader election this only needs to satisfy harness.RaftPeer's shape.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	return len(rf.log) - 1, rf.currentTerm, true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {}

// resetElectionDeadline picks a fresh randomized deadline. Must be called
// with rf.mu held. Randomization spreads deadlines apart so one election
// timeout firing alone is (usually) enough to avoid a split vote.
func (rf *Raft) resetElectionDeadline() {
	span := electionTimeoutMax - electionTimeoutMin
	timeout := electionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
	rf.electionDeadline = time.Now().Add(timeout)
}

// becomeFollower steps down into a later term, clearing any vote cast in a
// previous (now stale) term. Must be called with rf.mu held.
func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = follower
}

// ticker drives election timeouts: it never resets a per-server timer,
// it just checks on every tick whether the deadline has passed.
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(tickInterval)

		rf.mu.Lock()
		if rf.state != leader && time.Now().After(rf.electionDeadline) {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// startElection must be called with rf.mu held.
func (rf *Raft) startElection() {
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionDeadline()

	term := rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	votes := 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply
			if !rf.peers[i].Call("Raft.RequestVote", args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.state != candidate || rf.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes*2 > len(rf.peers) {
				rf.becomeLeader()
			}
		}(i)
	}
}

// becomeLeader must be called with rf.mu held.
func (rf *Raft) becomeLeader() {
	if rf.state != candidate {
		return
	}
	rf.state = leader
	lastLogIndex := len(rf.log) - 1
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	go rf.leaderHeartbeatLoop(rf.currentTerm)
}

// leaderHeartbeatLoop sends heartbeats until this server steps down or is
// no longer leader in this term.
func (rf *Raft) leaderHeartbeatLoop(term int) {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.state != leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.broadcastHeartbeat(term)
		rf.mu.Unlock()

		time.Sleep(heartbeatInterval)
	}
}

// broadcastHeartbeat must be called with rf.mu held. Entries are always
// empty for now — log replication lands in the next milestone.
func (rf *Raft) broadcastHeartbeat(term int) {
	prevLogIndex := len(rf.log) - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	leaderCommit := rf.commitIndex

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: leaderCommit,
		}
		go func(i int) {
			var reply AppendEntriesReply
			if !rf.peers[i].Call("Raft.AppendEntries", args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
			}
		}(i)
	}
}
