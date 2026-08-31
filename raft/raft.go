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

	// persistent state for the snapshot boundary — see snapshot.go's doc
	// comment for how these shift rf.log's indexing. Zero/zero means "no
	// snapshot yet," which is also what a peer that has never snapshotted
	// looks like after readPersist restores nothing.
	lastIncludedIndex int
	lastIncludedTerm  int

	state            state
	electionDeadline time.Time

	// volatile state
	commitIndex int
	lastApplied int
	applyCond   *sync.Cond // signaled whenever commitIndex advances or rf is killed

	// pendingSnapshot is non-nil when InstallSnapshot has updated rf's log
	// but applier hasn't yet delivered the snapshot to the state machine.
	pendingSnapshot *pendingSnapshot

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// dispatchSeq[i] is the next sequence number to hand to a replicateTo
	// round for peer i; acceptedSeq[i] is the sequence number of the last
	// round whose reply actually updated nextIndex/matchIndex[i]. Together
	// they replace comparing raw nextIndex values to detect a stale reply
	// — nextIndex can revisit the same integer through unrelated
	// backtrack/catch-up history (an ABA problem), but a sequence number
	// assigned once per dispatch never repeats, so "seq <= accepted" is an
	// unambiguous "something newer already won" regardless of what
	// nextIndex happens to equal at any given moment.
	dispatchSeq []int
	acceptedSeq []int
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
		dispatchSeq: make([]int, len(peers)),
		acceptedSeq: make([]int, len(peers)),
	}
	for i := range rf.acceptedSeq {
		rf.acceptedSeq[i] = -1 // below the first dispatched seq (0), so round 0 isn't rejected as stale
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.readPersist(ps.ReadRaftState())
	// Anything at or before lastIncludedIndex is covered by the snapshot;
	// the service layer is expected to load it from ps.ReadSnapshot()
	// itself rather than have Raft redeliver it over applyCh.
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex
	rf.resetElectionDeadline()
	go rf.ticker()
	go rf.applier()
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
// exit on their own. The flag itself is atomic (not rf.mu) so it's cheap to
// check every tick without contending with the rest of the state, but the
// wakeup must go out under rf.mu: applier holds rf.mu for its whole
// check-then-Wait sequence, so a Broadcast that isn't serialized against
// that lock could land in the gap between applier's condition check and
// its Wait() call — found no one waiting yet, and lost for good, leaking
// the goroutine.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

// Start appends command to the leader's own log and kicks off replication
// to followers immediately, rather than waiting for the next heartbeat
// tick. It returns the index the entry will land at *if* it commits — the
// caller (harness.Config.One, or a real client) must still watch applyCh /
// poll for that index to know when (or whether) it actually did.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	index := rf.lastLogIndex()
	rf.persist()
	rf.broadcastAppendEntries(rf.currentTerm)
	return index, rf.currentTerm, true
}

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
	rf.persist()
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
	rf.persist()
	rf.resetElectionDeadline()

	term := rf.currentTerm
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.termAt(lastLogIndex)
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
	lastLogIndex := rf.lastLogIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	go rf.leaderHeartbeatLoop(rf.currentTerm)
}

// leaderHeartbeatLoop periodically re-triggers replication until this
// server steps down or is no longer leader in this term. It is what
// retries a follower that fell behind or was momentarily unreachable —
// Start already sent one round immediately, this is just the backstop.
func (rf *Raft) leaderHeartbeatLoop(term int) {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.state != leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.broadcastAppendEntries(term)
		rf.mu.Unlock()

		time.Sleep(heartbeatInterval)
	}
}

// broadcastAppendEntries fans out one AppendEntries round to every peer.
// Must be called with rf.mu held; each per-peer RPC is built and sent by
// replicateTo, which re-reads state under its own lock acquisition so it
// always ships the freshest nextIndex/log even if this call was queued for
// a while before running.
func (rf *Raft) broadcastAppendEntries(term int) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateTo(i, term)
	}
}

// replicateTo sends one AppendEntries RPC to peer i carrying everything
// from rf.nextIndex[i] onward, and folds the reply back into nextIndex/
// matchIndex. On a log mismatch it uses the follower's ConflictTerm/
// ConflictIndex (Figure 2's optimization) to skip back over a whole
// conflicting term in one round trip instead of decrementing by one entry
// at a time — without it, a follower that fell far behind (TestBackup)
// takes one heartbeat round trip per missing entry to catch up.
func (rf *Raft) replicateTo(i int, term int) {
	rf.mu.Lock()
	if rf.killed() || rf.state != leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[i] - 1
	if prevLogIndex < rf.lastIncludedIndex {
		// The entry this peer needs next was already compacted out of our
		// log — only a snapshot has it anymore.
		rf.sendInstallSnapshot(i, term)
		return
	}
	prevLogTerm := rf.termAt(prevLogIndex)
	entries := append([]LogEntry(nil), rf.log[rf.sliceIndex(prevLogIndex)+1:]...)
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	seq := rf.dispatchSeq[i]
	rf.dispatchSeq[i]++
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if !rf.peers[i].Call("Raft.AppendEntries", args, &reply) {
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
	// Stale reply: some round dispatched after this one already updated
	// nextIndex/matchIndex[i]. Comparing nextIndex[i] itself to detect
	// staleness doesn't work — it goes down on backtrack and back up on
	// catch-up, so under heavy reordering (SetLongReordering) it can
	// coincidentally revisit the exact value this old reply was built
	// against, an ABA problem. seq is a per-peer counter handed out once
	// per dispatch and never reused, so "seq <= acceptedSeq[i]" is an
	// unambiguous "something newer already won" no matter what nextIndex
	// happens to equal right now.
	if seq <= rf.acceptedSeq[i] {
		return
	}
	rf.acceptedSeq[i] = seq

	if reply.Success {
		match := prevLogIndex + len(entries)
		rf.matchIndex[i] = match
		rf.nextIndex[i] = match + 1
		rf.advanceCommitIndex()
		return
	}

	var next int
	switch {
	case reply.ConflictTerm < 0:
		// Follower's log is shorter than PrevLogIndex.
		next = reply.ConflictIndex
	default:
		// Follower has ConflictTerm at ConflictIndex; if the leader also
		// has ConflictTerm somewhere, retry from just past its last entry
		// in that term, else fall back to the follower's ConflictIndex.
		next = -1
		for j := len(rf.log) - 1; j > 0; j-- {
			if rf.log[j].Term == reply.ConflictTerm {
				next = j + 1
				break
			}
		}
		if next == -1 {
			next = reply.ConflictIndex
		}
	}
	if next < 1 {
		next = 1
	}
	rf.nextIndex[i] = next
}

// advanceCommitIndex checks whether any higher index is now on a majority
// of peers and, if so, commits it. Must be called with rf.mu held.
//
// It only commits entries from the leader's *current* term directly — an
// older-term entry that reaches a majority is committed implicitly, as a
// side effect of the current-term entry ahead of it committing (Raft
// §5.4.2 / the paper's Figure 8 scenario). Committing an old-term entry as
// soon as it merely reaches a majority is the classic bug: a later leader
// that never saw it get committed can still overwrite it, which is exactly
// what TestFigure8 in the persistence milestone will probe for.
func (rf *Raft) advanceCommitIndex() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.termAt(n) != rf.currentTerm {
			continue
		}
		count := 1 // rf.me always has entry n in its own log
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}
		if count*2 > len(rf.peers) {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
			return
		}
	}
}

// applier delivers every newly committed entry to applyCh, in order, one
// at a time. It runs as its own goroutine (rather than being driven
// inline from AppendEntries/advanceCommitIndex) because sending on applyCh
// can block on the consumer, and nothing that holds rf.mu can afford to
// block — it would stall RPC handlers and the ticker along with it.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.pendingSnapshot != nil {
			snap := rf.pendingSnapshot
			rf.pendingSnapshot = nil
			msg := harness.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snap.data,
				SnapshotTerm:  snap.term,
				SnapshotIndex: snap.index,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			continue
		}
		if rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			continue
		}
		rf.lastApplied++
		entry := rf.log[rf.sliceIndex(rf.lastApplied)]
		msg := harness.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: rf.lastApplied,
			CommandTerm:  entry.Term,
		}
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
	}
}
