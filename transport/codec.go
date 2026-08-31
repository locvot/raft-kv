package transport

import (
	"github.com/locvth/mini-kv/raft"
	"github.com/locvth/mini-kv/transport/raftkvpb"
)

// codec.go converts between raft/rpc.go's Args/Reply structs and their
// wire form in raftkv.proto. Both raft_service.go (inbound: proto -> raft
// call) and raft_forwarder.go (outbound: raft call -> proto) use these, so
// the translation is written once in each direction.

func logEntriesToPB(entries []raft.LogEntry) []*raftkvpb.LogEntry {
	out := make([]*raftkvpb.LogEntry, len(entries))
	for i, e := range entries {
		out[i] = &raftkvpb.LogEntry{
			Term:    int64(e.Term),
			Command: encodeCommand(e.Command.(Command)),
		}
	}
	return out
}

func logEntriesFromPB(entries []*raftkvpb.LogEntry) ([]raft.LogEntry, error) {
	out := make([]raft.LogEntry, len(entries))
	for i, e := range entries {
		cmd, err := decodeCommand(e.Command)
		if err != nil {
			return nil, err
		}
		out[i] = raft.LogEntry{Term: int(e.Term), Command: cmd}
	}
	return out, nil
}

func requestVoteArgsToPB(a raft.RequestVoteArgs) *raftkvpb.RequestVoteArgs {
	return &raftkvpb.RequestVoteArgs{
		Term:         int64(a.Term),
		CandidateId:  int64(a.CandidateId),
		LastLogIndex: int64(a.LastLogIndex),
		LastLogTerm:  int64(a.LastLogTerm),
	}
}

func requestVoteArgsFromPB(a *raftkvpb.RequestVoteArgs) raft.RequestVoteArgs {
	return raft.RequestVoteArgs{
		Term:         int(a.Term),
		CandidateId:  int(a.CandidateId),
		LastLogIndex: int(a.LastLogIndex),
		LastLogTerm:  int(a.LastLogTerm),
	}
}

func requestVoteReplyToPB(r raft.RequestVoteReply) *raftkvpb.RequestVoteReply {
	return &raftkvpb.RequestVoteReply{Term: int64(r.Term), VoteGranted: r.VoteGranted}
}

func requestVoteReplyFromPB(r *raftkvpb.RequestVoteReply) raft.RequestVoteReply {
	return raft.RequestVoteReply{Term: int(r.Term), VoteGranted: r.VoteGranted}
}

func appendEntriesArgsToPB(a raft.AppendEntriesArgs) *raftkvpb.AppendEntriesArgs {
	return &raftkvpb.AppendEntriesArgs{
		Term:         int64(a.Term),
		LeaderId:     int64(a.LeaderId),
		PrevLogIndex: int64(a.PrevLogIndex),
		PrevLogTerm:  int64(a.PrevLogTerm),
		Entries:      logEntriesToPB(a.Entries),
		LeaderCommit: int64(a.LeaderCommit),
	}
}

func appendEntriesArgsFromPB(a *raftkvpb.AppendEntriesArgs) (raft.AppendEntriesArgs, error) {
	entries, err := logEntriesFromPB(a.Entries)
	if err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	return raft.AppendEntriesArgs{
		Term:         int(a.Term),
		LeaderId:     int(a.LeaderId),
		PrevLogIndex: int(a.PrevLogIndex),
		PrevLogTerm:  int(a.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int(a.LeaderCommit),
	}, nil
}

func appendEntriesReplyToPB(r raft.AppendEntriesReply) *raftkvpb.AppendEntriesReply {
	return &raftkvpb.AppendEntriesReply{
		Term:          int64(r.Term),
		Success:       r.Success,
		ConflictTerm:  int64(r.ConflictTerm),
		ConflictIndex: int64(r.ConflictIndex),
	}
}

func appendEntriesReplyFromPB(r *raftkvpb.AppendEntriesReply) raft.AppendEntriesReply {
	return raft.AppendEntriesReply{
		Term:          int(r.Term),
		Success:       r.Success,
		ConflictTerm:  int(r.ConflictTerm),
		ConflictIndex: int(r.ConflictIndex),
	}
}

func installSnapshotArgsToPB(a raft.InstallSnapshotArgs) *raftkvpb.InstallSnapshotArgs {
	return &raftkvpb.InstallSnapshotArgs{
		Term:              int64(a.Term),
		LeaderId:          int64(a.LeaderId),
		LastIncludedIndex: int64(a.LastIncludedIndex),
		LastIncludedTerm:  int64(a.LastIncludedTerm),
		Data:              a.Data,
	}
}

func installSnapshotArgsFromPB(a *raftkvpb.InstallSnapshotArgs) raft.InstallSnapshotArgs {
	return raft.InstallSnapshotArgs{
		Term:              int(a.Term),
		LeaderId:          int(a.LeaderId),
		LastIncludedIndex: int(a.LastIncludedIndex),
		LastIncludedTerm:  int(a.LastIncludedTerm),
		Data:              a.Data,
	}
}

func installSnapshotReplyToPB(r raft.InstallSnapshotReply) *raftkvpb.InstallSnapshotReply {
	return &raftkvpb.InstallSnapshotReply{Term: int64(r.Term)}
}

func installSnapshotReplyFromPB(r *raftkvpb.InstallSnapshotReply) raft.InstallSnapshotReply {
	return raft.InstallSnapshotReply{Term: int(r.Term)}
}
