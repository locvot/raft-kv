package transport

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/locvth/mini-kv/raft"
	"github.com/locvth/mini-kv/transport/raftkvpb"
)

// raft_service.go is the inbound half of RaftInternal: it decodes each
// proto request, calls straight into the local raft.Raft (no simnet
// involved — that machinery is only needed on the outbound side, see
// raft_forwarder.go, because raft.Make's peer type forces it), and encodes
// the reply back. AppendEntries/InstallSnapshot also update knownLeader so
// Get/Put/Delete can hand a client the right redirect address.
//
// Every handler refuses up front while SetPartitioned(true) is in effect
// (see server.go) — the inbound half of M5's fault-injection primitive: a
// partitioned node must not process RPCs arriving from peers that (by the
// simulation) can no longer reach it, the same way it can no longer reach
// them via its own outbound simnet ends.
var errPartitioned = status.Error(codes.Unavailable, "transport: node is partitioned (fault injection)")

func (s *Server) RequestVote(ctx context.Context, req *raftkvpb.RequestVoteArgs) (*raftkvpb.RequestVoteReply, error) {
	if s.isPartitioned() {
		return nil, errPartitioned
	}
	args := requestVoteArgsFromPB(req)
	var reply raft.RequestVoteReply
	s.rf.RequestVote(args, &reply)
	return requestVoteReplyToPB(reply), nil
}

func (s *Server) AppendEntries(ctx context.Context, req *raftkvpb.AppendEntriesArgs) (*raftkvpb.AppendEntriesReply, error) {
	if s.isPartitioned() {
		return nil, errPartitioned
	}
	args, err := appendEntriesArgsFromPB(req)
	if err != nil {
		return nil, err
	}
	var reply raft.AppendEntriesReply
	s.rf.AppendEntries(args, &reply)
	if reply.Term == args.Term {
		// Not rejected for a stale term: args.LeaderId is a real leader as
		// of at least this term.
		s.setKnownLeader(args.LeaderId)
	}
	return appendEntriesReplyToPB(reply), nil
}

func (s *Server) InstallSnapshot(ctx context.Context, req *raftkvpb.InstallSnapshotArgs) (*raftkvpb.InstallSnapshotReply, error) {
	if s.isPartitioned() {
		return nil, errPartitioned
	}
	args := installSnapshotArgsFromPB(req)
	var reply raft.InstallSnapshotReply
	s.rf.InstallSnapshot(args, &reply)
	if reply.Term == args.Term {
		s.setKnownLeader(args.LeaderId)
	}
	return installSnapshotReplyToPB(reply), nil
}
