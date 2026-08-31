package transport

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/locvth/mini-kv/raft"
	"github.com/locvth/mini-kv/transport/raftkvpb"
)

// rpcTimeout bounds how long a forwarded internal Raft RPC waits — for the
// connection to become ready (grpc.WaitForReady(true), see client.go) and
// for a reply — before giving up and leaving reply at its zero value. It
// must stay comfortably under simnet.Network's own 2s "handler hung"
// timeout (see simharness/simnet/simnet.go's processReq) so an
// unreachable peer is discovered through this timeout, not that one, and
// comfortably above a LAN round trip so a merely slow (or still-starting)
// peer isn't mistaken for a dead one.
const rpcTimeout = 1 * time.Second

// Raft is the simnet.Service raft.Raft dispatches every RPC to one
// specific remote peer through, in production. Its type name must be
// exactly "Raft": simnet derives the service name it dispatches to from
// reflect.Indirect(receiver).Type().Name(), and raft.go's Call sites are
// hardcoded to the strings "Raft.RequestVote" / "Raft.AppendEntries" /
// "Raft.InstallSnapshot" (mirroring how simharness/harness/config.go
// registers the real *raft.Raft under the same derived name for tests).
// It does not wrap or embed raft.Raft — it is a pure forwarder that turns
// each call into one outbound gRPC request to the peer it was built for.
//
// Each method has the shape simnet.MakeService requires: no error return,
// since transport failure is reported by simnet.ClientEnd.Call's bool
// return, not by the handler. On a dial/RPC failure this leaves reply at
// its zero value and returns — indistinguishable to raft.go from a reply
// carrying Term 0, which every caller already treats as harmless (a Term 0
// reply never wins a "reply.Term > rf.currentTerm" step-down; Success/
// VoteGranted false just costs one wasted retry next heartbeat or election
// timeout).
type Raft struct {
	client raftkvpb.RaftInternalClient
}

// newRaftForwarder builds the forwarder for one peer. conn is expected to
// come from grpc.NewClient, which dials lazily and reconnects on its own —
// this package does no connection health tracking of its own.
func newRaftForwarder(conn *grpc.ClientConn) *Raft {
	return &Raft{client: raftkvpb.NewRaftInternalClient(conn)}
}

func (f *Raft) RequestVote(args raft.RequestVoteArgs, reply *raft.RequestVoteReply) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := f.client.RequestVote(ctx, requestVoteArgsToPB(args), grpc.WaitForReady(true))
	if err != nil {
		return
	}
	*reply = requestVoteReplyFromPB(resp)
}

func (f *Raft) AppendEntries(args raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := f.client.AppendEntries(ctx, appendEntriesArgsToPB(args), grpc.WaitForReady(true))
	if err != nil {
		return
	}
	*reply = appendEntriesReplyFromPB(resp)
}

func (f *Raft) InstallSnapshot(args raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := f.client.InstallSnapshot(ctx, installSnapshotArgsToPB(args), grpc.WaitForReady(true))
	if err != nil {
		return
	}
	*reply = installSnapshotReplyFromPB(resp)
}
