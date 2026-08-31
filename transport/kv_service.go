package transport

import (
	"context"

	"github.com/locvth/mini-kv/transport/raftkvpb"
)

// kv_service.go is the client-facing KV service: API.md's Get/Put/Delete,
// redirect-on-not-leader per DECISIONS.md.
//
// Put/Delete go through the log (Start, then wait for the entry to
// apply — see awaitApply in server.go) so every acknowledged write is
// replicated to a majority before the client hears back.
//
// Get answers from the leader's local state directly, without a log
// entry. That's the "simple, correct" M4 baseline from API.md, not a full
// ReadIndex/lease-read protocol (lease read is listed as a stretch goal
// in raftkv.plan.md's M4 section) — it trades a narrow window of
// staleness for simplicity: a leader that has just lost a majority (e.g.
// to a partition) but hasn't yet stepped down can still answer a Get from
// its own last-known state for up to one election timeout after losing
// the ability to commit anything new.

func (s *Server) Get(ctx context.Context, req *raftkvpb.GetRequest) (*raftkvpb.GetResponse, error) {
	if _, isLeader := s.rf.GetState(); !isLeader {
		return &raftkvpb.GetResponse{NotLeader: true, LeaderHint: s.leaderHint()}, nil
	}
	value, ok, err := s.store.Get(req.Key)
	if err != nil {
		return nil, err
	}
	return &raftkvpb.GetResponse{Value: value, Ok: ok}, nil
}

func (s *Server) Put(ctx context.Context, req *raftkvpb.PutRequest) (*raftkvpb.PutResponse, error) {
	index, term, isLeader := s.rf.Start(Command{Op: opPut, Key: req.Key, Value: req.Value})
	if !isLeader {
		return &raftkvpb.PutResponse{NotLeader: true, LeaderHint: s.leaderHint()}, nil
	}
	if err := s.awaitApply(ctx, index, term); err != nil {
		if err == errNotLeader {
			return &raftkvpb.PutResponse{NotLeader: true, LeaderHint: s.leaderHint()}, nil
		}
		return nil, err
	}
	return &raftkvpb.PutResponse{}, nil
}

func (s *Server) Delete(ctx context.Context, req *raftkvpb.DeleteRequest) (*raftkvpb.DeleteResponse, error) {
	index, term, isLeader := s.rf.Start(Command{Op: opDelete, Key: req.Key})
	if !isLeader {
		return &raftkvpb.DeleteResponse{NotLeader: true, LeaderHint: s.leaderHint()}, nil
	}
	if err := s.awaitApply(ctx, index, term); err != nil {
		if err == errNotLeader {
			return &raftkvpb.DeleteResponse{NotLeader: true, LeaderHint: s.leaderHint()}, nil
		}
		return nil, err
	}
	return &raftkvpb.DeleteResponse{}, nil
}
