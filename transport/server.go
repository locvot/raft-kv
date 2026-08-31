// Package transport wires raft.Raft and storage.Store into a real,
// multi-process cluster over gRPC — the "adapter" the RPC/transport
// section of DECISIONS.md describes, implementing raft.go's Call-shaped
// dependency with real network I/O instead of simnet's in-process one.
//
// Peer-to-peer Raft RPCs and client-facing KV RPCs are both plain gRPC:
// RaftInternal (RequestVote/AppendEntries/InstallSnapshot) is dispatched
// to the local raft.Raft directly; each outbound call to a remote peer is
// funneled back through simnet's own ClientEnd/Server/Service machinery
// (see raft_forwarder.go) because raft.Make's signature — fixed by
// harness.RaftMaker, so raft.go can also run unmodified under the test
// harness — only accepts []*simnet.ClientEnd, which only simnet.Network
// can construct.
package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/locvth/mini-kv/raft"
	"github.com/locvth/mini-kv/storage"
	"github.com/locvth/mini-kv/transport/raftkvpb"

	"simharness/harness"
	"simharness/persister"
	"simharness/simnet"
)

// applyTimeout bounds how long Put/Delete wait for their entry to commit
// and apply before giving up and telling the client to retry. It's a
// multiple of raft's own heartbeatInterval so a leader that's merely slow
// this round still has time to succeed before a client gives up on it.
const applyTimeout = 2 * time.Second

// errNotLeader is returned by awaitApply when the entry a caller was
// waiting on either never committed within applyTimeout (most likely: this
// node lost leadership before it could) or committed at a different term
// than the one Start returned — proof some other leader's entry landed at
// that index instead of this caller's.
var errNotLeader = errors.New("transport: not leader (or lost leadership before commit)")

// Config is the static shape of the cluster this Server joins. Membership
// is fixed for the process's lifetime — see project_no_membership_change
// in memory / DECISIONS.md: dynamic reconfiguration was scoped out as too
// complex for this project's goals.
type Config struct {
	// Peers holds every peer's address (host:port), index-aligned with
	// Raft peer indices. Peers[Me] is this process's own listen address.
	Peers []string
	Me    int
	// DataDir is where storage.Store keeps its WAL/SSTables/manifest.
	DataDir string
}

// waiter is what a Put/Delete RPC handler blocks on while its entry makes
// its way through the log. term is the term Start returned the entry at —
// if the entry that actually lands at this index carries a different
// term, some other leader's entry won by winning an election first, and
// this waiter's own command never committed.
type waiter struct {
	term int
	ch   chan error
}

// Server owns one Raft peer's full stack: the gRPC endpoints for both
// services (RaftInternal for peer-to-peer consensus, KV for clients), the
// raft.Raft instance driving them, and the storage.Store state machine
// underneath. One process runs exactly one Server.
type Server struct {
	raftkvpb.UnimplementedRaftInternalServer
	raftkvpb.UnimplementedKVServer

	cfg     Config
	rf      *raft.Raft
	store   *storage.Store
	applyCh chan harness.ApplyMsg

	conns []*grpc.ClientConn // index-aligned with cfg.Peers; nil at cfg.Me

	mu          sync.Mutex
	waiters     map[int]waiter
	knownLeader int // index into cfg.Peers naming who last proved itself leader to us, or -1

	closed     chan struct{}
	closeOnce  sync.Once
	grpcServer *grpc.Server
}

// NewServer opens the on-disk state machine, dials every peer, and starts
// the local raft.Raft peer and its apply loop. It does not start serving
// gRPC yet — call Serve for that, once the caller is ready to block (or
// wants to run it in its own goroutine).
func NewServer(cfg Config) (*Server, error) {
	if cfg.Me < 0 || cfg.Me >= len(cfg.Peers) {
		return nil, fmt.Errorf("transport: Me=%d out of range for %d peers", cfg.Me, len(cfg.Peers))
	}

	store, err := storage.Open(cfg.DataDir, 0)
	if err != nil {
		return nil, fmt.Errorf("transport: open store: %w", err)
	}

	s := &Server{
		cfg:         cfg,
		store:       store,
		applyCh:     make(chan harness.ApplyMsg),
		conns:       make([]*grpc.ClientConn, len(cfg.Peers)),
		waiters:     make(map[int]waiter),
		knownLeader: -1,
		closed:      make(chan struct{}),
	}

	rn := simnet.MakeNetwork()
	peerEnds := make([]*simnet.ClientEnd, len(cfg.Peers))
	for j, addr := range cfg.Peers {
		peerEnds[j] = rn.MakeEnd(fmt.Sprintf("out-%d", j))
		if j == cfg.Me {
			continue // raft.go never calls peers[me] — see broadcastAppendEntries/startElection
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("transport: dial peer %d (%s): %w", j, addr, err)
		}
		s.conns[j] = conn

		srv := simnet.MakeServer()
		srv.AddService(simnet.MakeService(newRaftForwarder(conn)))
		serverName := fmt.Sprintf("peer-%d", j)
		rn.AddServer(serverName, srv)
		rn.Connect(fmt.Sprintf("out-%d", j), serverName)
	}

	ps := persister.MakePersister()
	raftPeer := raft.Make(peerEnds, cfg.Me, ps, s.applyCh)
	// raft.Make always constructs and returns a *raft.Raft; the assertion
	// only exists because it's typed as harness.RaftPeer to match
	// harness.RaftMaker, which doesn't expose RequestVote/AppendEntries/
	// InstallSnapshot — this Server needs those to serve RaftInternal.
	s.rf = raftPeer.(*raft.Raft)

	go s.applyLoop()

	return s, nil
}

// Serve starts the gRPC server on lis and blocks until it stops (via
// Close, or a listener error).
func (s *Server) Serve(lis net.Listener) error {
	s.grpcServer = grpc.NewServer()
	raftkvpb.RegisterRaftInternalServer(s.grpcServer, s)
	raftkvpb.RegisterKVServer(s.grpcServer, s)
	return s.grpcServer.Serve(lis)
}

// Close stops serving, tears down the Raft peer and its dialed peer
// connections, and closes the state machine. Safe to call more than once.
func (s *Server) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.closed)
		if s.grpcServer != nil {
			s.grpcServer.GracefulStop()
		}
		s.rf.Kill()
		for _, c := range s.conns {
			if c != nil {
				c.Close()
			}
		}
		err = s.store.Close()
	})
	return err
}

// applyLoop drains raft's applyCh and applies every committed command to
// the local state machine, then wakes whichever Put/Delete handler (if
// any, and if still waiting — awaitApply may have already timed out) is
// blocked on that index.
func (s *Server) applyLoop() {
	for {
		select {
		case msg := <-s.applyCh:
			s.handleApply(msg)
		case <-s.closed:
			return
		}
	}
}

func (s *Server) handleApply(msg harness.ApplyMsg) {
	if !msg.CommandValid {
		// SnapshotValid: log compaction is never triggered in M4 (nothing
		// calls s.rf.Snapshot), so this path is unreachable in practice —
		// kept so the apply loop doesn't silently wedge if that changes.
		return
	}

	cmd := msg.Command.(Command)
	var applyErr error
	switch cmd.Op {
	case opPut:
		applyErr = s.store.Put(cmd.Key, cmd.Value)
	case opDelete:
		applyErr = s.store.Delete(cmd.Key)
	default:
		applyErr = fmt.Errorf("transport: unknown command op %d", cmd.Op)
	}

	s.mu.Lock()
	w, ok := s.waiters[msg.CommandIndex]
	if ok {
		delete(s.waiters, msg.CommandIndex)
	}
	s.mu.Unlock()
	if !ok {
		return
	}
	if w.term != msg.CommandTerm {
		// A different leader's entry landed at this index — this waiter's
		// own command never committed.
		w.ch <- errNotLeader
		return
	}
	w.ch <- applyErr
}

// awaitApply blocks until the entry Start placed at index (in term term)
// is applied, ctx is done, or applyTimeout elapses.
func (s *Server) awaitApply(ctx context.Context, index, term int) error {
	ch := make(chan error, 1)
	s.mu.Lock()
	s.waiters[index] = waiter{term: term, ch: ch}
	s.mu.Unlock()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		s.dropWaiter(index)
		return ctx.Err()
	case <-time.After(applyTimeout):
		s.dropWaiter(index)
		return errNotLeader
	}
}

func (s *Server) dropWaiter(index int) {
	s.mu.Lock()
	delete(s.waiters, index)
	s.mu.Unlock()
}

// setKnownLeader records id as the peer this node most recently confirmed
// is leader — called from raft_service.go whenever an AppendEntries or
// InstallSnapshot we received didn't get rejected for having a stale term.
func (s *Server) setKnownLeader(id int) {
	s.mu.Lock()
	s.knownLeader = id
	s.mu.Unlock()
}

// leaderHint returns the address of the peer this node last confirmed is
// leader, or "" if it doesn't know (e.g. an election is in progress).
func (s *Server) leaderHint() string {
	s.mu.Lock()
	id := s.knownLeader
	s.mu.Unlock()
	if id < 0 || id >= len(s.cfg.Peers) {
		return ""
	}
	return s.cfg.Peers[id]
}
