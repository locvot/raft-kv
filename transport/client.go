package transport

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/locvth/mini-kv/transport/raftkvpb"
)

// retryBackoff is how long Client waits before trying again after a peer
// it picked blindly (no leader hint to follow — that peer doesn't know
// either, e.g. mid-election) turned out wrong, or was unreachable. It
// matters most right when a cluster starts or just lost its leader: an
// election takes at least raft's electionTimeoutMin (300ms), so retrying
// instantly would just burn through every peer in the list, over and
// over, before anyone has had a chance to win one.
const retryBackoff = 100 * time.Millisecond

// attemptTimeout bounds a single attempt within retry, separately from the
// ctx a caller passes to Get/Put/Delete. Without this, one unlucky pick —
// a peer that's dead or badly partitioned — would combine with
// grpc.WaitForReady(true) to block for the caller's *entire* remaining
// budget waiting for that one connection to become ready, leaving no time
// to round-robin to a peer that actually works.
const attemptTimeout = 1 * time.Second

// ErrNoLeader is returned once a Get/Put/Delete call's context runs out
// without any peer answering as leader — most likely an election is still
// in progress, or fewer than a majority of peers are reachable.
var ErrNoLeader = errors.New("transport: could not find a leader before the context expired")

// Client is a small redirect-following client for the KV service, matching
// the Get/Put/Delete shape from API.md. Callers don't need to know which
// peer is leader in advance: on a NotLeader reply it follows the returned
// hint, or round-robins the configured peer list when the hint is empty.
type Client struct {
	peers []string

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn // lazily dialed, address -> conn
	next  int                         // round-robin cursor into peers
}

// NewClient builds a Client against the given peer addresses. It doesn't
// dial anything until the first call.
func NewClient(peers []string) *Client {
	return &Client{peers: peers, conns: make(map[string]*grpc.ClientConn)}
}

// Close tears down every connection this Client has dialed so far.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *Client) kv(addr string) (raftkvpb.KVClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		return raftkvpb.NewKVClient(conn), nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return raftkvpb.NewKVClient(conn), nil
}

// nextAddr picks where to send the next attempt: hint if the previous
// reply named one, else the next peer in round-robin order.
func (c *Client) nextAddr(hint string) string {
	if hint != "" {
		return hint
	}
	c.mu.Lock()
	addr := c.peers[c.next%len(c.peers)]
	c.next++
	c.mu.Unlock()
	return addr
}

// redirectable is the shape every KV RPC response shares: whether the peer
// answering says it isn't leader, and if so, who it thinks is.
type redirectable interface {
	GetNotLeader() bool
	GetLeaderHint() string
}

// retry repeatedly calls call against a chosen peer until it returns a
// reply that isn't a NotLeader redirect, or ctx is done. Every call also
// passes grpc.WaitForReady(true) (callers wire that into call themselves):
// grpc-go's default is fail-fast, returning Unavailable immediately if the
// target connection isn't in the READY state yet rather than waiting for
// it — exactly the state a just-dialed peer sits in for the first few
// milliseconds of a real multi-process startup, or right after a
// connection blips. Without it, a client launched at the same moment as
// the cluster itself would fail its very first attempt against every peer
// before any connection had a chance to come up.
func retry[T redirectable](ctx context.Context, c *Client, call func(context.Context, raftkvpb.KVClient) (T, error)) (T, error) {
	var zero T
	hint := ""
	for {
		if ctx.Err() != nil {
			return zero, ErrNoLeader
		}

		addr := c.nextAddr(hint)
		hint = ""
		cli, err := c.kv(addr)
		if err == nil {
			attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
			var resp T
			resp, err = call(attemptCtx, cli)
			cancel()
			if err == nil {
				if !resp.GetNotLeader() {
					return resp, nil
				}
				hint = resp.GetLeaderHint()
				if hint != "" {
					continue // a specific peer to try next — no need to wait
				}
			}
		}

		select {
		case <-ctx.Done():
			return zero, ErrNoLeader
		case <-time.After(retryBackoff):
		}
	}
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	resp, err := retry(ctx, c, func(ctx context.Context, cli raftkvpb.KVClient) (*raftkvpb.GetResponse, error) {
		return cli.Get(ctx, &raftkvpb.GetRequest{Key: key}, grpc.WaitForReady(true))
	})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Ok, nil
}

func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	_, err := retry(ctx, c, func(ctx context.Context, cli raftkvpb.KVClient) (*raftkvpb.PutResponse, error) {
		return cli.Put(ctx, &raftkvpb.PutRequest{Key: key, Value: value}, grpc.WaitForReady(true))
	})
	return err
}

func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := retry(ctx, c, func(ctx context.Context, cli raftkvpb.KVClient) (*raftkvpb.DeleteResponse, error) {
		return cli.Delete(ctx, &raftkvpb.DeleteRequest{Key: key}, grpc.WaitForReady(true))
	})
	return err
}
