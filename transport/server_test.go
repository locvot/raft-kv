package transport

import (
	"bytes"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/locvth/mini-kv/transport/raftkvpb"
)

// TestClusterPutGetAcrossNodes is M4's own "done when" bar from
// raftkv.plan.md: Put on one node, Get the same value back through a
// different node, over real gRPC (loopback TCP, not simnet).
func TestClusterPutGetAcrossNodes(t *testing.T) {
	c := newTestCluster(t, 3)
	cli := c.client()
	defer cli.Close()

	ctx := ctxTimeout(t, 5*time.Second)
	if err := cli.Put(ctx, "hello", []byte("world")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	value, ok, err := cli.Get(ctxTimeout(t, 5*time.Second), "hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || !bytes.Equal(value, []byte("world")) {
		t.Fatalf("Get(hello) = (%q, %v), want (world, true)", value, ok)
	}

	if err := cli.Delete(ctxTimeout(t, 5*time.Second), "hello"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, ok, err = cli.Get(ctxTimeout(t, 5*time.Second), "hello")
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if ok {
		t.Fatalf("Get(hello) after Delete: ok = true, want false")
	}
}

// TestClusterFollowerRedirectsToLeader hits every node's KV service
// directly (bypassing Client's own retry logic) and checks that exactly
// one reports itself as leader and every other one names it correctly in
// LeaderHint — API.md's "một node không phải leader trả về lỗi kèm địa chỉ
// leader hiện tại".
func TestClusterFollowerRedirectsToLeader(t *testing.T) {
	c := newTestCluster(t, 3)
	cli := c.client()
	// Force an election/log entry so every node has processed at least one
	// AppendEntries round and can report a leader.
	if err := cli.Put(ctxTimeout(t, 5*time.Second), "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cli.Close()

	leaderAddr := ""
	for _, addr := range c.peers {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial %s: %v", addr, err)
		}
		defer conn.Close()
		resp, err := raftkvpb.NewKVClient(conn).Get(ctxTimeout(t, 2*time.Second), &raftkvpb.GetRequest{Key: "k"})
		if err != nil {
			t.Fatalf("Get on %s: %v", addr, err)
		}
		if !resp.NotLeader {
			if leaderAddr != "" {
				t.Fatalf("two nodes both claim leadership: %s and %s", leaderAddr, addr)
			}
			leaderAddr = addr
		}
	}
	if leaderAddr == "" {
		t.Fatalf("no node claims leadership")
	}

	for _, addr := range c.peers {
		if addr == leaderAddr {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial %s: %v", addr, err)
		}
		defer conn.Close()
		resp, err := raftkvpb.NewKVClient(conn).Get(ctxTimeout(t, 2*time.Second), &raftkvpb.GetRequest{Key: "k"})
		if err != nil {
			t.Fatalf("Get on %s: %v", addr, err)
		}
		if !resp.NotLeader || resp.LeaderHint != leaderAddr {
			t.Fatalf("follower %s: NotLeader=%v LeaderHint=%q, want NotLeader=true LeaderHint=%q",
				addr, resp.NotLeader, resp.LeaderHint, leaderAddr)
		}
	}
}

// TestClusterSurvivesLeaderKill covers M4's fault-tolerance-adjacent
// requirement implicit in "leader redirect": the cluster must still be
// able to serve writes through a new leader after the old one is gone.
func TestClusterSurvivesLeaderKill(t *testing.T) {
	c := newTestCluster(t, 3)
	cli := c.client()
	defer cli.Close()

	if err := cli.Put(ctxTimeout(t, 5*time.Second), "before", []byte("v1")); err != nil {
		t.Fatalf("Put before kill: %v", err)
	}

	killed := c.killLeader()

	// Give the remaining two peers time to elect a new leader.
	deadline := time.Now().Add(5 * time.Second)
	var putErr error
	for time.Now().Before(deadline) {
		putErr = cli.Put(ctxTimeout(t, time.Second), "after", []byte("v2"))
		if putErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if putErr != nil {
		t.Fatalf("Put after killing leader %d: %v", killed, putErr)
	}

	value, ok, err := cli.Get(ctxTimeout(t, 5*time.Second), "after")
	if err != nil {
		t.Fatalf("Get after kill: %v", err)
	}
	if !ok || !bytes.Equal(value, []byte("v2")) {
		t.Fatalf("Get(after) = (%q, %v), want (v2, true)", value, ok)
	}
}
