package transport

import (
	"bytes"
	"fmt"
	"sync/atomic"
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

// TestClusterSurvivesLeaderKillMidWrite is M5's first fault-tolerance
// check from raftkv.plan.md ("kill leader giữa lúc đang ghi → verify
// cluster bầu leader mới và tiếp tục nhận ghi"): unlike
// TestClusterSurvivesLeaderKill above, the leader is killed while a write
// loop is actively hammering the cluster, not between two isolated calls
// — so the kill can land mid-RPC, mid-retry-backoff, or anywhere else in
// the write loop's cycle.
func TestClusterSurvivesLeaderKillMidWrite(t *testing.T) {
	c := newTestCluster(t, 3)
	cli := c.client()
	defer cli.Close()

	var successes atomic.Int64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			key := fmt.Sprintf("k%d", i)
			if err := cli.Put(ctxTimeout(t, time.Second), key, []byte("v")); err == nil {
				successes.Add(1)
			}
		}
	}()

	// Let the loop get a handful of commits in first, so the kill below
	// genuinely lands mid-stream rather than before the loop even started.
	for successes.Load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}
	killed := c.killLeader()
	beforeKill := successes.Load()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && successes.Load() <= beforeKill {
		time.Sleep(50 * time.Millisecond)
	}
	close(stop)
	<-done

	if successes.Load() <= beforeKill {
		t.Fatalf("write loop made no further progress after killing leader %d mid-write: stuck at %d successes", killed, beforeKill)
	}
	t.Logf("killed leader %d mid-write at %d successful writes; write loop reached %d total after a new leader took over", killed, beforeKill, successes.Load())
}

// TestClusterPartitionedMinorityNeverElectsLeader is M5's second
// fault-tolerance check from raftkv.plan.md ("cô lập 1 node → verify
// minority không tự nhận là leader"). It partitions one follower via
// Server.SetPartitioned (see server.go's doc comment for why this
// simulates a real network partition without root/iptables) and polls it
// for several election-timeout windows: a lone node is 1 vote out of 3,
// so it must never reach majority and declare itself leader, even though
// its own ticker keeps trying. Meanwhile the still-connected two-node
// majority must keep serving writes throughout, and the cluster must be
// healthy again once the partition heals.
func TestClusterPartitionedMinorityNeverElectsLeader(t *testing.T) {
	c := newTestCluster(t, 3)
	cli := c.client()
	defer cli.Close()

	if err := cli.Put(ctxTimeout(t, 5*time.Second), "before", []byte("v1")); err != nil {
		t.Fatalf("Put before partition: %v", err)
	}

	isolated := -1
	for i, srv := range c.servers {
		if _, isLeader := srv.rf.GetState(); !isLeader {
			isolated = i
			break
		}
	}
	if isolated < 0 {
		t.Fatalf("no follower found to isolate")
	}
	c.servers[isolated].SetPartitioned(true)
	t.Logf("partitioned node %d", isolated)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, isLeader := c.servers[isolated].rf.GetState(); isLeader {
			t.Fatalf("partitioned node %d declared itself leader — a 1-of-3 minority must never win an election", isolated)
		}
		if err := cli.Put(ctxTimeout(t, time.Second), "during", []byte("v2")); err != nil {
			t.Fatalf("Put on the majority side while node %d was partitioned: %v", isolated, err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	c.servers[isolated].SetPartitioned(false)
	t.Logf("healed partition on node %d", isolated)

	if err := cli.Put(ctxTimeout(t, 5*time.Second), "after", []byte("v3")); err != nil {
		t.Fatalf("Put after healing partition: %v", err)
	}
}
