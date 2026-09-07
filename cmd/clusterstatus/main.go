// Command clusterstatus is an M5 evidence-gathering tool: it dials every
// peer directly (bypassing transport.Client's leader-following retry) and
// reports, for each one, whether it currently claims to be leader,
// believes some other address is leader, or is unreachable — exactly what
// TestClusterFollowerRedirectsToLeader checks inside a single test binary,
// but against real separate cmd/server processes.
//
// One-shot:
//
//	go run ./cmd/clusterstatus
//
// Repeated, for watching an election/partition unfold (used by
// scripts/fault_tolerance_demo.sh):
//
//	go run ./cmd/clusterstatus -watch=200ms
package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/locvth/mini-kv/transport/raftkvpb"
)

const defaultPeers = "localhost:9001,localhost:9002,localhost:9003"

// probeKey never needs to exist — Get on a leader with an absent key still
// answers not_leader=false, ok=false, which is all this tool reads.
const probeKey = "clusterstatus-probe"

func main() {
	peersFlag := flag.String("peers", defaultPeers, "comma-separated host:port for every peer")
	watch := flag.Duration("watch", 0, "if set, re-probe and reprint status on this interval until interrupted")
	timeout := flag.Duration("timeout", 500*time.Millisecond, "per-node dial+RPC deadline")
	flag.Parse()
	peers := strings.Split(*peersFlag, ",")

	if *watch <= 0 {
		fmt.Println(probeOnce(peers, *timeout))
		return
	}
	for {
		fmt.Printf("[%s] %s\n", time.Now().Format(time.RFC3339Nano), probeOnce(peers, *timeout))
		time.Sleep(*watch)
	}
}

// probeOnce queries every peer once and renders one line per node, e.g.:
//
//	node0=localhost:9001:LEADER node1=localhost:9002:follower(leader=localhost:9001) node2=localhost:9003:unreachable
func probeOnce(peers []string, timeout time.Duration) string {
	parts := make([]string, len(peers))
	for i, addr := range peers {
		parts[i] = fmt.Sprintf("node%d=%s:%s", i, addr, probeOne(addr, timeout))
	}
	return strings.Join(parts, " ")
}

func probeOne(addr string, timeout time.Duration) string {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Sprintf("unreachable(%v)", err)
	}
	defer conn.Close()

	resp, err := raftkvpb.NewKVClient(conn).Get(ctx, &raftkvpb.GetRequest{Key: probeKey})
	if err != nil {
		return fmt.Sprintf("unreachable(%v)", err)
	}
	if resp.NotLeader {
		if resp.LeaderHint == "" {
			return "follower(leader=unknown)"
		}
		return fmt.Sprintf("follower(leader=%s)", resp.LeaderHint)
	}
	return "LEADER"
}
