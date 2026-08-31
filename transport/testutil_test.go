package transport

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// testDir mirrors storage's testDir: real files under the project's
// (gitignored) tmp/, wiped at the start of the test rather than the end so
// a failed run's WAL/SSTable/MANIFEST files stay around to inspect.
func testDir(t *testing.T, name string) string {
	t.Helper()
	dir := filepath.Join("..", "tmp", "transport", filepath.FromSlash(t.Name()), name)
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("RemoveAll(%s): %v", dir, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", dir, err)
	}
	return dir
}

// testCluster is a real, multi-process-shaped cluster running as N
// in-process gRPC servers over real loopback TCP sockets (not simnet —
// simnet is raft's own test infrastructure; this exercises the actual
// transport package an operator would run as separate processes).
type testCluster struct {
	t       *testing.T
	servers []*Server
	lis     []net.Listener
	peers   []string
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	c := &testCluster{t: t, servers: make([]*Server, n), lis: make([]net.Listener, n), peers: make([]string, n)}

	for i := 0; i < n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		c.lis[i] = lis
		c.peers[i] = lis.Addr().String()
	}
	for i := 0; i < n; i++ {
		srv, err := NewServer(Config{Peers: c.peers, Me: i, DataDir: testDir(t, "node"+strconv.Itoa(i))})
		if err != nil {
			t.Fatalf("NewServer(%d): %v", i, err)
		}
		c.servers[i] = srv
		go srv.Serve(c.lis[i])
	}
	t.Cleanup(func() {
		for _, srv := range c.servers {
			if srv != nil {
				srv.Close()
			}
		}
	})
	return c
}

// killLeader closes whichever server is currently leader and returns its
// index, so a test can assert the remaining peers still make progress.
// Fails the test if no leader is found within a few election timeouts.
func (c *testCluster) killLeader() int {
	c.t.Helper()
	for i, srv := range c.servers {
		if srv == nil {
			continue
		}
		if _, isLeader := srv.rf.GetState(); isLeader {
			srv.Close()
			c.servers[i] = nil
			return i
		}
	}
	c.t.Fatalf("killLeader: no leader found")
	return -1
}

func (c *testCluster) client() *Client {
	return NewClient(c.peers)
}

func ctxTimeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
