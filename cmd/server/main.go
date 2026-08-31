// Command server runs one RaftKV node. Peer membership is static: every
// node in the cluster must be started with the same -peers list, differing
// only in -me. -peers/-dir have sensible local defaults (mirroring
// cmd/storagecli's tmp/storagecli-data) so a 3-node cluster on one machine
// only needs -me to differ:
//
//	go run ./cmd/server -me=0   # in one terminal
//	go run ./cmd/server -me=1   # in another
//	go run ./cmd/server -me=2   # and another
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/locvth/mini-kv/transport"
)

const defaultPeers = "localhost:9001,localhost:9002,localhost:9003"

func main() {
	peersFlag := flag.String("peers", defaultPeers, "comma-separated host:port for every peer, in Raft peer-index order")
	me := flag.Int("me", -1, "index into -peers that is this process (required)")
	dir := flag.String("dir", "", "directory for this node's storage.Store data (default: tmp/raftkv-<me>, reused across runs like cmd/storagecli)")
	flag.Parse()

	if *peersFlag == "" || *me < 0 {
		fmt.Fprintln(os.Stderr, "usage: server [-peers=host:port,...] -me=N [-dir=PATH]")
		os.Exit(2)
	}
	peers := strings.Split(*peersFlag, ",")
	if *me >= len(peers) {
		fmt.Fprintf(os.Stderr, "server: -me=%d out of range for %d peers\n", *me, len(peers))
		os.Exit(2)
	}
	if *dir == "" {
		*dir = fmt.Sprintf("tmp/raftkv-%d", *me)
	}

	srv, err := transport.NewServer(transport.Config{Peers: peers, Me: *me, DataDir: *dir})
	if err != nil {
		log.Fatalf("server: %v", err)
	}

	lis, err := net.Listen("tcp", peers[*me])
	if err != nil {
		log.Fatalf("server: listen on %s: %v", peers[*me], err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Printf("server: shutting down")
		srv.Close()
	}()

	log.Printf("server: node %d listening on %s, peers=%v, dir=%s", *me, peers[*me], peers, *dir)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("server: %v", err)
	}
}
