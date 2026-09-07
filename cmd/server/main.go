// Command server runs one RaftKV node. Peer membership is static: every
// node in the cluster must be started with the same -peers list, differing
// only in -me. -peers/-dir have sensible local defaults (mirroring
// cmd/storagecli's tmp/storagecli-data) so a 3-node cluster on one machine
// only needs -me to differ:
//
//	go run ./cmd/server -me=0   # in one terminal
//	go run ./cmd/server -me=1   # in another
//	go run ./cmd/server -me=2   # and another
//
// SIGUSR1 toggles transport.Server.SetPartitioned on this node — M5's
// fault-injection primitive (see doc/DECISIONS.md) for simulating a
// network partition of this one process without root/iptables:
//
//	kill -USR1 <pid>   # first: partition this node from every peer
//	kill -USR1 <pid>   # second: heal it back
//
// -metrics-addr serves this node's Prometheus metrics (M6, see
// doc/DECISIONS.md "Observability") in text exposition format at
// /metrics — latency histograms and request/error counters for
// Get/Put/Delete, and a live key-count gauge.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/locvth/mini-kv/transport"
)

const defaultPeers = "localhost:9001,localhost:9002,localhost:9003"

// defaultMetricsPort is added to -me to pick this node's default
// -metrics-addr, mirroring how -dir's default ("tmp/raftkv-<me>") and
// defaultPeers stagger by node index so a same-machine 3-node cluster
// needs no flags beyond -me to avoid colliding with itself.
const defaultMetricsPort = 9101

func main() {
	peersFlag := flag.String("peers", defaultPeers, "comma-separated host:port for every peer, in Raft peer-index order")
	me := flag.Int("me", -1, "index into -peers that is this process (required)")
	dir := flag.String("dir", "", "directory for this node's storage.Store data (default: tmp/raftkv-<me>, reused across runs like cmd/storagecli)")
	metricsAddr := flag.String("metrics-addr", "", "address to serve Prometheus metrics on at /metrics (default: localhost:9101+me)")
	flag.Parse()

	if *peersFlag == "" || *me < 0 {
		fmt.Fprintln(os.Stderr, "usage: server [-peers=host:port,...] -me=N [-dir=PATH] [-metrics-addr=HOST:PORT]")
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
	if *metricsAddr == "" {
		*metricsAddr = fmt.Sprintf("localhost:%d", defaultMetricsPort+*me)
	}

	log := slog.Default().With("node", *me)

	srv, err := transport.NewServer(transport.Config{Peers: peers, Me: *me, DataDir: *dir})
	if err != nil {
		log.Error("startup failed", "error", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", peers[*me])
	if err != nil {
		log.Error("listen failed", "addr", peers[*me], "error", err)
		os.Exit(1)
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", srv.MetricsHandler())
	metricsSrv := &http.Server{Addr: *metricsAddr, Handler: metricsMux}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "addr", *metricsAddr, "error", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Info("shutting down")
		metricsSrv.Shutdown(context.Background())
		srv.Close()
	}()

	partitionSig := make(chan os.Signal, 1)
	signal.Notify(partitionSig, syscall.SIGUSR1)
	go func() {
		partitioned := false
		for range partitionSig {
			partitioned = !partitioned
			srv.SetPartitioned(partitioned)
			log.Info("partition toggled", "partitioned", partitioned)
		}
	}()

	log.Info("listening", "addr", peers[*me], "peers", peers, "dir", *dir, "metrics_addr", *metricsAddr)
	if err := srv.Serve(lis); err != nil {
		log.Error("serve failed", "error", err)
		os.Exit(1)
	}
}
