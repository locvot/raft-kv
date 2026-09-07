// Command loadgen is RaftKV's M7 load generator: -clients concurrent
// simulated clients, each its own transport.Client, hammering a running
// cluster with a Put/Get mix through leader redirect exactly like
// cmd/client would, for -duration. It reports real p50/p99 latency and
// throughput at the end — the raw numbers doc/DECISIONS.md's M7 section and
// README quote come directly from this tool's output, not estimates.
//
// It talks real gRPC to a real cluster (started separately, e.g. via
// cmd/server) — it is not a benchmark against an in-process fake, so
// "1-node vs 3-node" and "sharded vs single-mutex" comparisons in the plan
// mean: run this twice against differently-sized real clusters, and
// separately run `go test ./engine/... -bench=.` for the concurrency-map
// comparison (that one never touches the network, so it doesn't belong in
// this tool).
//
//	go run ./cmd/loadgen -peers=localhost:9001,localhost:9002,localhost:9003 -clients=50 -duration=15s
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/locvth/mini-kv/transport"
)

const defaultPeers = "localhost:9001,localhost:9002,localhost:9003"

type opResult struct {
	latency time.Duration
	isWrite bool
	err     error
}

func main() {
	peersFlag := flag.String("peers", defaultPeers, "comma-separated host:port for every peer")
	numClients := flag.Int("clients", 50, "number of concurrent simulated clients, each its own transport.Client/gRPC connection")
	duration := flag.Duration("duration", 10*time.Second, "measured run duration")
	warmup := flag.Duration("warmup", 3*time.Second, "unmeasured Put-only warmup before the timed run, so measured Gets mostly hit real keys instead of missing")
	keyspace := flag.Int("keyspace", 10000, "number of distinct keys the run cycles through")
	valueSize := flag.Int("valuesize", 128, "size in bytes of every Put's value")
	writePct := flag.Int("writepct", 50, "percentage (0-100) of measured operations that are Put rather than Get")
	opTimeout := flag.Duration("op-timeout", 2*time.Second, "per-operation deadline")
	flag.Parse()

	peers := strings.Split(*peersFlag, ",")

	fmt.Printf("loadgen: peers=%s clients=%d keyspace=%d valuesize=%d writepct=%d warmup=%s duration=%s\n",
		*peersFlag, *numClients, *keyspace, *valueSize, *writePct, *warmup, *duration)

	if *warmup > 0 {
		fmt.Println("warmup (Put-only, unmeasured)...")
		runPhase(*warmup, *numClients, peers, *keyspace, *valueSize, 100, *opTimeout, false)
	}

	fmt.Println("measuring...")
	results := runPhase(*duration, *numClients, peers, *keyspace, *valueSize, *writePct, *opTimeout, true)

	report(results, *duration)
}

// runPhase runs numClients worker goroutines, each its own transport.Client,
// for dur. Each worker accumulates its own []opResult with no shared
// mutable state across goroutines (results[i] is written only by worker i),
// so nothing here can skew the very latencies it's trying to measure with
// lock contention.
func runPhase(dur time.Duration, numClients int, peers []string, keyspace, valueSize, writePct int, opTimeout time.Duration, record bool) []opResult {
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()

	perWorker := make([][]opResult, numClients)
	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			perWorker[i] = worker(ctx, peers, keyspace, valueSize, writePct, opTimeout, record, int64(i))
		}(i)
	}
	wg.Wait()

	if !record {
		return nil
	}
	var all []opResult
	for _, r := range perWorker {
		all = append(all, r...)
	}
	return all
}

func worker(ctx context.Context, peers []string, keyspace, valueSize, writePct int, opTimeout time.Duration, record bool, seed int64) []opResult {
	client := transport.NewClient(peers)
	defer client.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano() ^ seed))
	value := make([]byte, valueSize)
	rng.Read(value)

	var results []opResult
	for ctx.Err() == nil {
		key := fmt.Sprintf("key-%d", rng.Intn(keyspace))
		isWrite := rng.Intn(100) < writePct

		opCtx, cancel := context.WithTimeout(context.Background(), opTimeout)
		start := time.Now()
		var err error
		if isWrite {
			err = client.Put(opCtx, key, value)
		} else {
			_, _, err = client.Get(opCtx, key)
		}
		elapsed := time.Since(start)
		cancel()

		if record {
			results = append(results, opResult{latency: elapsed, isWrite: isWrite, err: err})
		}
	}
	return results
}

func report(results []opResult, dur time.Duration) {
	if len(results) == 0 {
		fmt.Println("no operations completed")
		return
	}

	var errs, writes int
	latencies := make([]time.Duration, 0, len(results))
	for _, r := range results {
		latencies = append(latencies, r.latency)
		if r.err != nil {
			errs++
		}
		if r.isWrite {
			writes++
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	total := len(results)
	throughput := float64(total) / dur.Seconds()

	fmt.Printf("\n--- results ---\n")
	fmt.Printf("total ops:      %d (%d put, %d get, %d errors)\n", total, writes, total-writes, errs)
	fmt.Printf("throughput:     %.1f ops/s\n", throughput)
	fmt.Printf("latency p50:    %s\n", percentile(latencies, 0.50))
	fmt.Printf("latency p90:    %s\n", percentile(latencies, 0.90))
	fmt.Printf("latency p99:    %s\n", percentile(latencies, 0.99))
	fmt.Printf("latency max:    %s\n", latencies[len(latencies)-1])
	fmt.Printf("latency mean:   %s\n", mean(latencies))
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func mean(durations []time.Duration) time.Duration {
	var sum time.Duration
	for _, d := range durations {
		sum += d
	}
	return sum / time.Duration(len(durations))
}
