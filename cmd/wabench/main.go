// Command wabench measures storage.Store's real write amplification — the
// ratio of physical bytes actually written to disk (WAL fsyncs plus every
// SSTable ever produced by a flush or a compaction, including ones later
// deleted) to the logical bytes the caller asked to write — for a given
// value size. It exists to answer the M7 question from doc/raftkv.plan.md
// §M2/M7 with a real measurement instead of a guess: is write amplification
// "rõ rệt cao" (clearly high) once values get into the multi-KB range, the
// stated trigger condition for the value-separation (WiscKey/Badger-style)
// stretch goal.
//
// Physical bytes are read from /proc/self/io's write_bytes counter
// (Linux-only), not from summing final on-disk file sizes: final size only
// counts whatever SSTable survived compaction, silently excluding every
// intermediate SSTable that compaction read and rewrote before deleting —
// exactly the rewrite cost write amplification is supposed to capture.
// write_bytes is reliable here specifically because every write this
// process makes to the store directory is followed by an f.Sync() before
// the call that produced it returns (storage.WAL.Append, SSTableWriter.
// Finish) — synchronous writeback is attributed to the calling task by the
// kernel, unlike lazily-flushed dirty pages, which normally get charged to
// the kernel's own flusher thread instead.
//
//	go run ./cmd/wabench -valuesize=64
//	go run ./cmd/wabench -valuesize=4096
//	go run ./cmd/wabench -valuesize=16384
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/locvth/mini-kv/storage"
)

func main() {
	dir := flag.String("dir", "tmp/wabench-data", "directory for the store under test (recreated fresh on every run)")
	valueSize := flag.Int("valuesize", 4096, "size in bytes of every Put's value")
	numOps := flag.Int("numops", 20000, "number of Put calls to issue")
	keyspace := flag.Int("keyspace", 1000, "number of distinct keys the Puts cycle through (small relative to -numops so keys get overwritten repeatedly, the workload shape that makes compaction do real work)")
	flushThreshold := flag.Int("flushthreshold", 256*1024, "memtable flush threshold in bytes; kept small so a run of this size actually triggers several flushes/compactions instead of sitting entirely in one memtable")
	flag.Parse()

	if err := os.RemoveAll(*dir); err != nil {
		fmt.Fprintf(os.Stderr, "wabench: RemoveAll(%s): %v\n", *dir, err)
		os.Exit(1)
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "wabench: MkdirAll(%s): %v\n", *dir, err)
		os.Exit(1)
	}

	before, err := readWriteBytes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "wabench: reading /proc/self/io: %v (this tool only works on Linux)\n", err)
		os.Exit(1)
	}

	s, err := storage.Open(*dir, *flushThreshold)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wabench: storage.Open(%s): %v\n", *dir, err)
		os.Exit(1)
	}

	rng := rand.New(rand.NewSource(1)) // fixed seed: identical key sequence across value sizes
	value := make([]byte, *valueSize)
	rng.Read(value)

	var logicalBytes int64
	start := time.Now()
	for i := 0; i < *numOps; i++ {
		key := fmt.Sprintf("key-%d", rng.Intn(*keyspace))
		if err := s.Put(key, value); err != nil {
			fmt.Fprintf(os.Stderr, "wabench: Put: %v\n", err)
			os.Exit(1)
		}
		logicalBytes += int64(len(key)) + int64(len(value))
	}
	writeElapsed := time.Since(start)

	waitForCompactionToSettle(*dir)

	if err := s.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "wabench: Close: %v\n", err)
		os.Exit(1)
	}

	after, err := readWriteBytes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "wabench: reading /proc/self/io: %v\n", err)
		os.Exit(1)
	}

	physicalBytes := after - before
	amp := float64(physicalBytes) / float64(logicalBytes)

	fmt.Printf("valuesize=%d numops=%d keyspace=%d flushthreshold=%d\n", *valueSize, *numOps, *keyspace, *flushThreshold)
	fmt.Printf("write phase: %s (%.0f ops/s)\n", writeElapsed, float64(*numOps)/writeElapsed.Seconds())
	fmt.Printf("logical bytes written by caller:  %12d\n", logicalBytes)
	fmt.Printf("physical bytes written to disk:   %12d  (/proc/self/io write_bytes delta)\n", physicalBytes)
	fmt.Printf("write amplification:              %12.2fx\n", amp)
}

// waitForCompactionToSettle polls dir's total file size until it stops
// changing for stableFor, so the physical-bytes measurement includes
// compactions still in flight when the write loop returned rather than
// racing them. Polling on-disk state is the only option available from
// outside package storage — Store exposes no "drain" API, and adding one
// solely for this one-off measurement tool wasn't worth the API surface.
func waitForCompactionToSettle(dir string) {
	const (
		pollInterval = 100 * time.Millisecond
		stableFor    = 1 * time.Second
		maxWait      = 30 * time.Second
	)
	deadline := time.Now().Add(maxWait)
	lastSize := dirSize(dir)
	stableSince := time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(pollInterval)
		size := dirSize(dir)
		if size != lastSize {
			lastSize = size
			stableSince = time.Now()
			continue
		}
		if time.Since(stableSince) >= stableFor {
			return
		}
	}
}

func dirSize(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return -1
	}
	var total int64
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}

// readWriteBytes reads this process's cumulative write_bytes counter from
// /proc/self/io — bytes actually charged to storage I/O, as opposed to
// wchar (bytes passed to write(2), including ones that only ever touch page
// cache). See the package doc comment for why this attributes correctly
// here.
func readWriteBytes() (int64, error) {
	f, err := os.Open(filepath.Join("/proc", "self", "io"))
	if err != nil {
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "write_bytes:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return 0, fmt.Errorf("unexpected write_bytes line %q", line)
		}
		return strconv.ParseInt(fields[1], 10, 64)
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("write_bytes not found in /proc/self/io")
}
