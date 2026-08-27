package engine

import (
	"flag"
	"fmt"
	"testing"
)

//	go test ./engine/... -bench=. -run=^$ -keyspace=100000 -valuesize=256 -writepct=50
var (
	benchKeyspace  = flag.Int("keyspace", 1000, "number of distinct keys used in the engine benchmarks")
	benchValueSize = flag.Int("valuesize", 1, "size in bytes of the value written on every Put in the engine benchmarks")
	benchWritePct  = flag.Int("writepct", 10, "percentage (0-100) of operations that are Put rather than Get in the engine benchmarks")
)

func BenchmarkMutexMap(b *testing.B) {
	benchmarkEngine(b, NewMutexMap())
}

func BenchmarkShardedMap(b *testing.B) {
	benchmarkEngine(b, NewShardedMap(0))
}

func BenchmarkSyncMap(b *testing.B) {
	benchmarkEngine(b, NewSyncMap())
}

// benchmarkEngine spreads iterations across GOMAXPROCS goroutines
// (b.RunParallel) hammering a keyspace of size *benchKeyspace with a
// Put/Get mix controlled by *benchWritePct — the contention pattern that
// separates a single-mutex map from a sharded one. A keyspace this small
// (by default) guarantees goroutines on different CPUs collide on the same
// keys/shards, which is what makes the comparison meaningful instead of
// both implementations looking equally fast on disjoint keys.
func benchmarkEngine(b *testing.B, e Engine) {
	keyspace := *benchKeyspace
	writePct := *benchWritePct
	value := make([]byte, *benchValueSize)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%keyspace)
			if i%100 < writePct {
				e.Put(key, value)
			} else {
				e.Get(key)
			}
			i++
		}
	})
}
