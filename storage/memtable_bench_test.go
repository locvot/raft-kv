package storage

import (
	"flag"
	"fmt"
	"testing"
)

// go test ./storage/... -bench=Memtable -run=^$ -mem-keyspace=100000 -valuesize=256 -mem-writepct=50
var (
	memBenchKeyspace = flag.Int("mem-keyspace", 1000, "number of distinct keys used in the memtable benchmarks")
	memBenchWritePct = flag.Int("mem-writepct", 10, "percentage (0-100) of operations that are Put rather than Get in the memtable benchmarks")
)

func BenchmarkMemtableSortedSlice(b *testing.B) {
	benchmarkMemtable(b, NewMemtable())
}

func BenchmarkMemtableSkipList(b *testing.B) {
	benchmarkMemtable(b, NewSkipListMemtable())
}

// benchmarkMemtable mirrors engine/engine_bench_test.go's benchmarkEngine:
// b.RunParallel across GOMAXPROCS goroutines hammering a shared keyspace
// (small by default) so concurrent Put/Get calls actually collide on the
// same mutex/nodes, instead of comparing the two structures on disjoint
// keys where they'd look equally fast. valuesize reuses the -valuesize
// flag already registered in store_bench_test.go (same test binary).
func benchmarkMemtable(b *testing.B, m memtableImpl) {
	keyspace := *memBenchKeyspace
	writePct := *memBenchWritePct
	value := make([]byte, *benchValueSize)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%keyspace)
			if i%100 < writePct {
				m.Put(key, value)
			} else {
				m.Get(key)
			}
			i++
		}
	})
}
