package storage

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// go test ./storage/... -bench=. -run=^$ -valuesize=256 -cpu=1,2,4,8
var benchValueSize = flag.Int("valuesize", 32, "size in bytes of the value written on every WAL append / Store.Put in the storage benchmarks")

// benchDir returns a directory under the project's tmp/ for b to write
// real files into. Deliberately NOT b.TempDir(): that resolves under
// os.TempDir(), which on this machine (and many dev/CI boxes) is a tmpfs
// mount — an in-RAM filesystem where fsync is nearly free. Since the whole
// point of these benchmarks is measuring fsync's real cost, running them
// against tmpfs would silently produce meaningless (much too fast)
// numbers. tmp/ lives on the project's actual disk (see `df -T tmp/`).
func benchDir(b *testing.B) string {
	b.Helper()
	dir := filepath.Join("..", "tmp", "storage-bench", filepath.FromSlash(b.Name()))
	if err := os.RemoveAll(dir); err != nil {
		b.Fatalf("RemoveAll(%s): %v", dir, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatalf("MkdirAll(%s): %v", dir, err)
	}
	return dir
}

// BenchmarkWALAppendParallel isolates the cost of WAL.Append itself: every
// call fully serializes on w.mu and fsyncs *inside* that lock (wal.go:43-72)
// — there is no group commit (batching several callers' records into one
// fsync). If that serialization is the bottleneck, ns/op should stay flat
// (or get worse) as GOMAXPROCS increases, instead of dropping the way it
// would for CPU-bound work spread across more cores.
//
// testing.B doesn't vary parallelism within a single run, so compare
// GOMAXPROCS settings across separate runs:
//
//	go test ./storage/... -bench=BenchmarkWALAppendParallel -run=^$ -cpu=1,2,4,8
func BenchmarkWALAppendParallel(b *testing.B) {
	w, err := OpenWAL(filepath.Join(benchDir(b), "wal-0.log"))
	if err != nil {
		b.Fatalf("OpenWAL: %v", err)
	}
	defer w.Close()

	value := make([]byte, *benchValueSize)
	var n int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := fmt.Sprintf("key-%d", atomic.AddInt64(&n, 1))
			if err := w.Append(recPut, key, value); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkStorePutParallel is the end-to-end equivalent through the public
// API: concurrent Store.Put calls, each of which does one WAL.Append (see
// above) before touching the memtable. flushThreshold is set high enough
// that no flush happens during the benchmark, so this measures the write
// path's own cost, not flush/compaction noise.
//
// Because flush never triggers here, the memtable's sorted slice (see
// memtable.go) grows for the entire run and every insert is O(current
// size) — run this with a bounded iteration count (`-benchtime=Nx`), NOT
// the default time-based auto-scaling, or the auto-scaler will keep
// doubling b.N into an O(n^2) memtable and the run will never converge:
//
//	go test ./storage/... -bench=BenchmarkStorePutParallel -run=^$ -benchtime=20000x -cpu=1,2,4,8
func BenchmarkStorePutParallel(b *testing.B) {
	s, err := Open(benchDir(b), 1<<30) // huge threshold: no flush during the run
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer s.Close()

	value := make([]byte, *benchValueSize)
	var n int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := fmt.Sprintf("key-%d", atomic.AddInt64(&n, 1))
			if err := s.Put(key, value); err != nil {
				b.Fatal(err)
			}
		}
	})
}
