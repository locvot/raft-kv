package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const testFlushTimeout = 2 * time.Second

func mustOpen(t *testing.T, dir string, flushThreshold int) *Store {
	t.Helper()
	s, err := Open(dir, flushThreshold)
	if err != nil {
		t.Fatalf("Open(%q, %d): %v", dir, flushThreshold, err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestStoreBasic(t *testing.T) {
	s := mustOpen(t, testDir(t), 0)

	if _, ok, err := s.Get("missing"); err != nil || ok {
		t.Fatalf("Get(missing) = ok=%v err=%v, want ok=false err=nil", ok, err)
	}

	if err := s.Put("k", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if v, ok, err := s.Get("k"); err != nil || !ok || string(v) != "v1" {
		t.Fatalf("Get(k) = (%q, %v, %v), want (v1, true, nil)", v, ok, err)
	}

	if err := s.Put("k", []byte("v2")); err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	if v, _, _ := s.Get("k"); string(v) != "v2" {
		t.Fatalf("Get(k) after overwrite = %q, want v2", v)
	}

	if err := s.Delete("k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, err := s.Get("k"); err != nil || ok {
		t.Fatalf("Get(k) after Delete = ok=%v err=%v, want ok=false err=nil", ok, err)
	}

	// Delete of a key that was never set must be a silent no-op.
	if err := s.Delete("never-set"); err != nil {
		t.Fatalf("Delete(never-set): %v", err)
	}
}

// TestStoreTombstoneAcrossFlush forces every write into its own flushed
// SSTable, so the delete's tombstone lives in a newer table than the
// original value's table — Get must still resolve to "deleted", not let
// the older table's stale value shine through.
func TestStoreTombstoneAcrossFlush(t *testing.T) {
	s := mustOpen(t, testDir(t), 1)

	if err := s.Put("k", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !s.waitForFlush(testFlushTimeout) {
		t.Fatal("flush after Put did not complete in time")
	}
	if v, ok, err := s.Get("k"); err != nil || !ok || string(v) != "v1" {
		t.Fatalf("Get(k) after flush = (%q, %v, %v), want (v1, true, nil)", v, ok, err)
	}

	if err := s.Delete("k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !s.waitForFlush(testFlushTimeout) {
		t.Fatal("flush after Delete did not complete in time")
	}
	if _, ok, err := s.Get("k"); err != nil || ok {
		t.Fatalf("Get(k) after Delete+flush = ok=%v err=%v, want ok=false err=nil (stale value must not resurface)", ok, err)
	}
}

// TestStoreCrashRecovery simulates a crash by never flushing the
// memtable to an SSTable — the only durable record of the writes is the
// WAL, already fsynced by Put. Reopening the same directory must recover
// every committed write via WAL replay.
func TestStoreCrashRecovery(t *testing.T) {
	dir := testDir(t)

	s1, err := Open(dir, 1<<30) // huge threshold: nothing auto-flushes
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := s1.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := s1.Delete("k5"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(dir, 1<<30)
	if err != nil {
		t.Fatalf("reopen after simulated crash: %v", err)
	}
	defer s2.Close()

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("k%d", i)
		v, ok, err := s2.Get(key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if i == 5 {
			if ok {
				t.Fatalf("Get(%s) after recovery = ok, want deleted", key)
			}
			continue
		}
		want := fmt.Sprintf("v%d", i)
		if !ok || string(v) != want {
			t.Fatalf("Get(%s) after recovery = (%q, %v), want (%q, true)", key, v, ok, want)
		}
	}
}

// TestStoreCrashRecoveryTornWAL simulates a crash mid-append: the last WAL
// record is truncated. Recovery must keep every record that preceded it
// and drop the torn one cleanly, without losing or corrupting the rest.
func TestStoreCrashRecoveryTornWAL(t *testing.T) {
	dir := testDir(t)

	s1, err := Open(dir, 1<<30)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s1.Put("k1", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s1.Put("k2", []byte("v2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s1.Put("k3", []byte("this-record-will-be-torn-off")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected exactly one leftover WAL file, got %v (err=%v)", matches, err)
	}
	info, err := os.Stat(matches[0])
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if err := os.Truncate(matches[0], info.Size()-8); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	s2, err := Open(dir, 1<<30)
	if err != nil {
		t.Fatalf("reopen after simulated crash: %v", err)
	}
	defer s2.Close()

	for _, tc := range []struct{ key, want string }{{"k1", "v1"}, {"k2", "v2"}} {
		v, ok, err := s2.Get(tc.key)
		if err != nil || !ok || string(v) != tc.want {
			t.Fatalf("Get(%s) = (%q, %v, %v), want (%q, true, nil)", tc.key, v, ok, err, tc.want)
		}
	}
	if _, ok, err := s2.Get("k3"); err != nil || ok {
		t.Fatalf("Get(k3) = ok=%v err=%v, want ok=false (its WAL record was torn off)", ok, err)
	}
}

// TestStoreCompactionMergesAndDropsTombstones forces four tier-0 flushes
// (the default fanout) so the background compactor merges them into one
// tier-1 table, then checks both that reads still resolve correctly and
// that a tombstone with no older data left beneath it is actually dropped
// from the merged output, not just shadowed.
func TestStoreCompactionMergesAndDropsTombstones(t *testing.T) {
	s := mustOpen(t, testDir(t), 1)

	steps := []struct {
		key       string
		value     string
		tombstone bool
	}{
		{key: "doomed", value: "v0"},
		{key: "doomed", tombstone: true},
		{key: "k1", value: "v1"},
		{key: "k2", value: "v2"},
	}
	for _, st := range steps {
		var err error
		if st.tombstone {
			err = s.Delete(st.key)
		} else {
			err = s.Put(st.key, []byte(st.value))
		}
		if err != nil {
			t.Fatalf("write %+v: %v", st, err)
		}
		if !s.waitForFlush(testFlushTimeout) {
			t.Fatalf("flush for %+v did not complete in time", st)
		}
	}

	var tier1 *sstHandle
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.RLock()
		for _, h := range s.sstables {
			if h.meta.Gen == 1 {
				tier1 = h
			}
		}
		s.mu.RUnlock()
		if tier1 != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if tier1 == nil {
		t.Fatal("no tier-1 SSTable appeared; background compaction never ran")
	}

	if _, ok, err := s.Get("doomed"); err != nil || ok {
		t.Fatalf("Get(doomed) after compaction = ok=%v err=%v, want ok=false err=nil", ok, err)
	}
	if v, ok, err := s.Get("k1"); err != nil || !ok || string(v) != "v1" {
		t.Fatalf("Get(k1) after compaction = (%q, %v, %v), want (v1, true, nil)", v, ok, err)
	}
	if v, ok, err := s.Get("k2"); err != nil || !ok || string(v) != "v2" {
		t.Fatalf("Get(k2) after compaction = (%q, %v, %v), want (v2, true, nil)", v, ok, err)
	}

	entries, err := tier1.reader.readAllEntries()
	if err != nil {
		t.Fatalf("readAllEntries on tier-1 table: %v", err)
	}
	for _, e := range entries {
		if e.key == "doomed" {
			t.Fatalf("tombstone for %q was merged into the output instead of being dropped: %+v", e.key, e)
		}
	}
}

func TestStoreConcurrent(t *testing.T) {
	s := mustOpen(t, testDir(t), 512)

	var wg sync.WaitGroup
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i%10)
			switch i % 3 {
			case 0:
				s.Put(key, []byte{byte(i)})
			case 1:
				s.Get(key)
			case 2:
				s.Delete(key)
			}
		}(i)
	}
	wg.Wait()

	if err := s.Err(); err != nil {
		t.Fatalf("background flush/compaction error: %v", err)
	}
}
