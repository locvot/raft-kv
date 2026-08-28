package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func TestWALAppendReplay(t *testing.T) {
	path := filepath.Join(testDir(t), "wal-0.log")

	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	if err := w.Append(recPut, "k1", []byte("v1")); err != nil {
		t.Fatalf("Append put: %v", err)
	}
	if err := w.Append(recPut, "k2", []byte("v2")); err != nil {
		t.Fatalf("Append put: %v", err)
	}
	if err := w.Append(recDel, "k1", nil); err != nil {
		t.Fatalf("Append del: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries, err := ReplayWAL(path)
	if err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}
	want := []WALEntry{
		{Type: recPut, Key: "k1", Value: []byte("v1")},
		{Type: recPut, Key: "k2", Value: []byte("v2")},
		{Type: recDel, Key: "k1"},
	}
	if len(entries) != len(want) {
		t.Fatalf("ReplayWAL returned %d entries, want %d: %+v", len(entries), len(want), entries)
	}
	for i, e := range entries {
		if e.Type != want[i].Type || e.Key != want[i].Key || string(e.Value) != string(want[i].Value) {
			t.Fatalf("entry %d = %+v, want %+v", i, e, want[i])
		}
	}
}

func TestReplayWALMissingFile(t *testing.T) {
	entries, err := ReplayWAL(filepath.Join(testDir(t), "does-not-exist.log"))
	if err != nil {
		t.Fatalf("ReplayWAL on missing file: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("ReplayWAL on missing file = %d entries, want 0", len(entries))
	}
}

// TestReplayWALTornRecord simulates a crash mid-append: a valid record
// followed by a truncated one. Replay must return the valid record and
// silently stop, not error out or lose the valid prefix.
func TestReplayWALTornRecord(t *testing.T) {
	path := filepath.Join(testDir(t), "wal-0.log")

	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	if err := w.Append(recPut, "good", []byte("value")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Append(recPut, "torn", []byte("this-will-be-cut-off")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	// Truncate off the tail so the second record is torn.
	if err := os.Truncate(path, info.Size()-5); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	entries, err := ReplayWAL(path)
	if err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}
	if len(entries) != 1 || entries[0].Key != "good" || string(entries[0].Value) != "value" {
		t.Fatalf("ReplayWAL after torn tail = %+v, want exactly the first record", entries)
	}
}

// TestWALAppendConcurrentGroupCommit hammers Append from many goroutines at
// once — the scenario group commit is for — and checks every single
// record still shows up intact on replay, exactly once, with a correct
// checksum. Group commit batches multiple callers' records into one
// write+fsync round (see commitLoop in wal.go); this is what would catch a
// batching bug that dropped, duplicated, or interleaved bytes between
// concurrent callers' records.
func TestWALAppendConcurrentGroupCommit(t *testing.T) {
	path := filepath.Join(testDir(t), "wal-0.log")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	const n = 500
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			value := []byte(fmt.Sprintf("value-%d", i))
			if err := w.Append(recPut, key, value); err != nil {
				t.Errorf("Append(%s): %v", key, err)
			}
		}(i)
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries, err := ReplayWAL(path)
	if err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}
	if len(entries) != n {
		t.Fatalf("ReplayWAL returned %d entries, want %d (group commit must not drop or duplicate records)", len(entries), n)
	}

	got := make(map[string]string, n)
	for _, e := range entries {
		if e.Type != recPut {
			t.Fatalf("entry %+v: want recPut", e)
		}
		got[e.Key] = string(e.Value)
	}
	var keys []string
	for i := 0; i < n; i++ {
		keys = append(keys, fmt.Sprintf("key-%d", i))
	}
	sort.Strings(keys)
	for _, key := range keys {
		want := "value-" + key[len("key-"):]
		if got[key] != want {
			t.Fatalf("entry for %s = %q, want %q (or entry missing entirely — checksum/framing corrupted by concurrent batching)", key, got[key], want)
		}
	}
}
