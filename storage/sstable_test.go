package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func writeTestSSTable(t *testing.T, path string, blockSize int, entries []sstEntry) {
	t.Helper()
	w, err := NewSSTableWriterSize(path, blockSize)
	if err != nil {
		t.Fatalf("NewSSTableWriterSize: %v", err)
	}
	for _, e := range entries {
		if err := w.Add(e.key, e.value, e.tombstone); err != nil {
			t.Fatalf("Add(%q): %v", e.key, err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
}

func TestSSTableRoundTripMultiBlock(t *testing.T) {
	path := filepath.Join(testDir(t), "0-0.sst")

	var entries []sstEntry
	for i := 0; i < 500; i++ {
		entries = append(entries, sstEntry{
			key:   fmt.Sprintf("key-%04d", i),
			value: []byte(fmt.Sprintf("value-%d", i)),
		})
	}
	// Small block size forces many blocks, exercising the index and
	// block-boundary logic instead of everything landing in one block.
	writeTestSSTable(t, path, 64, entries)

	r, err := OpenSSTableReader(path)
	if err != nil {
		t.Fatalf("OpenSSTableReader: %v", err)
	}
	defer r.Close()

	if len(r.index) < 2 {
		t.Fatalf("expected multiple data blocks with blockSize=64 and %d entries, got index len %d", len(entries), len(r.index))
	}

	for _, e := range entries {
		v, tomb, found, err := r.Get(e.key)
		if err != nil {
			t.Fatalf("Get(%q): %v", e.key, err)
		}
		if !found || tomb || string(v) != string(e.value) {
			t.Fatalf("Get(%q) = (%q, tomb=%v, found=%v), want (%q, false, true)", e.key, v, tomb, found, e.value)
		}
	}

	if _, _, found, err := r.Get("not-a-key"); err != nil || found {
		t.Fatalf("Get(not-a-key) = found=%v err=%v, want found=false err=nil", found, err)
	}
}

func TestSSTableTombstone(t *testing.T) {
	path := filepath.Join(testDir(t), "0-0.sst")
	writeTestSSTable(t, path, defaultBlockSize, []sstEntry{
		{key: "a", value: []byte("1")},
		{key: "b", tombstone: true},
	})

	r, err := OpenSSTableReader(path)
	if err != nil {
		t.Fatalf("OpenSSTableReader: %v", err)
	}
	defer r.Close()

	_, tomb, found, err := r.Get("b")
	if err != nil || !found || !tomb {
		t.Fatalf("Get(b) = tomb=%v found=%v err=%v, want tomb=true found=true err=nil", tomb, found, err)
	}
}

func TestSSTableAddRequiresAscendingKeys(t *testing.T) {
	path := filepath.Join(testDir(t), "0-0.sst")
	w, err := NewSSTableWriter(path)
	if err != nil {
		t.Fatalf("NewSSTableWriter: %v", err)
	}
	if err := w.Add("b", []byte("1"), false); err != nil {
		t.Fatalf("Add(b): %v", err)
	}
	if err := w.Add("a", []byte("2"), false); err == nil {
		t.Fatalf("Add(a) after Add(b) succeeded, want error (keys must be strictly ascending)")
	}
}

// TestSSTableCorruptBlockDetected flips a byte inside a data block after
// writing and confirms Get reports ErrCorruptBlock rather than silently
// returning the wrong value or claiming the key doesn't exist.
func TestSSTableCorruptBlockDetected(t *testing.T) {
	path := filepath.Join(testDir(t), "0-0.sst")
	writeTestSSTable(t, path, defaultBlockSize, []sstEntry{
		{key: "a", value: []byte("1")},
		{key: "b", value: []byte("2")},
	})

	r, err := OpenSSTableReader(path)
	if err != nil {
		t.Fatalf("OpenSSTableReader: %v", err)
	}
	defer r.Close()

	// The single data block starts at byte 0 (offset of the first, and
	// only, index entry). Flip a byte inside its payload, past the 4-byte
	// checksum prefix.
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	corruptOffset := r.index[0].offset + 6
	if _, err := f.WriteAt([]byte{0xFF}, corruptOffset); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	f.Close()

	_, _, _, err = r.Get("a")
	if !errors.Is(err, ErrCorruptBlock) {
		t.Fatalf("Get(a) on corrupted block returned err=%v, want ErrCorruptBlock", err)
	}
}
