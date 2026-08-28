package storage

import (
	"fmt"
	"sync"
	"testing"
)

// memtableImpl is the common surface exercised by the shared test/bench
// helpers below, implemented by both Memtable (sorted slice) and
// SkipListMemtable — so every scenario runs against both without
// duplicating the test bodies.
type memtableImpl interface {
	Put(key string, value []byte)
	Delete(key string)
	Get(key string) (value []byte, tombstone, found bool)
	Size() int
	Iterate(fn func(key string, value []byte, tombstone bool))
}

func testMemtableBasic(t *testing.T, m memtableImpl) {
	t.Helper()

	if _, _, found := m.Get("missing"); found {
		t.Fatalf("Get(missing) = found, want !found")
	}

	m.Put("b", []byte("2"))
	m.Put("a", []byte("1"))
	m.Put("c", []byte("3"))

	for _, tc := range []struct{ key, want string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		v, tomb, found := m.Get(tc.key)
		if !found || tomb || string(v) != tc.want {
			t.Fatalf("Get(%q) = (%q, tomb=%v, found=%v), want (%q, false, true)", tc.key, v, tomb, found, tc.want)
		}
	}

	m.Put("a", []byte("1-updated"))
	if v, _, _ := m.Get("a"); string(v) != "1-updated" {
		t.Fatalf("Get(a) after overwrite = %q, want 1-updated", v)
	}

	m.Delete("b")
	v, tomb, found := m.Get("b")
	if !found || !tomb || v != nil {
		t.Fatalf("Get(b) after Delete = (%q, tomb=%v, found=%v), want (nil, true, true)", v, tomb, found)
	}
}

func testMemtableIterateAscending(t *testing.T, m memtableImpl) {
	t.Helper()

	m.Put("c", []byte("3"))
	m.Put("a", []byte("1"))
	m.Put("b", []byte("2"))
	m.Delete("d") // tombstone for a never-Put key must still show up

	var gotKeys []string
	var gotTomb []bool
	m.Iterate(func(key string, value []byte, tombstone bool) {
		gotKeys = append(gotKeys, key)
		gotTomb = append(gotTomb, tombstone)
	})

	wantKeys := []string{"a", "b", "c", "d"}
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("Iterate visited %v, want %v", gotKeys, wantKeys)
	}
	for i, k := range wantKeys {
		if gotKeys[i] != k {
			t.Fatalf("Iterate order = %v, want %v", gotKeys, wantKeys)
		}
	}
	if !gotTomb[3] {
		t.Fatalf("Iterate did not mark key %q as a tombstone", "d")
	}
}

func testMemtableSizeTracksPutAndOverwrite(t *testing.T, m memtableImpl) {
	t.Helper()

	m.Put("k", []byte("1234")) // 1 + 4 = 5
	if got := m.Size(); got != 5 {
		t.Fatalf("Size after one Put = %d, want 5", got)
	}
	m.Put("k", []byte("12")) // overwrite: 1 + 2 = 3
	if got := m.Size(); got != 3 {
		t.Fatalf("Size after overwrite = %d, want 3", got)
	}
}

func testMemtableConcurrent(t *testing.T, m memtableImpl) {
	t.Helper()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i%10)
			switch i % 3 {
			case 0:
				m.Put(key, []byte{byte(i)})
			case 1:
				m.Get(key)
			case 2:
				m.Delete(key)
			}
		}(i)
	}
	wg.Wait()
}

func TestMemtableBasic(t *testing.T) { testMemtableBasic(t, NewMemtable()) }
func TestMemtableIterateAscending(t *testing.T) {
	testMemtableIterateAscending(t, NewMemtable())
}
func TestMemtableSizeTracksPutAndOverwrite(t *testing.T) {
	testMemtableSizeTracksPutAndOverwrite(t, NewMemtable())
}
func TestMemtableConcurrent(t *testing.T) { testMemtableConcurrent(t, NewMemtable()) }

func TestSkipListMemtableBasic(t *testing.T) { testMemtableBasic(t, NewSkipListMemtable()) }
func TestSkipListMemtableIterateAscending(t *testing.T) {
	testMemtableIterateAscending(t, NewSkipListMemtable())
}
func TestSkipListMemtableSizeTracksPutAndOverwrite(t *testing.T) {
	testMemtableSizeTracksPutAndOverwrite(t, NewSkipListMemtable())
}
func TestSkipListMemtableConcurrent(t *testing.T) { testMemtableConcurrent(t, NewSkipListMemtable()) }
