package storage

import (
	"fmt"
	"sync"
	"testing"
)

func TestMemtableBasic(t *testing.T) {
	m := NewMemtable()

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

func TestMemtableIterateAscending(t *testing.T) {
	m := NewMemtable()
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

func TestMemtableSizeTracksPutAndOverwrite(t *testing.T) {
	m := NewMemtable()
	m.Put("k", []byte("1234")) // 1 + 4 = 5
	if got := m.Size(); got != 5 {
		t.Fatalf("Size after one Put = %d, want 5", got)
	}
	m.Put("k", []byte("12")) // overwrite: 1 + 2 = 3
	if got := m.Size(); got != 3 {
		t.Fatalf("Size after overwrite = %d, want 3", got)
	}
}

func TestMemtableConcurrent(t *testing.T) {
	m := NewMemtable()
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
