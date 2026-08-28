package storage

import (
	"sort"
	"sync"
)

// Memtable is the in-memory, write-buffering front end of a Store: every
// Put/Delete lands here first (after being made durable via the WAL), and
// gets flushed out to an SSTable once it grows past a size threshold.
//
// Backed by a sorted slice with binary search, not a skip list. A skip
// list's payoff is lock-free concurrent insertion; here, writes are already
// serialized behind the WAL's fsync (see Store.Put/Delete), so a single
// mutex over a sorted slice is simpler to get right and just as fast in
// practice for this project's scale — matching M1's "measure before
// optimizing" precedent (see engine/ benchmarks). Revisit with a benchmark
// if this ever shows up as a bottleneck.
type Memtable struct {
	mu      sync.RWMutex
	entries []memEntry // kept sorted by Key
	size    int        // approx bytes: sum of len(key)+len(value) over entries
}

type memEntry struct {
	key       string
	value     []byte
	tombstone bool
}

func NewMemtable() *Memtable {
	return &Memtable{}
}

// search returns the index of key in m.entries, or the index it should be
// inserted at, and whether it was found. Caller must hold m.mu.
func (m *Memtable) search(key string) (int, bool) {
	i := sort.Search(len(m.entries), func(i int) bool { return m.entries[i].key >= key })
	if i < len(m.entries) && m.entries[i].key == key {
		return i, true
	}
	return i, false
}

func (m *Memtable) Put(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.upsert(key, value, false)
}

func (m *Memtable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.upsert(key, nil, true)
}

func (m *Memtable) upsert(key string, value []byte, tombstone bool) {
	i, found := m.search(key)
	entry := memEntry{key: key, value: value, tombstone: tombstone}
	if found {
		m.size += len(value) - len(m.entries[i].value)
		m.entries[i] = entry
		return
	}
	m.entries = append(m.entries, memEntry{})
	copy(m.entries[i+1:], m.entries[i:])
	m.entries[i] = entry
	m.size += len(key) + len(value)
}

// Get reports the most recent write to key in this memtable. tombstone is
// true if the most recent write was a Delete; found is false if key has
// never been written to this memtable at all.
func (m *Memtable) Get(key string) (value []byte, tombstone, found bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	i, found := m.search(key)
	if !found {
		return nil, false, false
	}
	e := m.entries[i]
	return e.value, e.tombstone, true
}

func (m *Memtable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// Iterate calls fn for every entry in ascending key order, tombstones
// included. Used when flushing the memtable out to an SSTable.
func (m *Memtable) Iterate(fn func(key string, value []byte, tombstone bool)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, e := range m.entries {
		fn(e.key, e.value, e.tombstone)
	}
}
