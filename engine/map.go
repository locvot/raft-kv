package engine

import "sync"

// MutexMap is the simplest possible concurrency-safe key-value store: one
// map guarded by one mutex. It's the M1 baseline that ShardedMap is
// benchmarked against.
type MutexMap struct {
	mu   sync.Mutex
	data map[string][]byte
}

func NewMutexMap() *MutexMap {
	return &MutexMap{data: make(map[string][]byte)}
}

func (m *MutexMap) Get(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

func (m *MutexMap) Put(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = append([]byte(nil), value...)
}

func (m *MutexMap) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}
