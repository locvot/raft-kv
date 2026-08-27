package engine

import "sync"

type RWMutexMap struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewRWMutexMap() *RWMutexMap {
	return &RWMutexMap{data: make(map[string][]byte)}
}

func (m *RWMutexMap) Get(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

func (m *RWMutexMap) Put(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = append([]byte(nil), value...)
}

func (m *RWMutexMap) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}
