package engine

import "sync"

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
