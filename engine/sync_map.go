package engine

import "sync"

type SyncMap struct {
	data sync.Map // string -> []byte
}

func NewSyncMap() *SyncMap {
	return &SyncMap{}
}

func (m *SyncMap) Get(key string) ([]byte, bool) {
	v, ok := m.data.Load(key)
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v.([]byte)...), true
}

func (m *SyncMap) Put(key string, value []byte) {
	m.data.Store(key, append([]byte(nil), value...))
}

func (m *SyncMap) Delete(key string) {
	m.data.Delete(key)
}
