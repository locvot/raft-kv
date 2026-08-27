package engine

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
)

// RCUShardedMap applies read-copy-update per shard: readers only do an
// atomic pointer load (no lock, no shared mutable counter to contend on),
// while writers serialize among themselves (per shard) to copy-modify-store
// a new map. Reads are as close to zero-synchronization-cost as Go allows;
// writes pay O(shard size) to copy the shard's map on every Put/Delete.
type RCUShardedMap struct {
	shards    []rcuShard
	numShards uint64
}

type rcuShard struct {
	writeMu sync.Mutex // serializes writers only; readers never take this
	m       atomic.Pointer[map[string][]byte]
}

func NewRCUShardedMap(numShards int) *RCUShardedMap {
	if numShards <= 0 {
		numShards = defaultShards
	}

	sm := &RCUShardedMap{
		shards:    make([]rcuShard, numShards),
		numShards: uint64(numShards),
	}
	for i := range sm.shards {
		empty := make(map[string][]byte)
		sm.shards[i].m.Store(&empty)
	}
	return sm
}

func (sm *RCUShardedMap) Get(key string) ([]byte, bool) {
	shard := &sm.shards[sm.getShardIndex(key)]
	m := shard.m.Load()
	v, ok := (*m)[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

func (sm *RCUShardedMap) Put(key string, value []byte) {
	shard := &sm.shards[sm.getShardIndex(key)]

	shard.writeMu.Lock()
	defer shard.writeMu.Unlock()

	old := shard.m.Load()
	next := make(map[string][]byte, len(*old)+1)
	for k, v := range *old {
		next[k] = v
	}
	next[key] = append([]byte(nil), value...)
	shard.m.Store(&next)
}

func (sm *RCUShardedMap) Delete(key string) {
	shard := &sm.shards[sm.getShardIndex(key)]

	shard.writeMu.Lock()
	defer shard.writeMu.Unlock()

	old := shard.m.Load()
	if _, ok := (*old)[key]; !ok {
		return
	}
	next := make(map[string][]byte, len(*old))
	for k, v := range *old {
		if k == key {
			continue
		}
		next[k] = v
	}
	shard.m.Store(&next)
}

func (sm *RCUShardedMap) getShardIndex(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64() % sm.numShards
}
