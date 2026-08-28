package engine

import (
	"hash/fnv"
	"sync"
)

const defaultShards = 128

type ShardedMap struct {
	shards    []shard
	numShards uint64
}

// shard holds one bucket's key-value data and its protecting mutex.
type shard struct {
	mu   sync.Mutex
	data map[string][]byte
}

func NewShardedMap(numShards int) *ShardedMap {
	if numShards <= 0 {
		numShards = defaultShards
	}

	sm := &ShardedMap{
		shards:    make([]shard, numShards),
		numShards: uint64(numShards),
	}

	for i := 0; i < numShards; i++ {
		sm.shards[i].data = make(map[string][]byte)
	}
	return sm
}

func (sm *ShardedMap) Get(key string) ([]byte, bool) {
	shardIndex := sm.getShardIndex(key)
	shard := &sm.shards[shardIndex]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	val, ok := shard.data[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), val...), true
}

func (sm *ShardedMap) Put(key string, value []byte) {
	shardIndex := sm.getShardIndex(key)
	shard := &sm.shards[shardIndex]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.data[key] = append([]byte(nil), value...)
}

func (sm *ShardedMap) Delete(key string) {
	shardIndex := sm.getShardIndex(key)
	shard := &sm.shards[shardIndex]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	delete(shard.data, key)
}

func (sm *ShardedMap) getShardIndex(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64() % sm.numShards
}
