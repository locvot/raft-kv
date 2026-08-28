package storage

import (
	"math/rand"
	"sync"
	"time"
)

// skipListMaxLevel bounds how many forward pointers a node can have.
// skipListP is the probability a node gets promoted to the next level up
// (the classic Pugh skip list default). With p=0.25 and maxLevel=16, the
// structure comfortably supports well beyond any memtable size this
// project's flushThreshold would ever let one grow to before it's flushed.
const (
	skipListMaxLevel = 16
	skipListP        = 0.25
)

type skipListNode struct {
	key       string
	value     []byte
	tombstone bool
	forward   []*skipListNode // forward[i] = next node at level i
}

// SkipListMemtable is an in-memory, write-buffering front end for a
// Store, backed by a skip list — a linked structure with
// probabilistically-assigned "express lane" pointers giving O(log n)
// search/insert regardless of how many entries it holds.
//
// This implementation still guards the whole structure with one
// sync.RWMutex — it is NOT the classic lock-free skip list (that needs
// atomic pointer CAS per node, a much larger undertaking).
type SkipListMemtable struct {
	mu    sync.RWMutex
	head  *skipListNode
	level int // current highest in-use level, 1-indexed
	size  int
	rnd   *rand.Rand
}

func NewSkipListMemtable() *SkipListMemtable {
	return &SkipListMemtable{
		head:  &skipListNode{forward: make([]*skipListNode, skipListMaxLevel)},
		level: 1,
		rnd:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *SkipListMemtable) randomLevel() int {
	lvl := 1
	for lvl < skipListMaxLevel && s.rnd.Float64() < skipListP {
		lvl++
	}
	return lvl
}

// findPredecessors returns, for each level, the rightmost node whose key
// is < key — i.e. the node key would be inserted after at that level.
// Caller must hold s.mu.
func (s *SkipListMemtable) findPredecessors(key string) []*skipListNode {
	update := make([]*skipListNode, skipListMaxLevel)
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
		update[i] = x
	}
	return update
}

func (s *SkipListMemtable) Put(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upsert(key, value, false)
}

func (s *SkipListMemtable) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upsert(key, nil, true)
}

func (s *SkipListMemtable) upsert(key string, value []byte, tombstone bool) {
	update := s.findPredecessors(key)

	if existing := update[0].forward[0]; existing != nil && existing.key == key {
		s.size += len(value) - len(existing.value)
		existing.value = value
		existing.tombstone = tombstone
		return
	}

	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	node := &skipListNode{key: key, value: value, tombstone: tombstone, forward: make([]*skipListNode, lvl)}
	for i := 0; i < lvl; i++ {
		node.forward[i] = update[i].forward[i]
		update[i].forward[i] = node
	}
	s.size += len(key) + len(value)
}

func (s *SkipListMemtable) Get(key string) (value []byte, tombstone, found bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
	}
	x = x.forward[0]
	if x == nil || x.key != key {
		return nil, false, false
	}
	return x.value, x.tombstone, true
}

func (s *SkipListMemtable) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

// Iterate calls fn for every entry in ascending key order — the level-0
// chain is a plain sorted linked list, so this is just a walk of it.
func (s *SkipListMemtable) Iterate(fn func(key string, value []byte, tombstone bool)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for x := s.head.forward[0]; x != nil; x = x.forward[0] {
		fn(x.key, x.value, x.tombstone)
	}
}
