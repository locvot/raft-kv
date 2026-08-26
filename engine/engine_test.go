package engine

import (
	"fmt"
	"sync"
	"testing"
)

// testEngineBasic is shared across every Engine implementation (MutexMap,
// and later ShardedMap) so they're all held to the exact same contract.
func testEngineBasic(t *testing.T, e Engine) {
	t.Helper()

	if _, ok := e.Get("missing"); ok {
		t.Fatalf("Get(missing) = ok, want !ok")
	}

	e.Put("k", []byte("v1"))
	if v, ok := e.Get("k"); !ok || string(v) != "v1" {
		t.Fatalf("Get(k) = (%q, %v), want (v1, true)", v, ok)
	}

	e.Put("k", []byte("v2"))
	if v, _ := e.Get("k"); string(v) != "v2" {
		t.Fatalf("Get(k) after overwrite = %q, want v2", v)
	}

	e.Delete("k")
	if _, ok := e.Get("k"); ok {
		t.Fatalf("Get(k) after Delete = ok, want !ok")
	}

	// Delete of a key that was never set must be a silent no-op.
	e.Delete("never-set")
}

// testEngineCopySemantics checks the contract documented on Engine: Put
// must copy its input, and Get must return a copy, so callers mutating
// either slice can never corrupt the engine's internal state.
func testEngineCopySemantics(t *testing.T, e Engine) {
	t.Helper()

	input := []byte("original")
	e.Put("k", input)
	input[0] = 'X' // mutate the caller's copy after Put returns

	v, _ := e.Get("k")
	if string(v) != "original" {
		t.Fatalf("Put did not copy its input: stored value changed to %q after caller mutated its own slice", v)
	}

	v[0] = 'Y' // mutate the slice returned by Get
	v2, _ := e.Get("k")
	if string(v2) != "original" {
		t.Fatalf("Get did not return a copy: stored value changed to %q after caller mutated the returned slice", v2)
	}
}

// testEngineConcurrent hammers e from many goroutines at once across a
// small, shared keyspace so Get/Put/Delete on the same keys actually
// overlap — the scenario -race is meant to catch.
func testEngineConcurrent(t *testing.T, e Engine) {
	t.Helper()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i%10)
			switch i % 3 {
			case 0:
				e.Put(key, []byte{byte(i)})
			case 1:
				e.Get(key)
			case 2:
				e.Delete(key)
			}
		}(i)
	}
	wg.Wait()
}

func TestMutexMapBasic(t *testing.T) {
	testEngineBasic(t, NewMutexMap())
}

func TestMutexMapCopySemantics(t *testing.T) {
	testEngineCopySemantics(t, NewMutexMap())
}

func TestMutexMapConcurrent(t *testing.T) {
	testEngineConcurrent(t, NewMutexMap())
}

func TestShardedMapBasic(t *testing.T) {
	testEngineBasic(t, NewShardedMap(0))
}

func TestShardedMapCopySemantics(t *testing.T) {
	testEngineCopySemantics(t, NewShardedMap(0))
}

func TestShardedMapConcurrent(t *testing.T) {
	testEngineConcurrent(t, NewShardedMap(0))
}
