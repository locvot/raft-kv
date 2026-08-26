package simnet

import (
	"testing"
	"time"
)

// Echo is a trivial RPC target used only to exercise the network plumbing —
// it has nothing to do with Raft.
type Echo struct{ calls int32 }

type EchoArgs struct{ N int }
type EchoReply struct{ N int }

func (e *Echo) Double(args EchoArgs, reply *EchoReply) {
	reply.N = args.N * 2
}

func wireUpPair(t *testing.T, rn *Network) (*ClientEnd, *ClientEnd) {
	t.Helper()
	s1, s2 := MakeServer(), MakeServer()
	s1.AddService(MakeService(&Echo{}))
	s2.AddService(MakeService(&Echo{}))
	rn.AddServer("s1", s1)
	rn.AddServer("s2", s2)

	e12 := rn.MakeEnd("1->2")
	rn.Connect("1->2", "s2")
	rn.Enable("1->2", true)

	e21 := rn.MakeEnd("2->1")
	rn.Connect("2->1", "s1")
	rn.Enable("2->1", true)

	return e12, e21
}

func TestBasicCall(t *testing.T) {
	rn := MakeNetwork()
	defer rn.Cleanup()
	e12, _ := wireUpPair(t, rn)

	var reply EchoReply
	ok := e12.Call("Echo.Double", EchoArgs{N: 21}, &reply)
	if !ok {
		t.Fatalf("Call returned false, expected success")
	}
	if reply.N != 42 {
		t.Fatalf("got %d, want 42", reply.N)
	}
}

func TestDisconnectBlocksCalls(t *testing.T) {
	rn := MakeNetwork()
	defer rn.Cleanup()
	e12, _ := wireUpPair(t, rn)

	rn.Enable("1->2", false) // simulate 1 losing its link to 2

	var reply EchoReply
	start := time.Now()
	ok := e12.Call("Echo.Double", EchoArgs{N: 5}, &reply)
	elapsed := time.Since(start)

	if ok {
		t.Fatalf("Call succeeded across a disabled link, expected failure")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("disabled-link failure took %v, expected a short simulated timeout (LongDelays off)", elapsed)
	}
}

func TestReconnectRecovers(t *testing.T) {
	rn := MakeNetwork()
	defer rn.Cleanup()
	e12, _ := wireUpPair(t, rn)

	rn.Enable("1->2", false)
	var reply EchoReply
	if ok := e12.Call("Echo.Double", EchoArgs{N: 1}, &reply); ok {
		t.Fatalf("expected failure while disconnected")
	}

	rn.Enable("1->2", true)
	if ok := e12.Call("Echo.Double", EchoArgs{N: 10}, &reply); !ok {
		t.Fatalf("expected success after reconnecting")
	}
	if reply.N != 20 {
		t.Fatalf("got %d, want 20", reply.N)
	}
}

func TestUnreliableDropsSome(t *testing.T) {
	rn := MakeNetwork()
	defer rn.Cleanup()
	e12, _ := wireUpPair(t, rn)
	rn.Reliable(false)

	const n = 300
	failures := 0
	for i := 0; i < n; i++ {
		var reply EchoReply
		if ok := e12.Call("Echo.Double", EchoArgs{N: i}, &reply); !ok {
			failures++
		} else if reply.N != i*2 {
			t.Fatalf("got %d, want %d", reply.N, i*2)
		}
	}
	// with a ~20% combined request+reply drop rate we expect a healthy chunk
	// of failures, but not all 300 and not zero — this is what makes it
	// "unreliable" rather than "broken".
	if failures == 0 {
		t.Fatalf("Reliable(false) produced zero drops across %d calls — network isn't actually unreliable", n)
	}
	if failures == n {
		t.Fatalf("Reliable(false) dropped every single call — that's not unreliable, that's disconnected")
	}
	t.Logf("unreliable mode: %d/%d calls failed (expected a nonzero, non-total fraction)", failures, n)
}

func TestLongReorderingDelaysSomeReplies(t *testing.T) {
	rn := MakeNetwork()
	defer rn.Cleanup()
	e12, _ := wireUpPair(t, rn)
	rn.Reliable(false)
	rn.LongReordering(true)

	fast, slow := 0, 0
	for i := 0; i < 40; i++ {
		var reply EchoReply
		start := time.Now()
		ok := e12.Call("Echo.Double", EchoArgs{N: i}, &reply)
		elapsed := time.Since(start)
		if !ok {
			continue
		}
		if elapsed > 150*time.Millisecond {
			slow++
		} else {
			fast++
		}
	}
	if slow == 0 {
		t.Fatalf("LongReordering(true) produced no delayed replies out of 40 calls")
	}
	t.Logf("long reordering: %d fast, %d delayed replies", fast, slow)
}

func TestRPCCount(t *testing.T) {
	rn := MakeNetwork()
	defer rn.Cleanup()
	e12, _ := wireUpPair(t, rn)

	for i := 0; i < 5; i++ {
		var reply EchoReply
		e12.Call("Echo.Double", EchoArgs{N: i}, &reply)
	}
	if got := rn.RPCCount(); got != 5 {
		t.Fatalf("RPCCount() = %d, want 5", got)
	}
}
