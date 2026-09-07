package transport

import "testing"

// TestServerCloseRightAfterServe is a regression test for a data race
// between Serve (which assigns s.grpcServer) and Close (which reads it) —
// see the comment on Serve in server.go. It deliberately does nothing
// between cluster creation and the end of the test, so t.Cleanup's Close()
// races Serve()'s background goroutine as tightly as possible: every
// earlier test happened to do something (wait for an election, Put/Get)
// that gave Serve time to run first, which is exactly how this race went
// unnoticed until a test this fast existed. Run with -race.
func TestServerCloseRightAfterServe(t *testing.T) {
	newTestCluster(t, 1)
}
