// Package raft implements the Raft consensus algorithm. Peers talk to each
// other only through the simnet.ClientEnd.Call interface (see raft.go), so
// the same code runs unmodified against simharness in tests and against a
// real gRPC transport in production.
package raft
