package transport

import (
	"bytes"
	"encoding/gob"
)

// raft.LogEntry.Command is an interface{}; gob can't encode/decode through
// an interface value without a registered concrete type, and raft.go's own
// persist() gob-encodes rf.log (including every Command in it) regardless
// of whether this package's gRPC codec is involved at all — so Command
// must be registered even for a single-process (non-transport) caller of
// raft.Make that happens to submit these commands.
func init() {
	gob.Register(Command{})
}

// op identifies what a Command does to the state machine. Get does not
// have one: M4's read path answers from the leader's local state directly
// (see Server.Get in kv_service.go) rather than going through the log, so
// there is nothing for a read to submit via Start.
type op int

const (
	opPut op = iota
	opDelete
)

// Command is the concrete type raft.LogEntry.Command holds for every entry
// this package submits via rf.Start — Raft itself only ever sees it as an
// interface{}. Fields are exported so gob (Raft's own persistence encoding,
// and this package's wire encoding for AppendEntries.Entries — see
// codec.go) can see them.
type Command struct {
	Op    op
	Key   string
	Value []byte
}

// encodeCommand/decodeCommand are the wire form for LogEntry.command in
// raftkv.proto: gob, matching how raft/persist.go already encodes the log
// for on-disk persistence, so there's exactly one serialization scheme for
// a Command to get right rather than two.
func encodeCommand(cmd Command) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		panic("transport: encode command: " + err.Error())
	}
	return buf.Bytes()
}

func decodeCommand(data []byte) (Command, error) {
	var cmd Command
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&cmd); err != nil {
		return Command{}, err
	}
	return cmd, nil
}
