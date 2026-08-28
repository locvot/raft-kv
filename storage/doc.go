// Package storage is a durable, LSM-style key-value storage engine: a
// write-ahead log, an in-memory memtable, and a chain of immutable,
// size-tiered-compacted SSTables on disk. See Store for the entry point.
package storage
