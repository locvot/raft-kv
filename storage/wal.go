package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

type recordType uint8

const (
	recPut recordType = 1
	recDel recordType = 2
)

// WAL is an append-only, fsync-before-return log. Store calls Append before
// touching the memtable and before returning to the caller, so a write is
// never acknowledged until it is durable on disk.
type WAL struct {
	mu sync.Mutex
	f  *os.File
}

// OpenWAL opens (creating if necessary) the WAL file at path for appending.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f}, nil
}

// Append writes one record and fsyncs it before returning. On-disk layout:
//
//	crc32(4B) | recType(1B) | keyLen(varint) | key | [valLen(varint) | value]
//
// The trailing valLen/value pair is only present for recPut; recDel carries
// no value.
func (w *WAL) Append(rt recordType, key string, value []byte) error {
	var payload bytes.Buffer
	payload.WriteByte(byte(rt))

	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(key)))
	payload.Write(lenBuf[:n])
	payload.WriteString(key)

	if rt == recPut {
		n = binary.PutUvarint(lenBuf[:], uint64(len(value)))
		payload.Write(lenBuf[:n])
		payload.Write(value)
	}

	crc := crc32.ChecksumIEEE(payload.Bytes())

	w.mu.Lock()
	defer w.mu.Unlock()

	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc)
	if _, err := w.f.Write(crcBuf[:]); err != nil {
		return err
	}
	if _, err := w.f.Write(payload.Bytes()); err != nil {
		return err
	}
	return w.f.Sync()
}

// Close closes the underlying file. It does not delete it.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}

// WALEntry is one decoded record, as returned by ReplayWAL.
type WALEntry struct {
	Type  recordType
	Key   string
	Value []byte
}

// ReplayWAL decodes every well-formed record in the WAL file at path, in
// write order. A crash can leave a torn final record (partially written,
// or with a CRC that doesn't match its payload) — ReplayWAL treats that as
// the logical end of the log, returning every complete record that
// preceded it, rather than failing the whole replay.
//
// A missing file is not an error: it just means there is nothing to
// replay (e.g. first run, or the WAL was already cleaned up after a
// successful flush).
func ReplayWAL(path string) ([]WALEntry, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []WALEntry
	r := bufio.NewReader(f)
	for {
		entry, ok := readWALRecord(r)
		if !ok {
			break
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// readWALRecord decodes one record from r. ok is false if the record is
// missing, truncated, or fails its checksum — any of which mark the end of
// a usable replay.
func readWALRecord(r *bufio.Reader) (entry WALEntry, ok bool) {
	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return WALEntry{}, false
	}
	wantCRC := binary.BigEndian.Uint32(crcBuf[:])

	var payload bytes.Buffer

	rtByte, err := r.ReadByte()
	if err != nil {
		return WALEntry{}, false
	}
	payload.WriteByte(rtByte)
	rt := recordType(rtByte)
	if rt != recPut && rt != recDel {
		return WALEntry{}, false
	}

	key, err := readLenPrefixed(r, &payload)
	if err != nil {
		return WALEntry{}, false
	}

	var value []byte
	if rt == recPut {
		value, err = readLenPrefixed(r, &payload)
		if err != nil {
			return WALEntry{}, false
		}
	}

	if crc32.ChecksumIEEE(payload.Bytes()) != wantCRC {
		return WALEntry{}, false
	}

	return WALEntry{Type: rt, Key: string(key), Value: value}, true
}

// echoByteReader wraps a *bufio.Reader and copies every byte it reads into
// buf, so a caller decoding a varint one byte at a time can still recover
// the exact bytes consumed (needed to recompute the record's checksum).
type echoByteReader struct {
	r   *bufio.Reader
	buf *bytes.Buffer
}

func (e *echoByteReader) ReadByte() (byte, error) {
	b, err := e.r.ReadByte()
	if err != nil {
		return 0, err
	}
	e.buf.WriteByte(b)
	return b, nil
}

// readLenPrefixed reads a varint length followed by that many bytes from r,
// echoing everything it reads into echo so the caller can recompute the
// record's checksum.
func readLenPrefixed(r *bufio.Reader, echo *bytes.Buffer) ([]byte, error) {
	length, err := binary.ReadUvarint(&echoByteReader{r: r, buf: echo})
	if err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	echo.Write(buf)
	return buf, nil
}
