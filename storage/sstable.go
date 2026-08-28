package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// sstableMagic identifies a valid SSTable footer.
const sstableMagic uint64 = 0x4D494E494B565331 // "MINIKVS1"

// defaultBlockSize is the target size (in bytes of entry payload, before
// the checksum) of one SSTable data block.
const defaultBlockSize = 8 * 1024

// footerSize is indexOffset(8) + indexLength(8) + magic(8).
const footerSize = 24

// ErrCorruptBlock is returned when a block's stored CRC32 doesn't match its
// payload — the block was corrupted on disk (or truncated) after being
// written. Callers must treat this as a hard error, never as "not found".
var ErrCorruptBlock = errors.New("storage: corrupt block (checksum mismatch)")

// On-disk format:
//
//	file := block* index-block footer
//	block := crc32(4B) payload
//	entry := keyLen(varint) key tombstone(1B) valLen(varint) value
//	payload := entry*                                  // data block
//	         | (keyLen(varint) key offset(8B))*         // index block
//	footer := indexOffset(8B) indexLength(8B) magic(8B)
//
// Index entries map a data block's first key to that block's file offset
// (the offset of its crc32 field), so a reader can binary-search the index
// to find the one data block that could contain a given key.

// SSTableWriter writes entries, which must arrive in strictly ascending
// key order, into a new SSTable file.
type SSTableWriter struct {
	f         *os.File
	blockSize int

	cur         bytes.Buffer
	curFirstKey string
	haveCur     bool

	index []indexEntry

	offset      int64
	lastKey     string
	haveLastKey bool
}

type indexEntry struct {
	key    string
	offset int64
}

// NewSSTableWriter creates path (truncating any existing file) and returns
// a writer using the default block size.
func NewSSTableWriter(path string) (*SSTableWriter, error) {
	return NewSSTableWriterSize(path, defaultBlockSize)
}

// NewSSTableWriterSize is NewSSTableWriter with an explicit block size,
// mainly so tests can force multiple small blocks without huge inputs.
func NewSSTableWriterSize(path string, blockSize int) (*SSTableWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &SSTableWriter{f: f, blockSize: blockSize}, nil
}

// Add appends one entry. key must be strictly greater than the key passed
// to the previous call.
func (w *SSTableWriter) Add(key string, value []byte, tombstone bool) error {
	if w.haveLastKey && key <= w.lastKey {
		return fmt.Errorf("storage: SSTableWriter.Add: key %q is not greater than previous key %q", key, w.lastKey)
	}
	w.lastKey = key
	w.haveLastKey = true

	if !w.haveCur {
		w.curFirstKey = key
		w.haveCur = true
	}
	writeEntry(&w.cur, key, value, tombstone)

	if w.cur.Len() >= w.blockSize {
		return w.flushBlock()
	}
	return nil
}

// flushBlock writes the buffered block (if any) to disk and records its
// index entry.
func (w *SSTableWriter) flushBlock() error {
	if w.cur.Len() == 0 {
		return nil
	}
	n, err := writeBlock(w.f, w.cur.Bytes())
	if err != nil {
		return err
	}
	w.index = append(w.index, indexEntry{key: w.curFirstKey, offset: w.offset})
	w.offset += n
	w.cur.Reset()
	w.haveCur = false
	return nil
}

// Finish flushes any buffered block, writes the index block and footer,
// fsyncs, and closes the file. The writer must not be used afterward.
func (w *SSTableWriter) Finish() error {
	if err := w.flushBlock(); err != nil {
		return err
	}

	indexOffset := w.offset
	var indexPayload bytes.Buffer
	for _, e := range w.index {
		var lenBuf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(lenBuf[:], uint64(len(e.key)))
		indexPayload.Write(lenBuf[:n])
		indexPayload.WriteString(e.key)
		var offBuf [8]byte
		binary.BigEndian.PutUint64(offBuf[:], uint64(e.offset))
		indexPayload.Write(offBuf[:])
	}
	indexLen, err := writeBlock(w.f, indexPayload.Bytes())
	if err != nil {
		return err
	}

	var footer [footerSize]byte
	binary.BigEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.BigEndian.PutUint64(footer[8:16], uint64(indexLen))
	binary.BigEndian.PutUint64(footer[16:24], sstableMagic)
	if _, err := w.f.Write(footer[:]); err != nil {
		return err
	}

	if err := w.f.Sync(); err != nil {
		return err
	}
	return w.f.Close()
}

// writeBlock writes crc32(payload)+payload to f and returns the number of
// bytes written.
func writeBlock(f *os.File, payload []byte) (int64, error) {
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc32.ChecksumIEEE(payload))
	if _, err := f.Write(crcBuf[:]); err != nil {
		return 0, err
	}
	if _, err := f.Write(payload); err != nil {
		return 0, err
	}
	return int64(4 + len(payload)), nil
}

func writeEntry(buf *bytes.Buffer, key string, value []byte, tombstone bool) {
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(key)))
	buf.Write(lenBuf[:n])
	buf.WriteString(key)

	if tombstone {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}

	n = binary.PutUvarint(lenBuf[:], uint64(len(value)))
	buf.Write(lenBuf[:n])
	buf.Write(value)
}

func readEntry(r *bytes.Reader) (key string, value []byte, tombstone bool, err error) {
	klen, err := binary.ReadUvarint(r)
	if err != nil {
		return "", nil, false, err
	}
	keyBuf := make([]byte, klen)
	if _, err := io.ReadFull(r, keyBuf); err != nil {
		return "", nil, false, err
	}

	flag, err := r.ReadByte()
	if err != nil {
		return "", nil, false, err
	}

	vlen, err := binary.ReadUvarint(r)
	if err != nil {
		return "", nil, false, err
	}
	valBuf := make([]byte, vlen)
	if vlen > 0 {
		if _, err := io.ReadFull(r, valBuf); err != nil {
			return "", nil, false, err
		}
	}

	return string(keyBuf), valBuf, flag == 1, nil
}

// SSTableReader serves point lookups against an immutable SSTable file. Its
// index is loaded fully into memory on open.
type SSTableReader struct {
	f       *os.File
	index   []indexEntry
	dataEnd int64 // end of the last data block == indexOffset
}

// OpenSSTableReader opens path and loads its index block into memory.
func OpenSSTableReader(path string) (*SSTableReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if info.Size() < footerSize {
		f.Close()
		return nil, fmt.Errorf("storage: %s: too small to be an SSTable", path)
	}

	var footer [footerSize]byte
	if _, err := f.ReadAt(footer[:], info.Size()-footerSize); err != nil {
		f.Close()
		return nil, err
	}
	indexOffset := int64(binary.BigEndian.Uint64(footer[0:8]))
	indexLength := int64(binary.BigEndian.Uint64(footer[8:16]))
	magic := binary.BigEndian.Uint64(footer[16:24])
	if magic != sstableMagic {
		f.Close()
		return nil, fmt.Errorf("storage: %s: bad magic number, not an SSTable", path)
	}

	indexBuf := make([]byte, indexLength)
	if _, err := f.ReadAt(indexBuf, indexOffset); err != nil {
		f.Close()
		return nil, err
	}
	payload, err := verifyAndStripCRC(indexBuf)
	if err != nil {
		f.Close()
		return nil, err
	}

	var index []indexEntry
	pr := bytes.NewReader(payload)
	for pr.Len() > 0 {
		klen, err := binary.ReadUvarint(pr)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("storage: %s: corrupt index: %w", path, err)
		}
		keyBuf := make([]byte, klen)
		if _, err := io.ReadFull(pr, keyBuf); err != nil {
			f.Close()
			return nil, fmt.Errorf("storage: %s: corrupt index: %w", path, err)
		}
		var offBuf [8]byte
		if _, err := io.ReadFull(pr, offBuf[:]); err != nil {
			f.Close()
			return nil, fmt.Errorf("storage: %s: corrupt index: %w", path, err)
		}
		index = append(index, indexEntry{key: string(keyBuf), offset: int64(binary.BigEndian.Uint64(offBuf[:]))})
	}

	return &SSTableReader{f: f, index: index, dataEnd: indexOffset}, nil
}

// verifyAndStripCRC checks block[:4] against crc32(block[4:]) and returns
// block[4:] (the payload) on success.
func verifyAndStripCRC(block []byte) ([]byte, error) {
	if len(block) < 4 {
		return nil, ErrCorruptBlock
	}
	want := binary.BigEndian.Uint32(block[:4])
	payload := block[4:]
	if crc32.ChecksumIEEE(payload) != want {
		return nil, ErrCorruptBlock
	}
	return payload, nil
}

// blockRange returns the [start, end) byte range of the data block at
// index i.
func (r *SSTableReader) blockRange(i int) (start, end int64) {
	start = r.index[i].offset
	if i+1 < len(r.index) {
		end = r.index[i+1].offset
	} else {
		end = r.dataEnd
	}
	return start, end
}

// Get looks up key in this table. found is false if the table has no entry
// for key at all. err is ErrCorruptBlock if the data block that would
// contain key fails its checksum — the caller must not treat that as
// "not found".
func (r *SSTableReader) Get(key string) (value []byte, tombstone bool, found bool, err error) {
	if len(r.index) == 0 {
		return nil, false, false, nil
	}
	// Rightmost block whose first key is <= key.
	i := sortSearchLastLE(r.index, key)
	if i < 0 {
		return nil, false, false, nil
	}

	start, end := r.blockRange(i)
	raw := make([]byte, end-start)
	if _, err := r.f.ReadAt(raw, start); err != nil {
		return nil, false, false, err
	}
	payload, err := verifyAndStripCRC(raw)
	if err != nil {
		return nil, false, false, err
	}

	br := bytes.NewReader(payload)
	for br.Len() > 0 {
		k, v, tomb, err := readEntry(br)
		if err != nil {
			return nil, false, false, ErrCorruptBlock
		}
		if k == key {
			return v, tomb, true, nil
		}
		if k > key {
			break
		}
	}
	return nil, false, false, nil
}

// sortSearchLastLE returns the index of the rightmost entry whose key is
// <= target, or -1 if every entry's key is greater than target.
func sortSearchLastLE(index []indexEntry, target string) int {
	lo, hi := 0, len(index)-1
	res := -1
	for lo <= hi {
		mid := (lo + hi) / 2
		if index[mid].key <= target {
			res = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return res
}

// Close closes the underlying file.
func (r *SSTableReader) Close() error {
	return r.f.Close()
}

// readAllEntries returns every entry in the table, in ascending key order,
// tombstones included. Used by compaction, which merges whole tables in
// memory.
func (r *SSTableReader) readAllEntries() ([]sstEntry, error) {
	var out []sstEntry
	for i := range r.index {
		start, end := r.blockRange(i)
		raw := make([]byte, end-start)
		if _, err := r.f.ReadAt(raw, start); err != nil {
			return nil, err
		}
		payload, err := verifyAndStripCRC(raw)
		if err != nil {
			return nil, err
		}
		br := bytes.NewReader(payload)
		for br.Len() > 0 {
			k, v, tomb, err := readEntry(br)
			if err != nil {
				return nil, ErrCorruptBlock
			}
			out = append(out, sstEntry{key: k, value: v, tombstone: tomb})
		}
	}
	return out, nil
}

type sstEntry struct {
	key       string
	value     []byte
	tombstone bool
}
