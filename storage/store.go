package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// defaultFlushThreshold is the memtable size (in approximate bytes, see
// Memtable.size) at which Store flushes it to a new SSTable.
const defaultFlushThreshold = 4 * 1024 * 1024

// sstHandle is one open, live SSTable: its identity (for the manifest and
// for filenames) plus a reader ready to serve lookups against it.
type sstHandle struct {
	meta   sstFileMeta
	reader *SSTableReader
}

// Store is a durable, LSM-style key-value store: a WAL-backed memtable
// backed by a chain of immutable, size-tiered-compacted SSTables on disk.
//
// Its Get/Put/Delete signatures are deliberately shaped like
// engine.Engine's, but Put/Delete return an error (disk I/O can fail) —
// Store does not implement engine.Engine as-is in M2; that's left for a
// later thin adapter, once Store is wired into the rest of the system.
type Store struct {
	mu       sync.RWMutex
	dir      string
	wal      *WAL
	walPath  string
	mem      *Memtable
	manifest *Manifest
	sstables []*sstHandle // newest-first by creation Seq, across all tiers

	nextSeq        uint64
	flushThreshold int
	tierFanout     int

	compactCh chan struct{}
	flushDone chan struct{}
	stopCh    chan struct{}
	wg        sync.WaitGroup

	errMu sync.Mutex
	err   error
}

// Open opens (creating if necessary) a Store rooted at dir, replaying any
// WAL left over from a prior run and discovering existing SSTables via the
// manifest. flushThreshold is the memtable size that triggers a flush; 0
// selects defaultFlushThreshold (mainly useful for tests that want to
// force frequent, small flushes).
func Open(dir string, flushThreshold int) (*Store, error) {
	if flushThreshold <= 0 {
		flushThreshold = defaultFlushThreshold
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	manifest, live, err := OpenManifest(filepath.Join(dir, "MANIFEST"))
	if err != nil {
		return nil, err
	}

	if err := removeOrphanSSTables(dir, live); err != nil {
		return nil, err
	}

	s := &Store{
		dir:            dir,
		manifest:       manifest,
		flushThreshold: flushThreshold,
		tierFanout:     defaultTierFanout,
		compactCh:      make(chan struct{}, 1),
		flushDone:      make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
	}

	for _, meta := range live {
		if meta.Seq+1 > s.nextSeq {
			s.nextSeq = meta.Seq + 1
		}
		reader, err := OpenSSTableReader(filepath.Join(dir, sstableFileName(meta)))
		if err != nil {
			return nil, err
		}
		s.sstables = append(s.sstables, &sstHandle{meta: meta, reader: reader})
	}
	sortHandlesBySeqDesc(s.sstables)

	if err := s.recoverWAL(); err != nil {
		return nil, err
	}

	walPath, _ := s.newWALPath()
	wal, err := OpenWAL(walPath)
	if err != nil {
		return nil, err
	}
	s.wal = wal
	s.walPath = walPath
	s.mem = NewMemtable()

	s.wg.Add(1)
	go s.compactionLoop()

	return s, nil
}

// newWALPath allocates the next global seq and returns the path for a new
// active WAL file. Caller must not hold s.mu (only used during Open,
// before any goroutine but this one can see s).
func (s *Store) newWALPath() (string, uint64) {
	seq := s.nextSeq
	s.nextSeq++
	return filepath.Join(s.dir, walFileName(seq)), seq
}

func walFileName(seq uint64) string {
	return fmt.Sprintf("wal-%d.log", seq)
}

// recoverWAL replays any WAL file(s) left over from a prior run into a
// throwaway memtable and, if they held any data, flushes that memtable to
// a brand-new SSTable immediately — so recovered writes are durably on
// disk again right away, rather than sitting only in memory where a second
// crash before the next flush would lose them a second time. It then
// deletes the old WAL file(s), which are now redundant with the flushed
// SSTable.
func (s *Store) recoverWAL() error {
	matches, err := filepath.Glob(filepath.Join(s.dir, "wal-*.log"))
	if err != nil {
		return err
	}
	if len(matches) == 0 {
		return nil
	}
	sort.Slice(matches, func(i, j int) bool {
		return walSeqOf(matches[i]) < walSeqOf(matches[j])
	})
	for _, p := range matches {
		if seq := walSeqOf(p); seq+1 > s.nextSeq {
			s.nextSeq = seq + 1
		}
	}

	mem := NewMemtable()
	for _, p := range matches {
		entries, err := ReplayWAL(p)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if e.Type == recPut {
				mem.Put(e.Key, e.Value)
			} else {
				mem.Delete(e.Key)
			}
		}
	}

	if mem.Size() > 0 {
		if err := s.flushToNewSSTable(mem); err != nil {
			return err
		}
	}

	for _, p := range matches {
		if err := os.Remove(p); err != nil {
			return err
		}
	}
	return nil
}

func walSeqOf(path string) uint64 {
	var seq uint64
	fmt.Sscanf(filepath.Base(path), "wal-%d.log", &seq)
	return seq
}

// removeOrphanSSTables deletes any sst-*.sst file in dir that isn't in the
// manifest's live set: either a compaction crashed after writing the file
// but before committing its ADD record, or crashed after committing a DEL
// record but before deleting the file. Both are safe to delete.
func removeOrphanSSTables(dir string, live []sstFileMeta) error {
	liveSet := make(map[string]bool, len(live))
	for _, m := range live {
		liveSet[sstableFileName(m)] = true
	}

	matches, err := filepath.Glob(filepath.Join(dir, "sst-*.sst"))
	if err != nil {
		return err
	}
	for _, p := range matches {
		if !liveSet[filepath.Base(p)] {
			if err := os.Remove(p); err != nil {
				return err
			}
		}
	}
	return nil
}

// Get looks up key. ok is false if key doesn't exist or was deleted. err is
// non-nil only on an I/O failure, including a checksum mismatch
// (ErrCorruptBlock) in a data block that would have contained key — that
// must never be reported as a plain miss.
func (s *Store) Get(key string) (value []byte, ok bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if v, tomb, found := s.mem.Get(key); found {
		if tomb {
			return nil, false, nil
		}
		return v, true, nil
	}

	for _, h := range s.sstables {
		v, tomb, found, err := h.reader.Get(key)
		if err != nil {
			return nil, false, err
		}
		if found {
			if tomb {
				return nil, false, nil
			}
			return v, true, nil
		}
	}
	return nil, false, nil
}

func (s *Store) Put(key string, value []byte) error {
	return s.write(key, value, false)
}

func (s *Store) Delete(key string) error {
	return s.write(key, nil, true)
}

// write appends the record to the WAL (fsyncing before it returns) and
// only then applies it to the memtable — write-ahead, exactly as the name
// says. The WAL append and the memtable mutation happen while holding
// s.mu for reading, so a concurrent flush's swap of (wal, mem) for a new
// pair (which takes s.mu for writing) can never happen in the middle of
// this sequence: either this write completes entirely against the old
// pair, or it hasn't started yet and sees the new one.
func (s *Store) write(key string, value []byte, tombstone bool) error {
	s.mu.RLock()
	wal := s.wal
	mem := s.mem

	rt := recPut
	if tombstone {
		rt = recDel
	}
	if err := wal.Append(rt, key, value); err != nil {
		s.mu.RUnlock()
		return err
	}
	if tombstone {
		mem.Delete(key)
	} else {
		mem.Put(key, value)
	}
	full := mem.Size() >= s.flushThreshold
	s.mu.RUnlock()

	if full {
		s.triggerFlush(mem)
	}
	return nil
}

// triggerFlush swaps out mem (and rotates the WAL feeding it) for a fresh
// empty pair, then hands the actual disk write off to a background
// goroutine so the caller's write path isn't blocked on it. The pointer
// swap itself is synchronous and cheap, which bounds memtable growth even
// under a fast, sustained write burst.
func (s *Store) triggerFlush(mem *Memtable) {
	s.mu.Lock()
	if s.mem != mem {
		s.mu.Unlock()
		return // another writer already triggered this flush
	}
	oldWAL, oldWALPath := s.wal, s.walPath

	newWALPath, _ := s.newWALPath()
	newWAL, err := OpenWAL(newWALPath)
	if err != nil {
		s.mu.Unlock()
		s.setErr(err)
		return
	}
	s.wal, s.walPath = newWAL, newWALPath
	s.mem = NewMemtable()
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			select {
			case s.flushDone <- struct{}{}:
			default:
			}
		}()
		if err := s.flushToNewSSTable(mem); err != nil {
			s.setErr(err)
			return
		}
		oldWAL.Close()
		os.Remove(oldWALPath)
		select {
		case s.compactCh <- struct{}{}:
		default:
		}
	}()
}

// flushToNewSSTable writes every entry in mem out to a new tier-0 SSTable,
// commits it to the manifest, opens a reader for it, and makes it visible
// to Get by prepending it to s.sstables.
func (s *Store) flushToNewSSTable(mem *Memtable) error {
	if mem.Size() == 0 {
		return nil
	}

	s.mu.Lock()
	seq := s.nextSeq
	s.nextSeq++
	s.mu.Unlock()

	meta := sstFileMeta{Gen: 0, Seq: seq}
	path := filepath.Join(s.dir, sstableFileName(meta))

	w, err := NewSSTableWriter(path)
	if err != nil {
		return err
	}
	var addErr error
	mem.Iterate(func(key string, value []byte, tombstone bool) {
		if addErr != nil {
			return
		}
		addErr = w.Add(key, value, tombstone)
	})
	if addErr != nil {
		return addErr
	}
	if err := w.Finish(); err != nil {
		return err
	}

	if err := s.manifest.AddTable(meta); err != nil {
		return err
	}
	reader, err := OpenSSTableReader(path)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.sstables = append([]*sstHandle{{meta: meta, reader: reader}}, s.sstables...)
	s.mu.Unlock()
	return nil
}

func (s *Store) compactionLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stopCh:
			return
		case <-s.compactCh:
			s.runCompactionCycle()
		}
	}
}

// waitForFlush blocks until the background flush goroutine triggered by
// triggerFlush finishes its attempt, or timeout elapses. Test-only
// synchronization: tests live in this package and call it right after a
// Put/Delete they know crossed the flush threshold, before issuing the
// next such write (so signals never need to coalesce across flushes).
func (s *Store) waitForFlush(timeout time.Duration) bool {
	select {
	case <-s.flushDone:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (s *Store) setErr(err error) {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

// Err returns the first background (flush or compaction) error observed
// since Open, if any.
func (s *Store) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

// Close stops the background compaction goroutine, waits for it and any
// in-flight flush to finish, then closes the active WAL, every open
// SSTable reader, and the manifest.
func (s *Store) Close() error {
	close(s.stopCh)
	s.wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()

	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	record(s.wal.Close())
	for _, h := range s.sstables {
		record(h.reader.Close())
	}
	record(s.manifest.Close())
	return firstErr
}
