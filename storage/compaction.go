package storage

import (
	"os"
	"path/filepath"
	"sort"
)

// defaultTierFanout is how many SSTables a tier accumulates before they are
// merged into one table at the next tier up.
const defaultTierFanout = 4

// runCompactionCycle repeatedly compacts the lowest tier that has reached
// tierFanout tables until no tier qualifies anymore, cascading through
// multiple tiers in one call if a merge pushes the next tier over the
// threshold too. It runs on the Store's single background compaction
// goroutine, so no two compactions ever run concurrently.
func (s *Store) runCompactionCycle() {
	for {
		s.mu.RLock()
		groups := groupByTier(s.sstables)
		s.mu.RUnlock()

		gen, tables := pickTierToCompact(groups, s.tierFanout)
		if tables == nil {
			return
		}
		if err := s.compactTier(gen, tables, groups); err != nil {
			s.setErr(err)
			return
		}
	}
}

func groupByTier(handles []*sstHandle) map[int][]*sstHandle {
	m := make(map[int][]*sstHandle)
	for _, h := range handles {
		m[h.meta.Gen] = append(m[h.meta.Gen], h)
	}
	return m
}

// pickTierToCompact returns the numerically lowest (freshest) tier that has
// reached fanout tables, sorted newest-first by creation Seq, or (0, nil)
// if no tier qualifies.
func pickTierToCompact(groups map[int][]*sstHandle, fanout int) (int, []*sstHandle) {
	minGen := -1
	for g, hs := range groups {
		if len(hs) >= fanout && (minGen == -1 || g < minGen) {
			minGen = g
		}
	}
	if minGen == -1 {
		return 0, nil
	}
	tables := append([]*sstHandle(nil), groups[minGen]...)
	sortHandlesBySeqDesc(tables)
	return minGen, tables
}

func sortHandlesBySeqDesc(handles []*sstHandle) {
	sort.Slice(handles, func(i, j int) bool { return handles[i].meta.Seq > handles[j].meta.Seq })
}

// compactTier merges tables (all from tier gen) into one new table at tier
// gen+1, then atomically (manifest first, in-memory list second, physical
// file deletion last) retires the inputs.
func (s *Store) compactTier(gen int, tables []*sstHandle, groups map[int][]*sstHandle) error {
	targetGen := gen + 1

	// A tombstone can only be safely dropped if no older data survives
	// beneath the table we're about to produce — otherwise a stale value
	// sitting in an older tier could "resurrect" after the delete that
	// shadowed it is gone. "Older" here means tier > gen among tables not
	// part of this merge (their own compaction, if any, happens in a later
	// cascade step, each re-evaluating this same rule for its own target).
	maxOtherGen := -1
	for g, hs := range groups {
		if g == gen || len(hs) == 0 {
			continue
		}
		if g > maxOtherGen {
			maxOtherGen = g
		}
	}
	dropTombstones := targetGen > maxOtherGen

	merged, err := mergeTables(tables, dropTombstones)
	if err != nil {
		return err
	}

	s.mu.Lock()
	seq := s.nextSeq
	s.nextSeq++
	s.mu.Unlock()

	newMeta := sstFileMeta{Gen: targetGen, Seq: seq}
	path := filepath.Join(s.dir, sstableFileName(newMeta))

	w, err := NewSSTableWriter(path)
	if err != nil {
		return err
	}
	for _, e := range merged {
		if err := w.Add(e.key, e.value, e.tombstone); err != nil {
			return err
		}
	}
	if err := w.Finish(); err != nil {
		return err
	}

	if err := s.manifest.AddTable(newMeta); err != nil {
		return err
	}
	reader, err := OpenSSTableReader(path)
	if err != nil {
		return err
	}

	oldMetas := make(map[sstFileMeta]bool, len(tables))
	for _, h := range tables {
		oldMetas[h.meta] = true
	}

	s.mu.Lock()
	newList := make([]*sstHandle, 0, len(s.sstables)-len(tables)+1)
	newList = append(newList, &sstHandle{meta: newMeta, reader: reader})
	var retired []*sstHandle
	for _, h := range s.sstables {
		if oldMetas[h.meta] {
			retired = append(retired, h)
			continue
		}
		newList = append(newList, h)
	}
	sortHandlesBySeqDesc(newList)
	s.sstables = newList
	s.mu.Unlock()

	for _, h := range retired {
		if err := s.manifest.RemoveTable(h.meta); err != nil {
			s.setErr(err)
			continue
		}
		h.reader.Close()
		os.Remove(filepath.Join(s.dir, sstableFileName(h.meta)))
	}
	return nil
}

// mergeTables reads every entry out of tables (which must be given
// newest-first) and returns the deduplicated, ascending-key-order result:
// for a key present in more than one input, the entry from the newest
// table wins. This loads every input table fully into memory, which is a
// deliberate simplification appropriate at this project's scale — see
// doc/DECISIONS.md on choosing size-tiered over leveled compaction.
func mergeTables(tables []*sstHandle, dropTombstones bool) ([]sstEntry, error) {
	merged := make(map[string]sstEntry)
	for _, h := range tables {
		entries, err := h.reader.readAllEntries()
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if _, exists := merged[e.key]; !exists {
				merged[e.key] = e
			}
		}
	}

	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]sstEntry, 0, len(keys))
	for _, k := range keys {
		e := merged[k]
		if dropTombstones && e.tombstone {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}
