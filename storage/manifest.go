package storage

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

// sstFileMeta identifies one SSTable file by its tier (gen, 0 = freshest)
// and its globally increasing creation sequence number (used to break ties
// on "which source is newer" during reads and compaction merges).
type sstFileMeta struct {
	Gen int
	Seq uint64
}

func sstableFileName(meta sstFileMeta) string {
	return fmt.Sprintf("sst-%d-%d.sst", meta.Gen, meta.Seq)
}

// Manifest is an append-only log of SSTable file add/remove events. It is
// the source of truth for which SSTable files are actually live: a file
// present on disk with no matching ADD record is garbage from a compaction
// that crashed before committing, and a file with a DEL record that never
// got deleted is garbage left over from one that crashed after committing
// but before cleanup. Both cases are handled by OpenManifest's caller
// during recovery (see Store.Open).
//
// This ADD/DEL-log approach mirrors how real LSM engines (e.g. LevelDB's
// MANIFEST) track live files, rather than trusting a directory listing.
type Manifest struct {
	mu sync.Mutex
	f  *os.File
}

// OpenManifest opens (creating if necessary) the manifest at path, replays
// it, and returns both the manifest handle (for further appends) and the
// current set of live SSTable files.
func OpenManifest(path string) (*Manifest, []sstFileMeta, error) {
	live, err := replayManifest(path)
	if err != nil {
		return nil, nil, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}

	return &Manifest{f: f}, live, nil
}

func replayManifest(path string) ([]sstFileMeta, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	liveSet := make(map[sstFileMeta]bool)
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var op string
		var meta sstFileMeta
		n, err := fmt.Sscanf(line, "%s %d %d", &op, &meta.Gen, &meta.Seq)
		if err != nil || n != 3 {
			// A torn final line (crash mid-append) — stop here, same
			// tolerance policy as WAL replay.
			break
		}
		switch op {
		case "ADD":
			liveSet[meta] = true
		case "DEL":
			delete(liveSet, meta)
		}
	}

	live := make([]sstFileMeta, 0, len(liveSet))
	for meta := range liveSet {
		live = append(live, meta)
	}
	return live, nil
}

// AddTable durably records that meta is now a live SSTable file. Call this
// only after the file itself has been fully written (SSTableWriter.Finish
// returned nil).
func (m *Manifest) AddTable(meta sstFileMeta) error {
	return m.appendLine(fmt.Sprintf("ADD %d %d\n", meta.Gen, meta.Seq))
}

// RemoveTable durably records that meta is no longer live. Call this
// before deleting the underlying file, so a crash between the two leaves
// an orphaned-but-harmless file rather than a live record pointing at a
// deleted one.
func (m *Manifest) RemoveTable(meta sstFileMeta) error {
	return m.appendLine(fmt.Sprintf("DEL %d %d\n", meta.Gen, meta.Seq))
}

func (m *Manifest) appendLine(line string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.f.WriteString(line); err != nil {
		return err
	}
	return m.f.Sync()
}

func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.f.Close()
}
