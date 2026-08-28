package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// testDir returns a directory under the project's (gitignored) tmp/ for
// t to write real files into — unlike t.TempDir(), nothing here is
// cleaned up when the test finishes, so WAL/SSTable/MANIFEST files are
// left on disk to inspect afterward (e.g. `find tmp/storage -type f`).
//
// It IS wiped at the start of the test (not the end), so re-running the
// same test doesn't trip over a previous run's leftover state.
func testDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join("..", "tmp", "storage", filepath.FromSlash(t.Name()))
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("RemoveAll(%s): %v", dir, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", dir, err)
	}
	return dir
}
