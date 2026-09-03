package storage

import (
	"os"
	"path/filepath"
	"testing"

	"raftmage/internal/raft"
)

func TestFileStorageSaveThenLoadRoundTrips(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStorage(filepath.Join(dir, "state.json"))

	want := raft.PersistentState{
		CurrentTerm: 7,
		VotedFor:    "node-2",
		Log:         []raft.LogEntry{{Term: 1, Command: []byte("a")}, {Term: 2, Command: []byte("b")}},
	}
	if err := fs.Save(want); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	got, err := fs.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if got.CurrentTerm != want.CurrentTerm || got.VotedFor != want.VotedFor {
		t.Fatalf("loaded state = %+v, want %+v", got, want)
	}
	if len(got.Log) != 2 || string(got.Log[0].Command) != "a" || string(got.Log[1].Command) != "b" {
		t.Fatalf("loaded log = %+v, want %+v", got.Log, want.Log)
	}
}

func TestFileStorageLoadOnMissingFileReturnsZeroValue(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStorage(filepath.Join(dir, "does-not-exist.json"))

	got, err := fs.Load()
	if err != nil {
		t.Fatalf("Load on a missing file should not error, got: %v", err)
	}
	if got.CurrentTerm != 0 || got.VotedFor != "" || len(got.Log) != 0 {
		t.Fatalf("loaded state = %+v, want the zero value", got)
	}
}

func TestFileStorageSaveOverwritesPreviousState(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStorage(filepath.Join(dir, "state.json"))

	if err := fs.Save(raft.PersistentState{CurrentTerm: 1, VotedFor: "node-2"}); err != nil {
		t.Fatalf("first Save failed: %v", err)
	}
	if err := fs.Save(raft.PersistentState{CurrentTerm: 2, VotedFor: "node-3"}); err != nil {
		t.Fatalf("second Save failed: %v", err)
	}

	got, err := fs.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if got.CurrentTerm != 2 || got.VotedFor != "node-3" {
		t.Fatalf("loaded state = %+v, want the second, most recent save", got)
	}
}

func TestFileStorageSaveLeavesNoTempFileBehind(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	fs := NewFileStorage(path)

	if err := fs.Save(raft.PersistentState{CurrentTerm: 1}); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("expected the .tmp file to be gone after a successful Save (renamed into place), stat err = %v", err)
	}
}
