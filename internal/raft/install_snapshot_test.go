package raft

import "testing"

func TestHandleInstallSnapshotAdoptsBoundaryAndDiscardsInconsistentLog(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
	n.log = []LogEntry{{Term: 1}, {Term: 1}}

	reply := n.HandleInstallSnapshot(InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "node-2",
		LastIncludedIndex: 5,
		LastIncludedTerm:  2,
	})

	if reply.Term != 2 {
		t.Errorf("reply term = %d, want 2", reply.Term)
	}
	if n.lastIncludedIndex != 5 || n.lastIncludedTerm != 2 {
		t.Fatalf("lastIncludedIndex/lastIncludedTerm = %d/%d, want 5/2", n.lastIncludedIndex, n.lastIncludedTerm)
	}
	if len(n.log) != 0 {
		t.Fatalf("log = %+v, want empty, since nothing from the old log is consistent with the new boundary", n.log)
	}
}

func TestHandleInstallSnapshotRetainsConsistentTrailingEntries(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
	n.log = []LogEntry{{Term: 1}, {Term: 2}, {Term: 2, Command: []byte("x")}}

	n.HandleInstallSnapshot(InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "node-2",
		LastIncludedIndex: 2,
		LastIncludedTerm:  2,
	})

	if len(n.log) != 1 || string(n.log[0].Command) != "x" {
		t.Fatalf("log = %+v, want one retained entry {Command:\"x\"}", n.log)
	}
	if n.lastIncludedIndex != 2 || n.lastIncludedTerm != 2 {
		t.Fatalf("lastIncludedIndex/lastIncludedTerm = %d/%d, want 2/2", n.lastIncludedIndex, n.lastIncludedTerm)
	}
}

func TestHandleInstallSnapshotIsNoOpWhenAlreadyAtOrAheadOfBoundary(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
	n.log = []LogEntry{{Term: 3, Command: []byte("kept")}}
	n.lastIncludedIndex = 5
	n.lastIncludedTerm = 2
	n.currentTerm = 3

	reply := n.HandleInstallSnapshot(InstallSnapshotArgs{
		Term:              3,
		LeaderID:          "node-2",
		LastIncludedIndex: 4,
		LastIncludedTerm:  2,
	})

	if reply.Term != 3 {
		t.Errorf("reply term = %d, want 3", reply.Term)
	}
	if n.lastIncludedIndex != 5 || len(n.log) != 1 || string(n.log[0].Command) != "kept" {
		t.Fatalf("state should be unchanged, since this node is already ahead of the offered boundary: lastIncludedIndex=%d log=%+v", n.lastIncludedIndex, n.log)
	}
}

func TestHandleInstallSnapshotIgnoresStaleTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
	n.currentTerm = 5
	n.lastIncludedIndex = 1
	n.lastIncludedTerm = 1

	reply := n.HandleInstallSnapshot(InstallSnapshotArgs{
		Term:              3,
		LeaderID:          "node-2",
		LastIncludedIndex: 10,
		LastIncludedTerm:  3,
	})

	if reply.Term != 5 {
		t.Errorf("reply term = %d, want 5", reply.Term)
	}
	if n.lastIncludedIndex != 1 {
		t.Fatalf("lastIncludedIndex = %d, want 1, unchanged, since a stale leader's snapshot must not be adopted", n.lastIncludedIndex)
	}
}

func TestHandleInstallSnapshotAdoptsHigherTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)

	n.HandleInstallSnapshot(InstallSnapshotArgs{
		Term:              7,
		LeaderID:          "node-2",
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
	})

	if got := n.Term(); got != 7 {
		t.Errorf("term = %d, want 7", got)
	}
	if got := n.Role(); got != Follower {
		t.Errorf("role = %v, want %v", got, Follower)
	}
}

func TestHandleInstallSnapshotAdvancesCommitIndex(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
	n.commitIndex = 1

	n.HandleInstallSnapshot(InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "node-2",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
	})

	if n.commitIndex != 5 {
		t.Errorf("commitIndex = %d, want 5", n.commitIndex)
	}
}

func TestHandleInstallSnapshotNeverDecreasesCommitIndex(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
	n.currentTerm = 1
	n.commitIndex = 10

	n.HandleInstallSnapshot(InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "node-2",
		LastIncludedIndex: 3,
		LastIncludedTerm:  1,
	})

	if n.commitIndex != 10 {
		t.Errorf("commitIndex = %d, want 10, unchanged, since it must never move backward", n.commitIndex)
	}
}

func TestHandleInstallSnapshotPersistsState(t *testing.T) {
	storage := &fakeStorage{}
	n := NewNode("node-1", []string{"node-2"}, nil, storage)

	n.HandleInstallSnapshot(InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "node-2",
		LastIncludedIndex: 3,
		LastIncludedTerm:  1,
	})

	storage.mu.Lock()
	defer storage.mu.Unlock()
	if len(storage.saved) == 0 {
		t.Fatalf("expected at least one Save call after installing a snapshot")
	}
	last := storage.saved[len(storage.saved)-1]
	if last.LastIncludedIndex != 3 || last.LastIncludedTerm != 1 {
		t.Fatalf("last persisted state = %+v, want LastIncludedIndex=3 LastIncludedTerm=1", last)
	}
}
