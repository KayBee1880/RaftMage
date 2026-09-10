package raft

import (
	"errors"
	"sync"
	"testing"
)

type fakeStateMachine struct {
	mu       sync.Mutex
	applied  [][]byte
	restored [][]byte
	err      error
	snapshot []byte
}

func (f *fakeStateMachine) Apply(command []byte) error {
	if f.err != nil {
		return f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, command)
	return nil
}

func (f *fakeStateMachine) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapshot, nil
}

func (f *fakeStateMachine) Restore(data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restored = append(f.restored, data)
	return nil
}

func TestNodeWithNoStateMachineDoesNotPanic(t *testing.T) {
	n := NewNode("node-1", nil, nil, nil)
	n.StartElection()

	if _, _, ok := n.Propose([]byte("x")); !ok {
		t.Fatalf("expected Propose to succeed")
	}
}

func TestApplyCommittedLockedAppliesEntriesInOrder(t *testing.T) {
	sm := &fakeStateMachine{}
	n := NewNode("node-1", nil, nil, nil)
	n.SetStateMachine(sm)
	n.StartElection()

	n.Propose([]byte("a"))
	n.Propose([]byte("b"))
	n.Propose([]byte("c"))

	if len(sm.applied) != 3 {
		t.Fatalf("applied %d commands, want 3", len(sm.applied))
	}
	for i, want := range []string{"a", "b", "c"} {
		if string(sm.applied[i]) != want {
			t.Errorf("applied[%d] = %q, want %q", i, sm.applied[i], want)
		}
	}
}

func TestApplyCommittedLockedSkipsConfigEntries(t *testing.T) {
	sm := &fakeStateMachine{}
	n := NewNode("node-1", nil, nil, nil)
	n.SetStateMachine(sm)
	n.log = []LogEntry{
		{Term: 1, Type: EntryConfig, Config: []string{"node-1"}},
		{Term: 1, Command: []byte("a")},
	}
	n.commitIndex = 2

	n.applyCommittedLocked()

	if len(sm.applied) != 1 || string(sm.applied[0]) != "a" {
		t.Fatalf("applied = %v, want exactly [\"a\"]", sm.applied)
	}
}

func TestApplyCommittedLockedPanicsOnApplyError(t *testing.T) {
	sm := &fakeStateMachine{err: errors.New("boom")}
	n := NewNode("node-1", nil, nil, nil)
	n.SetStateMachine(sm)
	n.log = []LogEntry{{Term: 1, Command: []byte("a")}}
	n.commitIndex = 1

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected applyCommittedLocked to panic on Apply error")
		}
	}()
	n.applyCommittedLocked()
}

func TestLastAppliedTracksCommitIndexForCommandEntries(t *testing.T) {
	sm := &fakeStateMachine{}
	n := NewNode("node-1", nil, nil, nil)
	n.SetStateMachine(sm)
	n.StartElection()

	n.Propose([]byte("a"))

	if n.LastApplied() != n.CommitIndex() {
		t.Fatalf("LastApplied() = %d, CommitIndex() = %d, want equal", n.LastApplied(), n.CommitIndex())
	}
}

func TestApplyCommittedLockedAdvancesLastAppliedPastGapAfterInstallSnapshot(t *testing.T) {
	sm := &fakeStateMachine{}
	n := NewNode("node-2", []string{"node-1"}, nil, nil)
	n.SetStateMachine(sm)

	n.HandleInstallSnapshot(InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "node-1",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Config:            []string{"node-1", "node-2"},
	})

	if n.LastApplied() != 5 {
		t.Fatalf("LastApplied() = %d, want 5", n.LastApplied())
	}
	if len(sm.applied) != 0 {
		t.Fatalf("expected no entries applied (they were never in this node's log), got %v", sm.applied)
	}
	if len(sm.restored) != 1 {
		t.Fatalf("expected exactly one Restore call, got %d", len(sm.restored))
	}
}

func TestApplyCommittedLockedAdvancesLastAppliedPastGapAfterCompactWithoutStateMachine(t *testing.T) {
	n := NewNode("node-1", nil, nil, nil)
	n.log = []LogEntry{{Term: 1, Command: []byte("a")}, {Term: 1, Command: []byte("b")}}
	n.commitIndex = 2

	if err := n.Compact(2); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	sm := &fakeStateMachine{}
	n.SetStateMachine(sm)
	n.log = append(n.log, LogEntry{Term: 1, Command: []byte("c")})
	n.commitIndex = 3
	n.applyCommittedLocked()

	if len(sm.applied) != 1 || string(sm.applied[0]) != "c" {
		t.Fatalf("applied = %v, want exactly [\"c\"] (a and b were compacted before any state machine was attached, permanently skipped)", sm.applied)
	}
	if n.LastApplied() != 3 {
		t.Fatalf("LastApplied() = %d, want 3", n.LastApplied())
	}
	if len(sm.restored) != 0 {
		t.Fatalf("expected no Restore call (nothing was ever snapshotted while unattached), got %d", len(sm.restored))
	}
}

func TestCompactCapturesStateMachineSnapshotWhenAttached(t *testing.T) {
	sm := &fakeStateMachine{snapshot: []byte(`{"x":"1"}`)}
	n := NewNode("node-1", nil, nil, nil)
	n.SetStateMachine(sm)
	n.log = []LogEntry{{Term: 1, Command: []byte("a")}}
	n.commitIndex = 1
	n.applyCommittedLocked()

	if err := n.Compact(1); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	n.mu.Lock()
	got := n.stateMachineSnapshot
	n.mu.Unlock()
	if string(got) != `{"x":"1"}` {
		t.Fatalf("stateMachineSnapshot = %s, want the state machine's own Snapshot() output", got)
	}
}

func TestHandleInstallSnapshotRestoresStateMachineFromPayload(t *testing.T) {
	sm := &fakeStateMachine{}
	n := NewNode("node-2", []string{"node-1"}, nil, nil)
	n.SetStateMachine(sm)

	payload := []byte(`{"x":"1"}`)
	n.HandleInstallSnapshot(InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "node-1",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Config:            []string{"node-1", "node-2"},
		Data:              payload,
	})

	if len(sm.restored) != 1 || string(sm.restored[0]) != string(payload) {
		t.Fatalf("restored = %v, want exactly one call with %s", sm.restored, payload)
	}
}

func TestStateMachineSnapshotPersistsAndReloadsAcrossRestart(t *testing.T) {
	storage := &fakeStorage{}
	before := NewNode("node-1", nil, nil, storage)
	beforeSM := &fakeStateMachine{snapshot: []byte(`{"x":"1"}`)}
	before.SetStateMachine(beforeSM)
	before.log = []LogEntry{{Term: 1, Command: []byte("a")}}
	before.commitIndex = 1
	before.applyCommittedLocked()

	if err := before.Compact(1); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	after := NewNode("node-1", nil, nil, storage)
	afterSM := &fakeStateMachine{}
	after.SetStateMachine(afterSM)

	if len(afterSM.restored) != 1 || string(afterSM.restored[0]) != `{"x":"1"}` {
		t.Fatalf("restored = %v, want exactly one call with the persisted snapshot", afterSM.restored)
	}
	if after.LastApplied() != 1 {
		t.Fatalf("LastApplied() after restart = %d, want 1", after.LastApplied())
	}
}
