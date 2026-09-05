package raft

import "testing"

func TestCompactTrimsLogAndRecordsBoundary(t *testing.T) {
	n := NewNode("node-1", nil, nil, nil)
	n.log = []LogEntry{{Term: 1, Command: []byte("a")}, {Term: 1, Command: []byte("b")}, {Term: 2, Command: []byte("c")}}
	n.commitIndex = 3

	if err := n.Compact(2); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	if n.lastIncludedIndex != 2 {
		t.Errorf("lastIncludedIndex = %d, want 2", n.lastIncludedIndex)
	}
	if n.lastIncludedTerm != 1 {
		t.Errorf("lastIncludedTerm = %d, want 1", n.lastIncludedTerm)
	}
	if len(n.log) != 1 || string(n.log[0].Command) != "c" {
		t.Fatalf("log = %+v, want one entry {Term:2 Command:\"c\"}", n.log)
	}
	if got := n.lastLogIndexLocked(); got != 3 {
		t.Errorf("lastLogIndexLocked() = %d, want 3 (full logical history preserved)", got)
	}
	if got := n.logTermAtLocked(2); got != 1 {
		t.Errorf("logTermAtLocked(2) = %d, want 1 (compaction boundary term)", got)
	}
}

func TestCompactRejectsUncommittedIndex(t *testing.T) {
	n := NewNode("node-1", nil, nil, nil)
	n.log = []LogEntry{{Term: 1}, {Term: 1}, {Term: 1}}
	n.commitIndex = 1

	err := n.Compact(2)

	if err != ErrCompactPastCommitIndex {
		t.Fatalf("err = %v, want ErrCompactPastCommitIndex", err)
	}
	if n.lastIncludedIndex != 0 || len(n.log) != 3 {
		t.Fatalf("state should be unchanged: lastIncludedIndex=%d log=%+v", n.lastIncludedIndex, n.log)
	}
}

func TestCompactRejectsPastFollowerMatchIndexOnLeader(t *testing.T) {
	n := NewNode("node-1", []string{"node-2", "node-3"}, nil, nil)
	n.log = []LogEntry{{Term: 1}, {Term: 1}, {Term: 1}}
	n.currentTerm = 1
	n.role = Leader
	n.commitIndex = 3
	n.matchIndex = map[string]uint64{"node-2": 3, "node-3": 1}

	err := n.Compact(2)

	if err != ErrCompactPastFollowerMatchIndex {
		t.Fatalf("err = %v, want ErrCompactPastFollowerMatchIndex", err)
	}
}

func TestCompactAllowsUpToMinMatchIndexOnLeader(t *testing.T) {
	n := NewNode("node-1", []string{"node-2", "node-3"}, nil, nil)
	n.log = []LogEntry{{Term: 1}, {Term: 1}, {Term: 1}}
	n.currentTerm = 1
	n.role = Leader
	n.commitIndex = 3
	n.matchIndex = map[string]uint64{"node-2": 3, "node-3": 2}

	if err := n.Compact(2); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}
	if n.lastIncludedIndex != 2 {
		t.Errorf("lastIncludedIndex = %d, want 2", n.lastIncludedIndex)
	}
}

func TestCompactIsNoOpWhenAlreadyPastUpToIndex(t *testing.T) {
	n := NewNode("node-1", nil, nil, nil)
	n.log = []LogEntry{{Term: 1}, {Term: 2}}
	n.commitIndex = 2
	if err := n.Compact(2); err != nil {
		t.Fatalf("first Compact failed: %v", err)
	}

	if err := n.Compact(1); err != nil {
		t.Fatalf("second (no-op) Compact failed: %v", err)
	}
	if n.lastIncludedIndex != 2 {
		t.Errorf("lastIncludedIndex = %d, want 2 (unchanged by no-op compact)", n.lastIncludedIndex)
	}
	if len(n.log) != 0 {
		t.Errorf("log = %+v, want empty", n.log)
	}
}

func TestCompactedStatePersistsAndRecoversAcrossRestart(t *testing.T) {
	storage := &fakeStorage{}
	before := NewNode("node-1", nil, nil, storage)
	before.log = []LogEntry{{Term: 1, Command: []byte("a")}, {Term: 2, Command: []byte("b")}}
	before.commitIndex = 2

	if err := before.Compact(1); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	after := NewNode("node-1", nil, nil, storage)

	if after.lastIncludedIndex != 1 {
		t.Errorf("lastIncludedIndex after restart = %d, want 1", after.lastIncludedIndex)
	}
	if after.lastIncludedTerm != 1 {
		t.Errorf("lastIncludedTerm after restart = %d, want 1", after.lastIncludedTerm)
	}
	if len(after.log) != 1 || string(after.log[0].Command) != "b" {
		t.Fatalf("log after restart = %+v, want one entry {Term:2 Command:\"b\"}", after.log)
	}
	if got := after.lastLogIndexLocked(); got != 2 {
		t.Errorf("lastLogIndexLocked() after restart = %d, want 2", got)
	}
}

func TestReplicatePeerSendsCorrectEntriesFromCompactedLog(t *testing.T) {
	transport := &fakeTransport{}
	n := NewNode("node-1", []string{"node-2"}, transport, nil)
	n.log = []LogEntry{{Term: 1, Command: []byte("a")}, {Term: 1, Command: []byte("b")}, {Term: 1, Command: []byte("c")}}
	n.currentTerm = 1
	n.role = Leader
	n.commitIndex = 3
	n.initLeaderStateLocked()
	n.matchIndex["node-2"] = 3
	if err := n.Compact(2); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}
	n.nextIndex["node-2"] = 3

	n.replicatePeer(1, "node-2")

	if len(transport.appendEntriesLog) != 1 {
		t.Fatalf("expected 1 AppendEntries call, got %d", len(transport.appendEntriesLog))
	}
	sent := transport.appendEntriesLog[0]
	if sent.PrevLogIndex != 2 || sent.PrevLogTerm != 1 {
		t.Errorf("PrevLogIndex/PrevLogTerm = %d/%d, want 2/1", sent.PrevLogIndex, sent.PrevLogTerm)
	}
	if len(sent.Entries) != 1 || string(sent.Entries[0].Command) != "c" {
		t.Fatalf("sent entries = %+v, want one entry {Command:\"c\"}", sent.Entries)
	}
}
