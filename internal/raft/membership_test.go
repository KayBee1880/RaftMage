package raft

import (
	"testing"
	"time"
)

func TestAddServerRejectsWhenNotLeader(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)

	index, term, ok := n.AddServer("node-3")

	if ok {
		t.Fatalf("expected ok = false for a follower")
	}
	if index != 0 || term != 0 {
		t.Errorf("index/term = %d/%d, want 0/0 on rejection", index, term)
	}
	if len(n.log) != 0 {
		t.Fatalf("log should be unchanged, got %+v", n.log)
	}
}

func TestAddServerRejectsExistingMember(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, &fakeTransport{}, nil)
	n.currentTerm = 1
	n.role = Leader
	n.initLeaderStateLocked()

	_, _, ok := n.AddServer("node-2")

	if ok {
		t.Fatalf("expected ok = false when adding an already-present member")
	}
	if len(n.log) != 0 {
		t.Fatalf("log should be unchanged, got %+v", n.log)
	}
}

func TestAddServerRejectsOwnID(t *testing.T) {
	n := NewNode("node-1", nil, &fakeTransport{}, nil)
	n.currentTerm = 1
	n.role = Leader
	n.initLeaderStateLocked()

	_, _, ok := n.AddServer("node-1")

	if ok {
		t.Fatalf("expected ok = false when adding the leader's own ID")
	}
}

func TestAddServerAppendsConfigEntryAndUpdatesCurrentConfig(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, &fakeTransport{}, nil)
	n.currentTerm = 1
	n.role = Leader
	n.initLeaderStateLocked()

	index, term, ok := n.AddServer("node-3")

	if !ok {
		t.Fatalf("expected ok = true")
	}
	if index != 1 || term != 1 {
		t.Errorf("index/term = %d/%d, want 1/1", index, term)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if len(n.log) != 1 || n.log[0].Type != EntryConfig {
		t.Fatalf("log = %+v, want one EntryConfig entry", n.log)
	}

	got := n.currentConfigLocked()
	want := map[string]bool{"node-2": true, "node-3": true}
	if len(got) != len(want) {
		t.Fatalf("currentConfigLocked() = %v, want %v", got, want)
	}
	for _, peer := range got {
		if !want[peer] {
			t.Fatalf("currentConfigLocked() = %v, unexpected member %q", got, peer)
		}
	}
	if _, ok := n.nextIndex["node-3"]; !ok {
		t.Fatalf("expected nextIndex to be initialized for newly added peer node-3")
	}
}

func TestAddServerRejectsWhilePendingChangeUncommitted(t *testing.T) {
	transport := &fakeTransport{errPeer: map[string]bool{"node-2": true, "node-3": true}}
	n := NewNode("node-1", []string{"node-2"}, transport, nil)
	n.currentTerm = 1
	n.role = Leader
	n.initLeaderStateLocked()

	if _, _, ok := n.AddServer("node-3"); !ok {
		t.Fatalf("expected the first AddServer to succeed")
	}

	_, _, ok := n.RemoveServer("node-2")
	if ok {
		t.Fatalf("expected RemoveServer to be rejected while a config change is still uncommitted")
	}
	if len(n.log) != 1 {
		t.Fatalf("log = %+v, want the second call to leave the log unchanged", n.log)
	}
}

func TestRemoveServerRejectsWhenNotLeader(t *testing.T) {
	n := NewNode("node-1", []string{"node-2", "node-3"}, nil, nil)

	index, term, ok := n.RemoveServer("node-3")

	if ok {
		t.Fatalf("expected ok = false for a follower")
	}
	if index != 0 || term != 0 {
		t.Errorf("index/term = %d/%d, want 0/0 on rejection", index, term)
	}
}

func TestRemoveServerRejectsNonMember(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, &fakeTransport{}, nil)
	n.currentTerm = 1
	n.role = Leader
	n.initLeaderStateLocked()

	_, _, ok := n.RemoveServer("node-9")

	if ok {
		t.Fatalf("expected ok = false when removing a peer that isn't a member")
	}
	if len(n.log) != 0 {
		t.Fatalf("log should be unchanged, got %+v", n.log)
	}
}

func TestRemoveServerAppendsConfigEntryAndUpdatesCurrentConfig(t *testing.T) {
	n := NewNode("node-1", []string{"node-2", "node-3"}, &fakeTransport{}, nil)
	n.currentTerm = 1
	n.role = Leader
	n.initLeaderStateLocked()

	index, term, ok := n.RemoveServer("node-3")

	if !ok {
		t.Fatalf("expected ok = true")
	}
	if index != 1 || term != 1 {
		t.Errorf("index/term = %d/%d, want 1/1", index, term)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	got := n.currentConfigLocked()
	if len(got) != 1 || got[0] != "node-2" {
		t.Fatalf("currentConfigLocked() = %v, want [node-2]", got)
	}
	if _, ok := n.nextIndex["node-3"]; ok {
		t.Fatalf("expected nextIndex to no longer track removed peer node-3")
	}
}

func TestConfigEntryAdoptedByFollowerImmediatelyOnAppend(t *testing.T) {
	n := NewNode("node-2", []string{"node-1"}, nil, nil)

	reply := n.HandleAppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "node-1",
		Entries: []LogEntry{
			{Term: 1, Type: EntryConfig, Config: []string{"node-1", "node-2", "node-3"}},
		},
	})

	if !reply.Success {
		t.Fatalf("expected the append to succeed, got %+v", reply)
	}

	got := n.currentConfigLocked()
	want := map[string]bool{"node-1": true, "node-3": true}
	if len(got) != len(want) {
		t.Fatalf("currentConfigLocked() = %v, want %v", got, want)
	}
	for _, peer := range got {
		if !want[peer] {
			t.Fatalf("currentConfigLocked() = %v, unexpected member %q", got, peer)
		}
	}
}

func TestConfigEntryRevertsOnTruncation(t *testing.T) {
	n := NewNode("node-2", []string{"node-1"}, nil, nil)

	n.HandleAppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "node-1",
		Entries: []LogEntry{
			{Term: 1, Type: EntryConfig, Config: []string{"node-1", "node-2", "node-3"}},
		},
	})
	if len(n.currentConfigLocked()) != 2 {
		t.Fatalf("expected the config entry to be adopted first, currentConfigLocked() = %v", n.currentConfigLocked())
	}

	reply := n.HandleAppendEntries(AppendEntriesArgs{
		Term:         2,
		LeaderID:     "node-4",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Term: 2, Command: []byte("unrelated write")},
		},
	})

	if !reply.Success {
		t.Fatalf("expected the append to succeed, got %+v", reply)
	}

	got := n.currentConfigLocked()
	if len(got) != 1 || got[0] != "node-1" {
		t.Fatalf("currentConfigLocked() = %v, want [node-1] (reverted to baseConfig once the config entry was truncated away)", got)
	}
}

func TestCompactSnapshotsConfigIntoBaseConfig(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
	n.log = []LogEntry{
		{Term: 1, Type: EntryConfig, Config: []string{"node-1", "node-2", "node-3"}},
		{Term: 1, Command: []byte("x")},
	}
	n.commitIndex = 2

	if err := n.Compact(2); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	got := n.currentConfigLocked()
	want := map[string]bool{"node-2": true, "node-3": true}
	if len(got) != len(want) {
		t.Fatalf("currentConfigLocked() after compaction = %v, want %v", got, want)
	}
	for _, peer := range got {
		if !want[peer] {
			t.Fatalf("currentConfigLocked() = %v, unexpected member %q", got, peer)
		}
	}
}

func TestNewNodePersistsAndReloadsBaseConfigAfterCompaction(t *testing.T) {
	storage := &fakeStorage{}
	before := NewNode("node-1", []string{"node-2"}, nil, storage)
	before.log = []LogEntry{
		{Term: 1, Type: EntryConfig, Config: []string{"node-1", "node-2", "node-3"}},
	}
	before.commitIndex = 1

	if err := before.Compact(1); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	after := NewNode("node-1", []string{"node-2"}, nil, storage)

	got := after.currentConfigLocked()
	want := map[string]bool{"node-2": true, "node-3": true}
	if len(got) != len(want) {
		t.Fatalf("currentConfigLocked() after restart = %v, want %v", got, want)
	}
	for _, peer := range got {
		if !want[peer] {
			t.Fatalf("currentConfigLocked() = %v, unexpected member %q", got, peer)
		}
	}
}

func TestInstallSnapshotAdoptsConfig(t *testing.T) {
	n := NewNode("node-2", []string{"node-1"}, nil, nil)

	n.HandleInstallSnapshot(InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "node-1",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Config:            []string{"node-1", "node-2", "node-4"},
	})

	got := n.currentConfigLocked()
	want := map[string]bool{"node-1": true, "node-4": true}
	if len(got) != len(want) {
		t.Fatalf("currentConfigLocked() after InstallSnapshot = %v, want %v", got, want)
	}
	for _, peer := range got {
		if !want[peer] {
			t.Fatalf("currentConfigLocked() = %v, unexpected member %q", got, peer)
		}
	}
}

func TestLeaderStepsDownAfterCommittingSelfRemoval(t *testing.T) {
	transport := &fakeTransport{}
	n := NewNode("node-1", []string{"node-2"}, transport, nil)
	n.currentTerm = 1
	n.role = Leader
	n.initLeaderStateLocked()

	index, _, ok := n.RemoveServer("node-1")
	if !ok {
		t.Fatalf("expected RemoveServer(self) to succeed")
	}

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n.Role() == Follower && n.CommitIndex() == index {
			if got := n.CurrentLeader(); got != "" {
				t.Fatalf("CurrentLeader() = %q, want empty after stepping down from self-removal", got)
			}
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("expected the leader to commit its own removal and step down, role = %v, commitIndex = %d", n.Role(), n.CommitIndex())
}
