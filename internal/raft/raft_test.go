package raft

import "testing"

func TestNewNodeStartsAsFollower(t *testing.T) {
	n := NewNode("node-1", []string{"node-2", "node-3"}, nil, nil)

	if got := n.Role(); got != Follower {
		t.Errorf("new node role = %v, want %v", got, Follower)
	}
	if got := n.Term(); got != 0 {
		t.Errorf("new node term = %d, want 0", got)
	}
}

func TestNewNodeLoadsExistingPersistedState(t *testing.T) {
	storage := &fakeStorage{saved: []PersistentState{{
		CurrentTerm: 5,
		VotedFor:    "node-2",
		Log:         []LogEntry{{Term: 3, Command: []byte("x")}},
	}}}

	n := NewNode("node-1", []string{"node-2"}, nil, storage)

	if got := n.Term(); got != 5 {
		t.Errorf("term = %d, want 5", got)
	}
	if got := n.VotedFor(); got != "node-2" {
		t.Errorf("votedFor = %q, want %q", got, "node-2")
	}
	if len(n.log) != 1 || string(n.log[0].Command) != "x" {
		t.Fatalf("log = %+v, want one entry {Term:3 Command:\"x\"}", n.log)
	}
}

func TestNewNodePanicsOnStorageLoadError(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected NewNode to panic when Storage.Load fails")
		}
	}()

	storage := &fakeStorage{loadErr: errSimulatedStorageFailure}
	NewNode("node-1", nil, nil, storage)
}

func TestNodePersistsVoteBeforeGrantingIt(t *testing.T) {
	storage := &fakeStorage{}
	n := NewNode("node-1", []string{"node-2"}, nil, storage)

	n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node-2"})

	storage.mu.Lock()
	defer storage.mu.Unlock()
	if len(storage.saved) == 0 {
		t.Fatalf("expected at least one Save call after granting a vote")
	}
	last := storage.saved[len(storage.saved)-1]
	if last.CurrentTerm != 1 || last.VotedFor != "node-2" {
		t.Fatalf("last persisted state = %+v, want {CurrentTerm:1 VotedFor:node-2}", last)
	}
}

func TestNodePersistsAppendedEntriesBeforeAcceptingThem(t *testing.T) {
	storage := &fakeStorage{}
	n := NewNode("node-1", []string{"node-2"}, nil, storage)

	n.HandleAppendEntries(AppendEntriesArgs{
		Term:    1,
		Entries: []LogEntry{{Term: 1, Command: []byte("a")}},
	})

	storage.mu.Lock()
	defer storage.mu.Unlock()
	if len(storage.saved) == 0 {
		t.Fatalf("expected at least one Save call after appending entries")
	}
	last := storage.saved[len(storage.saved)-1]
	if len(last.Log) != 1 || string(last.Log[0].Command) != "a" {
		t.Fatalf("last persisted log = %+v, want one entry {Term:1 Command:\"a\"}", last.Log)
	}
}

func TestPersistStateLockedPanicsOnStorageSaveError(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic when Storage.Save fails")
		}
	}()

	storage := &fakeStorage{saveErr: errSimulatedStorageFailure}
	n := NewNode("node-1", []string{"node-2"}, nil, storage)

	n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node-2"})
}

func TestNodeSurvivesSimulatedRestartViaSharedStorage(t *testing.T) {
	storage := &fakeStorage{}

	before := NewNode("node-1", []string{"node-2", "node-3"}, nil, storage)
	before.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node-2"})
	before.HandleAppendEntries(AppendEntriesArgs{
		Term:    1,
		Entries: []LogEntry{{Term: 1, Command: []byte("set x=1")}},
	})

	after := NewNode("node-1", []string{"node-2", "node-3"}, nil, storage)

	if got := after.Term(); got != 1 {
		t.Errorf("term after restart = %d, want 1", got)
	}
	if got := after.VotedFor(); got != "node-2" {
		t.Errorf("votedFor after restart = %q, want %q", got, "node-2")
	}
	if len(after.log) != 1 || string(after.log[0].Command) != "set x=1" {
		t.Fatalf("log after restart = %+v, want one entry {Term:1 Command:\"set x=1\"}", after.log)
	}
}
