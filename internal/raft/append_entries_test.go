package raft

import "testing"

func TestHandleAppendEntriesRejectsStaleTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)
	n.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node-2"})

	reply := n.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: "node-3"})

	if reply.Success {
		t.Fatalf("expected AppendEntries rejected for stale term")
	}
	if reply.Term != 5 {
		t.Errorf("reply term = %d, want 5", reply.Term)
	}
}

func TestHandleAppendEntriesAcceptsCurrentTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)

	reply := n.HandleAppendEntries(AppendEntriesArgs{Term: 0, LeaderID: "node-2"})

	if !reply.Success {
		t.Fatalf("expected AppendEntries accepted")
	}
}

func TestHandleAppendEntriesAdoptsHigherTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)

	reply := n.HandleAppendEntries(AppendEntriesArgs{Term: 7, LeaderID: "node-2"})

	if !reply.Success {
		t.Fatalf("expected AppendEntries accepted")
	}
	if got := n.Term(); got != 7 {
		t.Errorf("term = %d, want 7", got)
	}
	if got := n.Role(); got != Follower {
		t.Errorf("role = %v, want %v", got, Follower)
	}
}

func TestHandleAppendEntriesCandidateStepsDownOnEqualTerm(t *testing.T) {
	transport := &fakeTransport{
		replies: map[string]RequestVoteReply{
			"node-2": {Term: 1, VoteGranted: false},
			"node-3": {Term: 1, VoteGranted: false},
		},
	}
	n := NewNode("node-1", []string{"node-2", "node-3"}, transport)
	n.StartElection()

	if got := n.Role(); got != Candidate {
		t.Fatalf("precondition failed: role = %v, want %v", got, Candidate)
	}

	reply := n.HandleAppendEntries(AppendEntriesArgs{Term: n.Term(), LeaderID: "node-2"})

	if !reply.Success {
		t.Fatalf("expected AppendEntries accepted")
	}
	if got := n.Role(); got != Follower {
		t.Fatalf("role = %v after AppendEntries at same term, want %v", got, Follower)
	}
}

func TestHandleAppendEntriesAppendsToEmptyLog(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)

	reply := n.HandleAppendEntries(AppendEntriesArgs{
		Term:    1,
		Entries: []LogEntry{{Term: 1, Command: []byte("a")}, {Term: 1, Command: []byte("b")}},
	})

	if !reply.Success {
		t.Fatalf("expected success")
	}
	if got := len(n.log); got != 2 {
		t.Fatalf("log length = %d, want 2", got)
	}
	if string(n.log[0].Command) != "a" || string(n.log[1].Command) != "b" {
		t.Fatalf("log contents = %+v, want [a b]", n.log)
	}
}

func TestHandleAppendEntriesRejectsPrevLogIndexBeyondLog(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)

	reply := n.HandleAppendEntries(AppendEntriesArgs{
		Term:         1,
		PrevLogIndex: 5,
		PrevLogTerm:  1,
	})

	if reply.Success {
		t.Fatalf("expected rejection: PrevLogIndex beyond local log")
	}
	if len(n.log) != 0 {
		t.Fatalf("log should be unchanged, got %+v", n.log)
	}
}

func TestHandleAppendEntriesRejectsPrevLogTermMismatch(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)
	n.log = []LogEntry{{Term: 1}}

	reply := n.HandleAppendEntries(AppendEntriesArgs{
		Term:         2,
		PrevLogIndex: 1,
		PrevLogTerm:  2,
	})

	if reply.Success {
		t.Fatalf("expected rejection: PrevLogTerm mismatch")
	}
}

func TestHandleAppendEntriesTruncatesConflictingEntries(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)
	n.log = []LogEntry{{Term: 1}, {Term: 1}, {Term: 1}}

	reply := n.HandleAppendEntries(AppendEntriesArgs{
		Term:         2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 2, Command: []byte("x")}},
	})

	if !reply.Success {
		t.Fatalf("expected success")
	}
	if got := len(n.log); got != 2 {
		t.Fatalf("log length = %d, want 2", got)
	}
	if n.log[1].Term != 2 || string(n.log[1].Command) != "x" {
		t.Fatalf("log[1] = %+v, want Term=2 Command=x", n.log[1])
	}
}

func TestHandleAppendEntriesIgnoresAlreadyMatchingEntries(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)
	entries := []LogEntry{{Term: 1, Command: []byte("a")}, {Term: 1, Command: []byte("b")}}
	n.HandleAppendEntries(AppendEntriesArgs{Term: 1, Entries: entries})

	reply := n.HandleAppendEntries(AppendEntriesArgs{Term: 1, Entries: entries})

	if !reply.Success {
		t.Fatalf("expected success")
	}
	if got := len(n.log); got != 2 {
		t.Fatalf("log length = %d after duplicate AppendEntries, want 2", got)
	}
}

func TestHandleAppendEntriesAdvancesCommitIndex(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)
	n.HandleAppendEntries(AppendEntriesArgs{
		Term:    1,
		Entries: []LogEntry{{Term: 1}, {Term: 1}, {Term: 1}},
	})

	reply := n.HandleAppendEntries(AppendEntriesArgs{
		Term:         1,
		PrevLogIndex: 3,
		PrevLogTerm:  1,
		LeaderCommit: 2,
	})

	if !reply.Success {
		t.Fatalf("expected success")
	}
	if n.commitIndex != 2 {
		t.Fatalf("commitIndex = %d, want 2", n.commitIndex)
	}
}

func TestHandleAppendEntriesCommitIndexBoundedByLocalLog(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)

	reply := n.HandleAppendEntries(AppendEntriesArgs{
		Term:         1,
		Entries:      []LogEntry{{Term: 1}, {Term: 1}},
		LeaderCommit: 10,
	})

	if !reply.Success {
		t.Fatalf("expected success")
	}
	if n.commitIndex != 2 {
		t.Fatalf("commitIndex = %d, want 2 (bounded by local log length)", n.commitIndex)
	}
}
