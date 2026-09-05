package raft

import "testing"

func TestHandleAppendEntriesRejectsStaleTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
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
	n := NewNode("node-1", []string{"node-2"}, nil, nil)

	reply := n.HandleAppendEntries(AppendEntriesArgs{Term: 0, LeaderID: "node-2"})

	if !reply.Success {
		t.Fatalf("expected AppendEntries accepted")
	}
}

func TestHandleAppendEntriesAdoptsHigherTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)

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
	n := NewNode("node-1", []string{"node-2", "node-3"}, transport, nil)
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
	n := NewNode("node-1", []string{"node-2"}, nil, nil)

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
	n := NewNode("node-1", []string{"node-2"}, nil, nil)

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
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
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
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
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
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
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
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
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
	n := NewNode("node-1", []string{"node-2"}, nil, nil)

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

func TestHandleAppendEntriesAcceptsPrevLogIndexAtCompactionBoundary(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
	n.log = []LogEntry{{Term: 1}}
	n.commitIndex = 1
	if err := n.Compact(1); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	reply := n.HandleAppendEntries(AppendEntriesArgs{
		Term:         1,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 1, Command: []byte("c")}},
	})

	if !reply.Success {
		t.Fatalf("expected success: PrevLogIndex/PrevLogTerm exactly match the compaction boundary")
	}
	if len(n.log) != 1 || string(n.log[0].Command) != "c" {
		t.Fatalf("log = %+v, want one entry {Term:1 Command:\"c\"}", n.log)
	}
}

func TestHandleAppendEntriesRejectsPrevLogIndexBeforeCompactionBoundary(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
	n.log = []LogEntry{{Term: 1}, {Term: 2}}
	n.commitIndex = 2
	if err := n.Compact(2); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	reply := n.HandleAppendEntries(AppendEntriesArgs{
		Term:         2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
	})

	if reply.Success {
		t.Fatalf("expected rejection: PrevLogIndex is before the compaction boundary and can't be verified")
	}
}

func TestInitLeaderStateLockedStartsNextIndexAtOwnLogEnd(t *testing.T) {
	n := NewNode("node-1", []string{"node-2", "node-3"}, nil, nil)
	n.log = []LogEntry{{Term: 1}, {Term: 1}}

	n.initLeaderStateLocked()

	for _, peer := range []string{"node-2", "node-3"} {
		if got := n.nextIndex[peer]; got != 3 {
			t.Errorf("nextIndex[%s] = %d, want 3 (lastLogIndex+1)", peer, got)
		}
		if got := n.matchIndex[peer]; got != 0 {
			t.Errorf("matchIndex[%s] = %d, want 0", peer, got)
		}
	}
}

func TestStartElectionInitializesLeaderState(t *testing.T) {
	n := NewNode("node-1", nil, nil, nil)

	n.StartElection()

	if n.nextIndex == nil || n.matchIndex == nil {
		t.Fatalf("expected nextIndex/matchIndex to be initialized on becoming leader")
	}
}

func TestReplicatePeerSendsEntriesAndAdvancesIndexesOnSuccess(t *testing.T) {
	transport := &fakeTransport{}
	n := NewNode("node-1", []string{"node-2"}, transport, nil)
	n.currentTerm = 1
	n.role = Leader
	n.initLeaderStateLocked()
	n.log = []LogEntry{{Term: 1, Command: []byte("a")}, {Term: 1, Command: []byte("b")}}

	n.replicatePeer(1, "node-2")

	if got := n.matchIndex["node-2"]; got != 2 {
		t.Errorf("matchIndex = %d, want 2", got)
	}
	if got := n.nextIndex["node-2"]; got != 3 {
		t.Errorf("nextIndex = %d, want 3", got)
	}
	if len(transport.appendEntriesLog) != 1 {
		t.Fatalf("expected 1 AppendEntries call, got %d", len(transport.appendEntriesLog))
	}
	sent := transport.appendEntriesLog[0]
	if len(sent.Entries) != 2 || string(sent.Entries[0].Command) != "a" || string(sent.Entries[1].Command) != "b" {
		t.Errorf("sent entries = %+v, want [a b]", sent.Entries)
	}
	if sent.PrevLogIndex != 0 {
		t.Errorf("PrevLogIndex = %d, want 0 (fresh leader, nothing before the first entry)", sent.PrevLogIndex)
	}
}

func TestReplicatePeerRetriesWithEarlierPrevLogIndexAfterRejection(t *testing.T) {
	var calls int
	transport := &fakeTransport{
		appendEntriesFn: func(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
			calls++
			if calls < 3 {
				return AppendEntriesReply{Term: args.Term, Success: false}, nil
			}
			return AppendEntriesReply{Term: args.Term, Success: true}, nil
		},
	}
	n := NewNode("node-1", []string{"node-2"}, transport, nil)
	n.log = []LogEntry{{Term: 1}, {Term: 1}, {Term: 1}}
	n.currentTerm = 1
	n.role = Leader
	n.initLeaderStateLocked()

	n.replicatePeer(1, "node-2")

	if calls != 3 {
		t.Fatalf("expected 3 attempts before success, got %d", calls)
	}
	if got := n.nextIndex["node-2"]; got != 4 {
		t.Errorf("nextIndex = %d, want 4 (caught up to end of log)", got)
	}
	if got := n.matchIndex["node-2"]; got != 3 {
		t.Errorf("matchIndex = %d, want 3", got)
	}
	wantPrevLogIndex := []uint64{3, 2, 1}
	for i, args := range transport.appendEntriesLog {
		if args.PrevLogIndex != wantPrevLogIndex[i] {
			t.Errorf("attempt %d: PrevLogIndex = %d, want %d", i, args.PrevLogIndex, wantPrevLogIndex[i])
		}
	}
}

func TestReplicatePeerStepsDownOnHigherTerm(t *testing.T) {
	transport := &fakeTransport{
		appendReplies: map[string]AppendEntriesReply{
			"node-2": {Term: 5, Success: false},
		},
	}
	n := NewNode("node-1", []string{"node-2"}, transport, nil)
	n.currentTerm = 1
	n.role = Leader
	n.initLeaderStateLocked()

	n.replicatePeer(1, "node-2")

	if got := n.Role(); got != Follower {
		t.Fatalf("role = %v, want %v", got, Follower)
	}
	if got := n.Term(); got != 5 {
		t.Errorf("term = %d, want 5", got)
	}
}

func TestAdvanceCommitIndexLockedCommitsOnMajorityAtCurrentTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2", "node-3", "node-4"}, nil, nil)
	n.log = []LogEntry{{Term: 1}, {Term: 2}, {Term: 2}}
	n.currentTerm = 2
	n.role = Leader
	n.matchIndex = map[string]uint64{"node-2": 3, "node-3": 3, "node-4": 0}

	n.advanceCommitIndexLocked(2)

	if n.commitIndex != 3 {
		t.Fatalf("commitIndex = %d, want 3 (leader + 2 of 3 peers is a majority of 4)", n.commitIndex)
	}
}

func TestAdvanceCommitIndexLockedWithoutMajorityDoesNotCommit(t *testing.T) {
	n := NewNode("node-1", []string{"node-2", "node-3", "node-4"}, nil, nil)
	n.log = []LogEntry{{Term: 1}, {Term: 2}, {Term: 2}}
	n.currentTerm = 2
	n.role = Leader
	n.matchIndex = map[string]uint64{"node-2": 3, "node-3": 0, "node-4": 0}

	n.advanceCommitIndexLocked(2)

	if n.commitIndex != 0 {
		t.Fatalf("commitIndex = %d, want 0 (only leader + 1 of 3 peers, not a majority of 4)", n.commitIndex)
	}
}

func TestAdvanceCommitIndexLockedNeverCommitsAnEarlierTermEntryDirectly(t *testing.T) {
	n := NewNode("node-1", []string{"node-2", "node-3"}, nil, nil)
	n.log = []LogEntry{{Term: 1}, {Term: 1}}
	n.currentTerm = 2
	n.role = Leader
	n.matchIndex = map[string]uint64{"node-2": 2, "node-3": 2}

	n.advanceCommitIndexLocked(2)

	if n.commitIndex != 0 {
		t.Fatalf("commitIndex = %d, want 0: entries are from term 1, not the leader's current term 2, so a majority alone must not commit them", n.commitIndex)
	}
}
